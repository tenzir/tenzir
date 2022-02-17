//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/arrow_extension_types.hpp>
#include <vast/arrow_table_slice_builder.hpp>
#include <vast/concept/convertible/data.hpp>
#include <vast/concept/convertible/to.hpp>
#include <vast/concept/parseable/to.hpp>
#include <vast/concept/parseable/vast/time.hpp>
#include <vast/detail/narrow.hpp>
#include <vast/hash/hash_append.hpp>
#include <vast/plugin.hpp>
#include <vast/transform_step.hpp>

#include <arrow/compute/api.h>
#include <arrow/config.h>
#include <arrow/table.h>
#include <arrow/table_builder.h>

#include <unordered_map>

namespace vast::plugins::aggregate {

/// The configuration of an aggregate transform step.
struct configuration {
  /// Duration window for grouping time values.
  std::optional<duration> time_resolution = {};

  /// List of fields to group by.
  std::vector<std::string> group_by = {};

  /// List of fields to sum.
  std::vector<std::string> sum = {};

  /// List of fields to take the minimum of.
  std::vector<std::string> min = {};

  /// List of fields to take the maximum of.
  std::vector<std::string> max = {};

  /// List of fields to take the disjunction of.
  std::vector<std::string> any = {};

  /// List of fields to take the conjunction of.
  std::vector<std::string> all = {};

  /// Support type inspection for easy parsing with convertible.
  template <class Inspector>
  friend auto inspect(Inspector& f, configuration& x) {
    return f(x.time_resolution, x.group_by, x.sum, x.min, x.max, x.any, x.all);
  }

  /// Enable parsing from a record via convertible.
  static inline const record_type& layout() noexcept {
    static auto result = record_type{
      {"time-resolution", duration_type{}},
      {"group-by", list_type{string_type{}}},
      {"sum", list_type{string_type{}}},
      {"min", list_type{string_type{}}},
      {"max", list_type{string_type{}}},
      {"any", list_type{string_type{}}},
      {"all", list_type{string_type{}}},
    };
    return result;
  }
};

/// The layout-specific state for an aggregation.
struct aggregation {
  /// The action to take for a given column. Columns without an action are
  /// dropped as part of the aggregation.
  enum class action {
    group_by, ///< Group identical values.
    sum,      ///< Accumulate values within the same group.
    min,      ///< Use the minimum value within the same group.
    max,      ///< Use the maximum value within the same group.
    any,      ///< Disjoin values within the same group.
    all,      ///< Conjoin values within the same group.
  };

  /// The key by which aggregations are grouped. Essentially, this is an
  /// *arrow::ScalarVector* with custom equality and hash operations.
  struct group_by_key : arrow::ScalarVector {
    using arrow::ScalarVector::ScalarVector;
  };

  /// The equality functor for enabling use of *group_by_key* as a key in
  /// map data structures.
  struct group_by_key_equal {
    bool operator()(const group_by_key& lhs,
                    const group_by_key& rhs) const noexcept {
      return std::equal(lhs.begin(), lhs.end(), rhs.begin(), rhs.end(),
                        [](const std::shared_ptr<arrow::Scalar>& x,
                           const std::shared_ptr<arrow::Scalar>& y) noexcept {
                          return x->Equals(y);
                        });
    }
  };

  /// The hash functor for enabling use of *group_by_key* as a key in unordered
  /// map data structures.
  struct group_by_key_hash {
    size_t operator()(const group_by_key& x) const noexcept {
      auto hasher = vast::xxh64{};
      for (const auto& scalar : x)
        if (scalar)
          vast::hash_append(hasher, scalar->hash());
      return hasher.finish();
    }
  };

  /// Groups record batch slices where a configured set of columns is equal.
  using bucket_map = std::unordered_map<group_by_key, arrow::RecordBatchVector,
                                        group_by_key_hash, group_by_key_equal>;

  /// Creates a new aggregation given a configuration and a layout.
  static caf::expected<aggregation>
  make(const configuration& config, const type& layout) {
    auto result = aggregation{};
    VAST_ASSERT(caf::holds_alternative<record_type>(layout));
    const auto& rt = caf::get<record_type>(layout);
    auto unflattened_actions = std::vector<std::pair<offset, action>>{};
    auto resolve_action = [&](const auto& keys, enum action action) noexcept {
      for (const auto& key : keys)
        for (auto&& index : rt.resolve_key_suffix(key, layout.name()))
          unflattened_actions.emplace_back(index, action);
    };
    resolve_action(config.group_by, action::group_by);
    resolve_action(config.sum, action::sum);
    resolve_action(config.min, action::min);
    resolve_action(config.max, action::max);
    resolve_action(config.any, action::any);
    resolve_action(config.all, action::all);
    std::sort(unflattened_actions.begin(), unflattened_actions.end());
    const auto has_duplicates
      = std::adjacent_find(unflattened_actions.begin(),
                           unflattened_actions.end(),
                           [](const auto& lhs, const auto& rhs) noexcept {
                             return lhs.first == rhs.first;
                           })
        != unflattened_actions.end();
    if (has_duplicates)
      return caf::make_error(ec::invalid_configuration,
                             fmt::format("aggregation detected ambiguous "
                                         "action configuration for layout {}",
                                         layout));
    auto drop_transformations = std::vector<record_type::transformation>{};
    for (int flat_index = 0; const auto& leaf : rt.leaves()) {
      if (leaf.index
          != unflattened_actions[result.selected_columns_.size()].first) {
        drop_transformations.push_back({leaf.index, record_type::drop()});
      } else {
        const auto action
          = unflattened_actions[result.selected_columns_.size()].second;
        result.actions_.push_back(action);
        if (action == action::group_by
            && caf::holds_alternative<time_type>(leaf.field.type))
          result.round_temporal_columns_.push_back(
            detail::narrow_cast<int>(result.selected_columns_.size()));
        result.selected_columns_.push_back(flat_index);
      }
      ++flat_index;
    }
    auto adjusted_rt = rt.transform(std::move(drop_transformations));
    VAST_ASSERT(adjusted_rt);
    VAST_ASSERT(!layout.has_attributes());
    result.adjusted_layout_ = type{layout.name(), *adjusted_rt};
    result.num_group_by_columns_ = std::count(
      result.actions_.begin(), result.actions_.end(), action::group_by);
    result.time_resolution_ = config.time_resolution;
    return result;
  }

  /// Aggregations are expensive to copy because they hold a lot of state, so we
  /// make them non-copyable. Not because they cannot be copied theoretically,
  /// but because doing so would most certainly be a design issue within the code.
  aggregation(const aggregation&) = delete;
  aggregation& operator=(const aggregation&) = delete;

  /// Destruction and move-operations can simply be defaulted for an aggregation.
  ~aggregation() noexcept = default;
  aggregation(aggregation&&) noexcept = default;
  aggregation& operator=(aggregation&&) noexcept = default;

  /// Adds a record batch to the aggregation. Unless disabled, this performs an
  /// eager aggregation already.
  caf::error add(std::shared_ptr<arrow::RecordBatch> batch) {
    // First, adjust the record batch: We only want to aggregate a subset of
    // rows, and the remaining rows can just be dropped eagerly. It is important
    // that we do this first to avoid unnecessary overhead first, and also
    // because all the indices calculated from the configuration in the
    // constructor are for the selected columns only.
    auto select_columns_result = batch->SelectColumns(selected_columns_);
    if (!select_columns_result.ok())
      return caf::make_error(
        ec::unspecified,
        fmt::format("aggregate transform failed to select columns: {}",
                    select_columns_result.status().ToString()));
    batch = select_columns_result.MoveValueUnsafe();
    VAST_ASSERT(batch->num_columns()
                == detail::narrow_cast<int>(actions_.size()));
    // Second, round time values to a multiple of the configured value.
    if (time_resolution_) {
#if ARROW_VERSION_MAJOR >= 7
      const auto options = arrow::compute::RoundTemporalOptions{
        std::chrono::duration_cast<std::chrono::duration<int, std::milli>>(
          *time_resolution_)
          .count(),
        arrow::compute::CalendarUnit::MILLISECOND,
      };
#else
      const auto options = arrow::compute::RoundToMultipleOptions{
        detail::narrow_cast<double>(time_resolution_->count())};
#endif
      for (const auto& column : round_temporal_columns_) {
#if ARROW_VERSION_MAJOR >= 7
        auto round_temporal_result
          = arrow::compute::RoundTemporal(batch->column(column), options);
#else
        auto round_temporal_result
          = arrow::compute::RoundToMultiple(batch->column(column), options);
        if (round_temporal_result.ok())
          round_temporal_result
            = arrow::compute::Cast(round_temporal_result.MoveValueUnsafe(),
                                   batch->column(column)->type());
#endif
        if (!round_temporal_result.ok())
          return caf::make_error(
            ec::unspecified,
            fmt::format("aggregate transform failed to round time column {} "
                        "to multiple of {}: {}",
                        batch->column_name(column), *time_resolution_,
                        round_temporal_result.status().ToString()));
        auto set_column_result = batch->SetColumn(
          column, batch->schema()->field(column),
          round_temporal_result.MoveValueUnsafe().make_array());
        if (!set_column_result.ok())
          return caf::make_error(
            ec::unspecified,
            fmt::format("aggregate transform failed to replace column: {}",
                        set_column_result.status().ToString()));
        batch = set_column_result.MoveValueUnsafe();
      }
    }
    // Third and last, aggregate the batch eagerly.
    return aggregate({std::move(batch)});
  }

  /// Returns the aggregated batches, doing a second aggregation pass unless
  /// disabled.
  caf::expected<std::vector<transform_batch>> finish() {
    if (auto err = aggregate(std::exchange(buffer_, {})))
      return err;
    // Collect the results from the buffer.
    auto result = std::vector<transform_batch>{};
    result.reserve(buffer_.size());
    for (auto&& batch : std::exchange(buffer_, {}))
      result.emplace_back(adjusted_layout_, std::move(batch));
    return result;
  }

private:
  /// Default-constructor for internal use in `make(...)`.
  aggregation() = default;

  /// Finds or creates the bucket for a given row in a record batch.
  bucket_map::iterator
  find_or_create_bucket(bucket_map& buckets,
                        const std::shared_ptr<arrow::RecordBatch>& batch,
                        int row) {
    // If our row goes beyond the end of the batch, signal that we do not have a
    // bucket.
    if (row >= batch->num_rows())
      return buckets.end();
    // Create current bucket key.
    auto key = group_by_key{};
    key.reserve(num_group_by_columns_);
    for (int column = 0; column < batch->num_columns(); ++column) {
      if (actions_[column] == action::group_by) {
        auto scalar = batch->column(column)
                        ->View(batch->schema()->field(column)->type())
                        .MoveValueUnsafe()
                        ->GetScalar(row)
                        .ValueOr(nullptr);
        key.push_back(scalar);
      }
    }
    // Find bucket for current key.
    const auto bucket = buckets.find(key);
    if (bucket != buckets.end())
      return bucket;
    // Create a new bucket consisting of an empty table that we can later append
    // to.
    auto [new_bucket, ok] = buckets.try_emplace(std::move(key));
    VAST_ASSERT(ok);
    VAST_ASSERT(new_bucket != buckets.end());
    return new_bucket;
  }

  /// Aggregates a vector of record batches into a set of record batches that
  /// belong to the same buckets.
  caf::error aggregate(const arrow::RecordBatchVector& batches) {
    if (batches.empty())
      return caf::none;
    // Iterate over the record batches row-wise and select slices that group
    // into the same bucket as large as possible.
    auto buckets = bucket_map{};
    for (const auto& batch : batches) {
      VAST_ASSERT(batch->num_rows() >= 1);
      int start = 0;
      int next_start = 0;
      auto next_bucket = find_or_create_bucket(buckets, batch, start);
      for (auto bucket = next_bucket; next_bucket != buckets.end();
           bucket = next_bucket) {
        start = next_start;
        do {
          next_bucket = find_or_create_bucket(buckets, batch, ++next_start);
        } while (bucket == next_bucket);
        VAST_TRACE("aggregate transform slices [{}, {}) out of {} row(s)",
                   start, next_start, batch->num_rows());
        auto slice = batch->Slice(start, next_start - start);
        VAST_ASSERT([&]() {
          for (int column = 0;
               column < detail::narrow_cast<int>(actions_.size()); column++) {
            if (actions_[column] != action::group_by)
              continue;
            if (arrow::compute::Unique(slice->column(column))
                  .ValueOrDie()
                  ->length()
                != 1)
              return false;
          }
          return true;
        }());
        VAST_ASSERT(slice);
        VAST_ASSERT(bucket != buckets.end());
        bucket->second.push_back(std::move(slice));
      }
    }
    // If we don't have a builder yet for storing the intermediate results, this
    // is where we create it.
    if (!builder_) {
      const auto initial_size = detail::narrow_cast<int>(buckets.size());
      auto make_builder_result
        = arrow::RecordBatchBuilder::Make(batches[0]->schema(),
                                          arrow::default_memory_pool(),
                                          initial_size, &builder_);
      if (!make_builder_result.ok())
        return caf::make_error(ec::unspecified,
                               fmt::format("aggregate transform failed to "
                                           "create record batch builder: {}",
                                           make_builder_result.ToString()));
    } else {
      builder_->SetInitialCapacity(detail::narrow_cast<int>(buckets.size()));
    }
    // Third and last, for every bucket we must aggregate the results and put
    // them into the builder.
    for (const auto& bucket : buckets) {
      VAST_ASSERT(!bucket.second.empty());
      auto table = arrow::Table::FromRecordBatches(bucket.second).ValueOrDie();
      VAST_ASSERT(table->num_columns()
                  == detail::narrow_cast<int>(actions_.size()));
      VAST_ASSERT(table->num_columns() == builder_->num_fields());
      VAST_ASSERT(table->num_rows() > 0);
      for (int column = 0; column < table->num_columns(); ++column) {
        auto append_to_builder
          = [&](const std::shared_ptr<arrow::Scalar>& scalar) noexcept {
              if (scalar && scalar->is_valid) {
                auto append_result
                  = builder_->GetField(column)->AppendScalar(*scalar);
                VAST_ASSERT(append_result.ok());
              } else {
                auto append_result = builder_->GetField(column)->AppendNull();
                VAST_ASSERT(append_result.ok());
              }
            };
        switch (actions_[column]) {
          case action::group_by: {
            auto get_scalar_result = table->column(column)->GetScalar(0);
            if (!get_scalar_result.ok())
              return caf::make_error(
                ec::unspecified,
                fmt::format("aggregate transform failed to access grouped "
                            "value: {}",
                            get_scalar_result.status().ToString()));
            append_to_builder(get_scalar_result.MoveValueUnsafe());
            break;
          }
          case action::sum: {
            auto sum_result = arrow::compute::Sum(table->column(column));
            if (!sum_result.ok())
              return caf::make_error(
                ec::unspecified, fmt::format("aggregate transform failed to "
                                             "compute sum: {}",
                                             sum_result.status().ToString()));
            append_to_builder(sum_result.MoveValueUnsafe().scalar());
            break;
          }
          case action::min: {
            auto min_max_result = arrow::compute::MinMax(table->column(column));
            if (!min_max_result.ok())
              return caf::make_error(
                ec::unspecified,
                fmt::format("aggregate transform failed to compute minimum: "
                            "{}",
                            min_max_result.status().ToString()));
            append_to_builder(min_max_result.MoveValueUnsafe()
                                .scalar_as<arrow::StructScalar>()
                                .value[0]);
            break;
          }
          case action::max: {
            auto min_max_result = arrow::compute::MinMax(table->column(column));
            if (!min_max_result.ok())
              return caf::make_error(
                ec::unspecified,
                fmt::format("aggregate transform failed to compute maximum: "
                            "{}",
                            min_max_result.status().ToString()));
            append_to_builder(min_max_result.MoveValueUnsafe()
                                .scalar_as<arrow::StructScalar>()
                                .value[1]);
            break;
          }
          case action::any: {
            auto any_result = arrow::compute::Any(table->column(column));
            if (!any_result.ok())
              return caf::make_error(
                ec::unspecified, fmt::format("aggregate transform failed to "
                                             "compute disjunction: {}",
                                             any_result.status().ToString()));
            append_to_builder(any_result.MoveValueUnsafe().scalar());
            break;
          }
          case action::all: {
            auto all_result = arrow::compute::All(table->column(column));
            if (!all_result.ok())
              return caf::make_error(
                ec::unspecified, fmt::format("aggregate transform failed to "
                                             "compute conjunction: {}",
                                             all_result.status().ToString()));
            append_to_builder(all_result.MoveValueUnsafe().scalar());
            break;
          }
        }
      }
    }
    // Lastly, add the newly created aggregated batch to the buffer.
    auto batch = std::shared_ptr<arrow::RecordBatch>{};
    auto flush_result = builder_->Flush(&batch);
    if (!flush_result.ok())
      return caf::make_error(ec::unspecified,
                             fmt::format("aggregate transform failed to flush "
                                         "aggregated batch: {}",
                                         flush_result.ToString()));
    buffer_.push_back(std::move(batch));
    return caf::none;
  }

  /// The action to take during aggregation for every individual column in the
  /// incoming record batches.
  std::vector<action> actions_ = {};

  /// The columnns that are selected from the incoming record batches as part of
  /// the data transformation.
  std::vector<int> selected_columns_ = {};

  /// The group-by columns from the record batches that hold time values. These
  /// need to be handled with special care, as we round them to a multiple of a
  /// configured value.
  std::vector<int> round_temporal_columns_ = {};

  /// The duration used as the multiple value when rounding grouped temporal
  /// values.
  std::optional<duration> time_resolution_ = {};

  /// The adjusted layout with the dropped columns removed.
  type adjusted_layout_ = {};

  /// The builder used as part of the aggregation. Stored since it only needs to
  /// be created once per layout effectively, and we can do so lazily.
  std::unique_ptr<arrow::RecordBatchBuilder> builder_ = {};

  /// The buffer responsible for holding the intermediate aggregation results.
  arrow::RecordBatchVector buffer_ = {};

  /// The number of columns to group by.
  size_t num_group_by_columns_ = {};
};

/// The aggregate transform step, which holds applies an aggregation to every
/// incoming record batch, which is configured per-type. The aggregation
/// configuration is resolved eagerly and then executed eagerly and/or lazily
/// per type.
class aggregate_step : public transform_step {
public:
  /// Create a new aggregate step from an already parsed configuration.
  aggregate_step(configuration config) : config_{std::move(config)} {
    // nop
  }

  /// Marks this transform step as an aggregating transform step.
  [[nodiscard]] bool is_aggregate() const override {
    return true;
  }

  /// Applies the transformation to an Arrow Record Batch with a corresponding
  /// VAST layout; this creates a layout-specific aggregation lazily.
  [[nodiscard]] caf::error
  add(type layout, std::shared_ptr<arrow::RecordBatch> batch) override {
    auto aggregation = aggregations_.find(layout);
    if (aggregation == aggregations_.end()) {
      auto make_aggregation_result = aggregation::make(config_, layout);
      if (!make_aggregation_result)
        return make_aggregation_result.error();
      auto [new_aggregation, ok] = aggregations_.try_emplace(
        layout, std::move(*make_aggregation_result));
      VAST_ASSERT(ok);
      aggregation = new_aggregation;
    }
    return aggregation->second.add(std::move(batch));
  }

  /// Retrieves the result of the transformation.
  [[nodiscard]] caf::expected<std::vector<transform_batch>> finish() override {
    auto result = std::vector<transform_batch>{};
    for (auto&& [_, aggregation] : aggregations_) {
      auto batches = aggregation.finish();
      if (!batches)
        return batches.error();
      result.reserve(result.size() + batches->size());
      std::move(batches->begin(), batches->end(), std::back_inserter(result));
    }
    return result;
  }

private:
  /// The underlying configuration of the transformation.
  configuration config_ = {};

  /// A mapping of layout to the configured aggregation.
  std::unordered_map<type, aggregation> aggregations_ = {};
};

/// The plugin entrypoint for the aggregate transform plugin.
class plugin final : public transform_plugin {
public:
  /// Initializes the aggregate plugin. This plugin has no general configuration,
  /// and is configured per instantiation as part of the transforms definition.
  /// We only check whether there's no unexpected configuration here.
  caf::error initialize(data options) override {
    if (caf::holds_alternative<caf::none_t>(options))
      return caf::none;
    if (const auto* rec = caf::get_if<record>(&options))
      if (rec->empty())
        return caf::none;
    return caf::make_error(ec::invalid_configuration, //
                           "expected empty configuration under "
                           "vast.plugins.aggregate");
  }

  /// Returns the unique name of the plugin, which also equals the transform
  /// step name that is used to refer to instantiations of the aggregate step
  /// when configuring transforms.
  [[nodiscard]] const char* name() const override {
    return "aggregate";
  };

  /// This is called once for every time this transform step appears in a
  /// transform definition. The configuration for the step is opaquely
  /// passed as the first argument.
  [[nodiscard]] caf::expected<std::unique_ptr<transform_step>>
  make_transform_step(const caf::settings& options) const override {
    auto rec = to<record>(options);
    if (!rec)
      return rec.error();
    auto config = to<configuration>(*rec);
    if (!config)
      return config.error();
    return std::make_unique<aggregate_step>(std::move(*config));
  }
};

} // namespace vast::plugins::aggregate

// Finally, register our plugin.
VAST_REGISTER_PLUGIN(vast::plugins::aggregate::plugin)

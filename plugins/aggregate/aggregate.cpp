//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/arrow_table_slice_builder.hpp>
#include <vast/concept/convertible/data.hpp>
#include <vast/concept/convertible/to.hpp>
#include <vast/concept/parseable/to.hpp>
#include <vast/concept/parseable/vast/time.hpp>
#include <vast/detail/narrow.hpp>
#include <vast/hash/hash_append.hpp>
#include <vast/plugin.hpp>
#include <vast/transform_step.hpp>

#include <arrow/builder.h>
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
      auto hasher = xxh64{};
      for (const auto& scalar : x)
        if (scalar)
          hash_append(hasher, scalar->hash());
      return hasher.finish();
    }
  };

  /// Groups accumulators for slices of incoming record batches with a matching
  /// set of configured set of configured group-by columns.
  using bucket_map = std::unordered_map<group_by_key, arrow::ScalarVector,
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
    // Create a builder if we don't have one already.
    if (!builder_) {
      auto status = arrow::RecordBatchBuilder::Make(
        batch->schema(), arrow::default_memory_pool(), &builder_);
      VAST_ASSERT(status.ok(), status.ToString().c_str());
      VAST_ASSERT(builder_);
    }
    // Round time values to a multiple of the configured value.
    if (time_resolution_) {
      const auto options = arrow::compute::RoundTemporalOptions{
        std::chrono::duration_cast<std::chrono::duration<int, std::milli>>(
          *time_resolution_)
          .count(),
        arrow::compute::CalendarUnit::MILLISECOND,
      };
      for (const auto& column : round_temporal_columns_) {
        auto round_temporal_result
          = arrow::compute::RoundTemporal(batch->column(column), options);
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
    // Iterate over the record batches row-wise and select slices that group
    // into the same bucket as large as possible, adding them into the buckets
    // accumulator state one slice at a time.
    VAST_ASSERT(batch->num_rows() >= 1);
    int start = 0;
    int next_start = 0;
    auto next_bucket = try_emplace_bucket(batch, start);
    for (auto bucket = next_bucket; next_bucket != buckets_.end();
         bucket = next_bucket) {
      start = next_start;
      do {
        next_bucket = try_emplace_bucket(batch, ++next_start);
      } while (bucket == next_bucket);
      VAST_ASSERT(bucket->second.size() == actions_.size());
      for (int column = 0; column < batch->num_columns(); ++column) {
        auto f =
          [&]<class Array>([[maybe_unused]] const Array& array) -> caf::error {
          if constexpr (std::is_same_v<Array, arrow::NullArray>) {
            die("aggregate transform step cannot operate on null arrays");
          } else {
            // TODO: Remove FixedSizeBinaryArray from this list when making the
            // experimental arrow encoding the default.
            constexpr auto is_non_primitive_array
              = detail::is_any_v<Array, arrow::FixedSizeBinaryArray,
                                 type_to_arrow_array_t<pattern_type>,
                                 type_to_arrow_array_t<address_type>,
                                 type_to_arrow_array_t<subnet_type>,
                                 type_to_arrow_array_t<enumeration_type>,
                                 type_to_arrow_array_t<list_type>,
                                 type_to_arrow_array_t<map_type>,
                                 type_to_arrow_array_t<record_type>>;
            auto make_non_primitive_error = [&]() {
              return caf::make_error(ec::invalid_configuration,
                                     fmt::format("aggregate transform step "
                                                 "cannot handle non-primitive "
                                                 "field {}",
                                                 array.type()->ToString()));
            };
            using scalar_type
              = std::conditional_t<is_non_primitive_array, arrow::Scalar,
                                   typename arrow::TypeTraits<std::conditional_t<
                                     is_non_primitive_array, arrow::BooleanType,
                                     typename Array::TypeClass>>::ScalarType>;
            auto scalar
              = std::static_pointer_cast<scalar_type>(bucket->second[column]);
            for (int row = start; row < next_start; ++row) {
              if (array.IsNull(row))
                continue;
              if (!scalar) {
                scalar = std::static_pointer_cast<scalar_type>(
                  array.GetScalar(row).MoveValueUnsafe());
                continue;
              }
              switch (actions_[column]) {
                case action::group_by: {
                  row = next_start;
                  break;
                }
                case action::sum: {
                  if constexpr (is_non_primitive_array) {
                    return make_non_primitive_error();
                  } else if constexpr (requires(scalar_type lhs,
                                                scalar_type rhs) {
                                         {lhs.value + rhs.value};
                                       })
                    scalar->value = scalar->value + array.Value(row);
                  else
                    return caf::make_error(
                      ec::invalid_configuration,
                      fmt::format("aggregate transform step cannot "
                                  "calculate 'sum' of field {}",
                                  batch->schema()->field(column)->ToString()));
                  break;
                }
                case action::min: {
                  if constexpr (is_non_primitive_array) {
                    return make_non_primitive_error();
                  } else if constexpr (std::is_same_v<scalar_type,
                                                      arrow::StringScalar>) {
                    if (array.Value(row) < scalar->view())
                      scalar = std::static_pointer_cast<scalar_type>(
                        array.GetScalar(row).MoveValueUnsafe());
                  } else if constexpr (requires(scalar_type lhs,
                                                scalar_type rhs) {
                                         {lhs.value < rhs.value};
                                       }) {
                    scalar->value = std::min(scalar->value, array.Value(row));
                  } else {
                    return caf::make_error(
                      ec::invalid_configuration,
                      fmt::format("aggregate transform step cannot "
                                  "calculate 'min' of field {}",
                                  batch->schema()->field(column)->ToString()));
                  }
                  break;
                }
                case action::max: {
                  if constexpr (is_non_primitive_array) {
                    return make_non_primitive_error();
                  } else if constexpr (std::is_same_v<scalar_type,
                                                      arrow::StringScalar>) {
                    if (array.Value(row) > scalar->view())
                      scalar = std::static_pointer_cast<scalar_type>(
                        array.GetScalar(row).MoveValueUnsafe());
                  } else if constexpr (requires(scalar_type lhs,
                                                scalar_type rhs) {
                                         {lhs.value > rhs.value};
                                       }) {
                    scalar->value = std::max(scalar->value, array.Value(row));
                  } else {
                    return caf::make_error(
                      ec::invalid_configuration,
                      fmt::format("aggregate transform step cannot "
                                  "calculate 'max' of field {}",
                                  batch->schema()->field(column)->ToString()));
                  }
                  break;
                }
                case action::any: {
                  if constexpr (is_non_primitive_array) {
                    return make_non_primitive_error();
                  } else if constexpr (std::is_same_v<Array,
                                                      arrow::BooleanArray>) {
                    scalar->value = scalar->value || array.Value(row);
                  } else {
                    return caf::make_error(
                      ec::invalid_configuration,
                      fmt::format("aggregate transform step cannot "
                                  "calculate 'any' of field {}",
                                  batch->schema()->field(column)->ToString()));
                  }
                  break;
                }
                case action::all: {
                  if constexpr (is_non_primitive_array) {
                    return make_non_primitive_error();
                  } else if constexpr (std::is_same_v<Array,
                                                      arrow::BooleanArray>) {
                    scalar->value = scalar->value && array.Value(row);
                  } else {
                    return caf::make_error(
                      ec::invalid_configuration,
                      fmt::format("aggregate transform step cannot "
                                  "calculate 'all' of field {}",
                                  batch->schema()->field(column)->ToString()));
                  }
                  break;
                }
              }
            }
            bucket->second[column] = std::move(scalar);
          }
          return caf::none;
        };
        auto arr = batch->column(column);
        // TODO: Remove this workaround to support old fixed size binary arrays.
        if (arr->type_id() == arrow::Type::FIXED_SIZE_BINARY) {
          if (auto err
              = f(static_cast<const arrow::FixedSizeBinaryArray&>(*arr)))
            return err;
        } else if (auto err = caf::visit(f, *batch->column(column))) {
          return err;
        }
      }
    }
    return caf::none;
  }

  /// Returns the aggregated batches.
  caf::expected<transform_batch> finish() {
    VAST_ASSERT(builder_);
    auto cast_requirements
      = std::vector<std::optional<bool>>(builder_->num_fields(), std::nullopt);
    // Calculate whether a column needs to be casted only once.
    auto needs_cast = [&](int column, const arrow::Scalar& scalar) {
      auto& cast_requirement = cast_requirements[column];
      if (!cast_requirement)
        cast_requirement
          = !builder_->GetField(column)->type()->Equals(scalar.type);
      return *cast_requirement;
    };
    builder_->SetInitialCapacity(detail::narrow_cast<int>(buckets_.size()));
    for (auto&& bucket : std::exchange(buckets_, {})) {
      for (int column = 0; auto& scalar : bucket.second) {
        auto* column_builder = builder_->GetField(column);
        VAST_ASSERT(column_builder);
        if (scalar && needs_cast(column, *scalar)) {
          auto cast_result = scalar->CastTo(column_builder->type());
          VAST_ASSERT(cast_result.ok(),
                      cast_result.status().ToString().c_str());
          scalar = cast_result.MoveValueUnsafe();
        }
        if (scalar) {
          auto append_result = column_builder->AppendScalar(*scalar);
          VAST_ASSERT(append_result.ok(), append_result.ToString().c_str());
        } else {
          auto append_result = column_builder->AppendNull();
          VAST_ASSERT(append_result.ok(), append_result.ToString().c_str());
        }
        column++;
      }
    }
    auto sc = std::shared_ptr<arrow::UInt64Scalar>{};
    auto batch = std::shared_ptr<arrow::RecordBatch>{};
    auto flush_result = builder_->Flush(&batch);
    VAST_ASSERT(flush_result.ok(), flush_result.ToString().c_str());
    return transform_batch{adjusted_layout_, std::move(batch)};
  }

private:
  /// Default-constructor for internal use in `make(...)`.
  aggregation() = default;

  /// Finds or creates the bucket for a given row in a record batch.
  bucket_map::iterator
  try_emplace_bucket(const std::shared_ptr<arrow::RecordBatch>& batch,
                     int row) {
    // If our row goes beyond the end of the batch, signal that we do not have
    // a bucket.
    if (row >= batch->num_rows())
      return buckets_.end();
    // Create current bucket key.
    auto key = group_by_key{};
    key.reserve(num_group_by_columns_);
    for (int column = 0; column < batch->num_columns(); ++column)
      if (actions_[column] == action::group_by)
        key.push_back(batch->column(column)->GetScalar(row).MoveValueUnsafe());
    // Create a new bucket.
    auto [iterator, inserted] = buckets_.try_emplace(std::move(key));
    if (inserted)
      iterator->second.resize(actions_.size(), nullptr);
    return iterator;
  }

  /// The action to take during aggregation for every individual column in the
  /// incoming record batches.
  std::vector<action> actions_ = {};

  /// The columnns that are selected from the incoming record batches as part
  /// of the data transformation.
  std::vector<int> selected_columns_ = {};

  /// The group-by columns from the record batches that hold time values.
  /// These need to be handled with special care, as we round them to a
  /// multiple of a configured value.
  std::vector<int> round_temporal_columns_ = {};

  /// The duration used as the multiple value when rounding grouped temporal
  /// values.
  std::optional<duration> time_resolution_ = {};

  /// The adjusted layout with the dropped columns removed.
  type adjusted_layout_ = {};

  /// The buckets holding the intemediate accumulators.
  bucket_map buckets_ = {};

  /// The builder used as part of the aggregation. Stored since it only needs
  /// to be created once per layout effectively, and we can do so lazily.
  std::unique_ptr<arrow::RecordBatchBuilder> builder_ = {};

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
    result.reserve(aggregations_.size());
    for (auto&& [_, aggregation] : aggregations_) {
      auto batch = aggregation.finish();
      if (!batch)
        return batch.error();
      result.push_back(std::move(*batch));
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
  /// Initializes the aggregate plugin. This plugin has no general
  /// configuration, and is configured per instantiation as part of the
  /// transforms definition. We only check whether there's no unexpected
  /// configuration here.
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

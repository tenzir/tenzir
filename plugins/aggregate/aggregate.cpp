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

#include <arrow/compute/api_aggregate.h>
#include <arrow/compute/api_scalar.h>
#include <arrow/compute/function.h>
#include <arrow/table.h>
#include <arrow/table_builder.h>

#include <unordered_map>

namespace vast::plugins::aggregate {

/// The configuration of an aggregate transform step.
struct configuration {
  /// Duration window for grouping time values.
  std::optional<duration> time_window = {};

  /// List of fields to group by.
  std::vector<std::string> group_by = {};

  /// List of fields to sum.
  std::vector<std::string> sum = {};

  /// List of fields to take the minimum of.
  std::vector<std::string> min = {};

  /// List of fields to take the maximum of.
  std::vector<std::string> max = {};

  /// List of fields to take the maximum of.
  std::vector<std::string> mean = {};

  /// List of fields to take the disjunction of.
  std::vector<std::string> any = {};

  /// List of fields to take the conjunction of.
  std::vector<std::string> all = {};

  template <class Inspector>
  friend auto inspect(Inspector& f, configuration& x) {
    return f(x.time_window, x.group_by, x.sum, x.min, x.max, x.mean, x.any,
             x.all);
  }

  static inline const record_type& layout() noexcept {
    static auto result = record_type{
      {"time-window", duration_type{}},  {"group-by", list_type{string_type{}}},
      {"sum", list_type{string_type{}}}, {"min", list_type{string_type{}}},
      {"max", list_type{string_type{}}}, {"mean", list_type{string_type{}}},
      {"any", list_type{string_type{}}}, {"all", list_type{string_type{}}},
    };
    return result;
  }
};

enum class action {
  drop,
  group_by_time_window,
  group_by,
  sum,
  min,
  max,
  mean,
  any,
  all,
};

struct bucket_key : arrow::ScalarVector {
  using arrow::ScalarVector::ScalarVector;

  friend bool
  operator==(const bucket_key& lhs, const bucket_key& rhs) noexcept {
    return std::equal(lhs.begin(), lhs.end(), rhs.begin(), rhs.end(),
                      [](const std::shared_ptr<arrow::Scalar>& x,
                         const std::shared_ptr<arrow::Scalar>& y) noexcept {
                        return x->Equals(y);
                      });
  }
};

} // namespace vast::plugins::aggregate

namespace std {

template <>
struct std::hash<vast::plugins::aggregate::bucket_key> {
  size_t
  operator()(const vast::plugins::aggregate::bucket_key& x) const noexcept {
    auto hasher = vast::xxh64{};
    for (const auto& scalar : x) {
      VAST_ASSERT(scalar);
      vast::hash_append(hasher, scalar->hash());
    }
    return hasher.finish();
  }
};

} // namespace std

namespace vast::plugins::aggregate {

struct resolved_columns {
  resolved_columns(const configuration& config, const type& layout) {
    const auto& rt = caf::get<record_type>(layout);
    group_by_actions.resize(rt.num_leaves(), action::drop);
    for (const auto& key : config.group_by) {
      for (auto&& offset : rt.resolve_key_suffix(key, layout.name())) {
        group_by_actions[rt.flat_index(offset)]
          = caf::holds_alternative<time_type>(rt.field(offset).type)
              ? action::group_by_time_window
              : action::group_by;
      }
    }
    for (const auto& key : config.sum)
      for (auto&& offset : rt.resolve_key_suffix(key, layout.name()))
        group_by_actions[rt.flat_index(offset)] = action::sum;
    for (const auto& key : config.min)
      for (auto&& offset : rt.resolve_key_suffix(key, layout.name()))
        group_by_actions[rt.flat_index(offset)] = action::min;
    for (const auto& key : config.max)
      for (auto&& offset : rt.resolve_key_suffix(key, layout.name()))
        group_by_actions[rt.flat_index(offset)] = action::max;
    for (const auto& key : config.mean)
      for (auto&& offset : rt.resolve_key_suffix(key, layout.name()))
        group_by_actions[rt.flat_index(offset)] = action::mean;
    for (const auto& key : config.any)
      for (auto&& offset : rt.resolve_key_suffix(key, layout.name()))
        group_by_actions[rt.flat_index(offset)] = action::any;
    for (const auto& key : config.all)
      for (auto&& offset : rt.resolve_key_suffix(key, layout.name()))
        group_by_actions[rt.flat_index(offset)] = action::all;
  }

  resolved_columns() = default;
  ~resolved_columns() noexcept = default;
  resolved_columns(const resolved_columns&) = default;
  resolved_columns& operator=(const resolved_columns&) = default;
  resolved_columns(resolved_columns&&) noexcept = default;
  resolved_columns& operator=(resolved_columns&&) noexcept = default;

  /// The action to take during aggregation for every individual column in the
  /// incoming record batches.
  std::vector<action> group_by_actions = {};

  /// Buckets that we sort into.
  std::unordered_map<bucket_key, arrow::ChunkedArrayVector> buckets = {};
};

// The main job of a transform plugin is to create a `transform_step`
// when required. A transform step is a function that gets a table
// slice and returns the slice with a transformation applied.
class aggregate_step : public transform_step {
public:
  aggregate_step(configuration config) : config_{std::move(config)} {
    if (config_.time_window) {
      static_assert(std::is_same_v<time::period, std::nano>,
                    "aggregate step assumed nanosecond time resolution");
      round_temporal_options_ = arrow::compute::RoundTemporalOptions{
        detail::narrow_cast<int>(
          std::chrono::duration_cast<std::chrono::seconds>(*config_.time_window)
            .count()),
        arrow::compute::CalendarUnit::SECOND};
    }
  }

  /// Applies the transformation to an Arrow Record Batch with a corresponding
  /// VAST layout.
  [[nodiscard]] caf::error
  add(type layout, std::shared_ptr<arrow::RecordBatch> batch) override {
    VAST_ASSERT(caf::holds_alternative<record_type>(layout));
    const auto& rt = caf::get<record_type>(layout);
    // Find an existing resolved columns cache entry, or build a new one if it
    // doesn't exist yet.
    auto cache_entry = resolved_columns_cache_.find(layout);
    if (cache_entry == resolved_columns_cache_.end()) {
      // Parse resolved columns from configuration + layout.
      cache_entry = resolved_columns_cache_.insert(
        cache_entry, {layout, resolved_columns{config_, layout}});
    }
    auto& resolved_columns = cache_entry->second;
    // Apply timestamp window.
    if (config_.time_window) {
      for (int column = 0; column < batch->num_columns(); ++column) {
        // Skip all the olumns that are not supposed to be time windowed.
        if (resolved_columns.group_by_actions[column]
            != action::group_by_time_window)
          continue;
        // Find time column that we group by.
        const auto* time_column
          = caf::get_if<arrow::TimestampArray>(batch->column(column).get());
        if (!time_column)
          return caf::make_error(
            ec::invalid_configuration,
            fmt::format("failed to apply time window to column {}: column has "
                        "unexpected type",
                        rt.key(rt.resolve_flat_index(column))));
        // Apply rounding to multiple for time column.
        auto rounded_time_column = arrow::compute::RoundTemporal(
          *time_column, round_temporal_options_);
        if (!rounded_time_column.ok())
          return caf::make_error(
            ec::invalid_configuration,
            fmt::format("failed to apply time window to column {}: {}",
                        rt.key(rt.resolve_flat_index(column)),
                        rounded_time_column.status().ToString()));
        // Update the existing time column in the record batch.
        auto updated_batch
          = batch->SetColumn(column, batch->schema()->field(column),
                             rounded_time_column->make_array());
        if (!updated_batch.ok())
          return caf::make_error(
            ec::invalid_configuration,
            fmt::format("failed to apply time window to column {}: {}",
                        rt.key(rt.resolve_flat_index(column)),
                        updated_batch.status().ToString()));
        batch = updated_batch.MoveValueUnsafe();
      }
    }
    // Group into buckets.
    auto last_bucket = resolved_columns.buckets.end();
    for (int row = 0, last_row = 0; row < batch->num_rows(); ++row) {
      // Create current bucket key.
      auto current_key = bucket_key{};
      for (int column = 0; column < batch->num_columns(); ++column) {
        auto action = resolved_columns.group_by_actions[column];
        if (action == action::group_by
            || action == action::group_by_time_window) {
          current_key.emplace_back(
            batch->column(column)->GetScalar(row).ValueOr(nullptr));
        }
      }
      // Find bucket for current key.
      auto current_bucket = resolved_columns.buckets.find(current_key);
      // If bucket does not exist yet, create bucket with one chunked array per
      // column in the batch.
      if (current_bucket == resolved_columns.buckets.end()) {
        auto chunked_arrays = arrow::ChunkedArrayVector{};
        for (int column = 0; column < batch->num_columns(); ++column)
          if (resolved_columns.group_by_actions[column] != action::drop)
            chunked_arrays.emplace_back(
              arrow::ChunkedArray::MakeEmpty(batch->column(column)->type())
                .ValueOrDie());
        auto [it, inserted] = resolved_columns.buckets.try_emplace(
          std::move(current_key), std::move(chunked_arrays));
        VAST_ASSERT(inserted);
        current_bucket = it;
      }
      // If the bucket did not change we can slice more than one row at once.
      if (current_bucket == last_bucket)
        continue;
      // Append to chunked array per column in the batch.
      const auto next_last_row = row + 1;
      for (int column = 0, builder_column = 0; column < batch->num_columns();
           ++column) {
        if (resolved_columns.group_by_actions[column] == action::drop)
          continue;
        auto chunks = current_bucket->second[builder_column]->chunks();
        chunks.emplace_back(
          batch->column(column)->Slice(last_row, next_last_row - last_row));
        current_bucket->second[builder_column]
          = arrow::ChunkedArray::Make(
              std::move(chunks), current_bucket->second[builder_column]->type())
              .ValueOr(nullptr);
        ++builder_column;
      }
      last_row = next_last_row;
    }
    return caf::none;
  }

  /// Retrieves the result of the transformation.
  [[nodiscard]] caf::expected<std::vector<transform_batch>> finish() override {
    auto result = std::vector<transform_batch>{};
    result.reserve(resolved_columns_cache_.size());
    for (const auto& [layout, resolved_columns] : resolved_columns_cache_) {
      // Modify the layout to drop the columns we don't care about.
      auto transformations = std::vector<record_type::transformation>{};
      auto rt = caf::get<record_type>(layout);
      for (size_t index = 0; const auto& leaf : rt.leaves()) {
        if (resolved_columns.group_by_actions[index] == action::drop)
          transformations.push_back({leaf.index, record_type::drop()});
        ++index;
      }
      auto adjusted_rt = rt.transform(std::move(transformations));
      // TODO: can we handle the error safely in some way?
      VAST_ASSERT(adjusted_rt);
      VAST_ASSERT(!layout.has_attributes());
      auto adjusted_layout = type{layout.name(), *adjusted_rt};
      auto adjusted_schema = make_arrow_schema(adjusted_layout);
      // Set up the record batch builder for our aggregated slice.
      auto batch_builder = std::unique_ptr<arrow::RecordBatchBuilder>{};
      auto make_batch_builder_result = arrow::RecordBatchBuilder::Make(
        adjusted_schema, arrow::default_memory_pool(),
        detail::narrow_cast<int>(resolved_columns.buckets.size()),
        &batch_builder);
      VAST_ASSERT(batch_builder->num_fields()
                  == detail::narrow_cast<int>(adjusted_rt->num_leaves()));
      VAST_ASSERT(batch_builder->num_fields()
                  == detail::narrow_cast<int>(
                    resolved_columns.buckets.begin()->second.size()));
      // TODO: can we handle the error safely in some way?
      VAST_ASSERT(make_batch_builder_result.ok());
      // Iterate over all the buckets to perform the aggregations.
      for (const auto& bucket : resolved_columns.buckets) {
        for (int builder_column = 0;
             auto action : resolved_columns.group_by_actions) {
          switch (action) {
            case action::drop:
              // If we dropped the field then we must not increment the running
              // builder column index.
              continue;
            case action::group_by_time_window:
            case action::group_by: {
              auto value = bucket.second[builder_column]->GetScalar(0);
              if (value.ok()) {
                auto append_result = batch_builder->GetField(builder_column)
                                       ->AppendScalar(*value.MoveValueUnsafe());
                VAST_ASSERT(append_result.ok());
              } else {
                auto append_result
                  = batch_builder->GetField(builder_column)->AppendNull();
                VAST_ASSERT(append_result.ok());
              }
              break;
            }
            case action::sum: {
              auto sum = arrow::compute::Sum(bucket.second[builder_column])
                           .ValueOrDie();
              auto append_result = batch_builder->GetField(builder_column)
                                     ->AppendScalar(*sum.scalar());
              VAST_ASSERT(append_result.ok());
              break;
            }
            case action::min: {
              auto min_max
                = arrow::compute::MinMax(bucket.second[builder_column])
                    .ValueOrDie();
              auto append_result
                = batch_builder->GetField(builder_column)
                    ->AppendScalar(
                      *min_max.scalar_as<arrow::StructScalar>().value[0]);
              VAST_ASSERT(append_result.ok());
              break;
            }
            case action::max: {
              auto min_max
                = arrow::compute::MinMax(bucket.second[builder_column])
                    .ValueOrDie();
              auto append_result
                = batch_builder->GetField(builder_column)
                    ->AppendScalar(
                      *min_max.scalar_as<arrow::StructScalar>().value[1]);
              VAST_ASSERT(append_result.ok());
              break;
            }
            case action::mean: {
              auto mean = arrow::compute::Mean(bucket.second[builder_column])
                            .ValueOrDie();
              auto cast_result
                = mean.scalar()->CastTo(bucket.second[builder_column]->type());
              VAST_ASSERT(cast_result.ok());
              auto append_result
                = batch_builder->GetField(builder_column)
                    ->AppendScalar(*cast_result.MoveValueUnsafe());
              VAST_ASSERT(append_result.ok());
              break;
            }
            case action::any: {
              auto any = arrow::compute::Any(bucket.second[builder_column])
                           .ValueOrDie();
              auto append_result = batch_builder->GetField(builder_column)
                                     ->AppendScalar(*any.scalar());
              VAST_ASSERT(append_result.ok());
              break;
            }
            case action::all: {
              auto any = arrow::compute::All(bucket.second[builder_column])
                           .ValueOrDie();
              auto append_result = batch_builder->GetField(builder_column)
                                     ->AppendScalar(*any.scalar());
              VAST_ASSERT(append_result.ok());
              break;
            }
          }
          ++builder_column;
        }
      }
      // Create aggregated record batch, and emplace it into the result
      // alongside the modified type.
      auto adjusted_batch = std::shared_ptr<arrow::RecordBatch>{};
      auto flush_result = batch_builder->Flush(&adjusted_batch);
      VAST_ASSERT(flush_result.ok());
      result.emplace_back(adjusted_layout, adjusted_batch);
    }
    return result;
  }

private:
  /// The underlying configuration of the transformation.
  configuration config_ = {};

  /// Cache for mapping of layout to resolved columns.
  std::unordered_map<type, resolved_columns> resolved_columns_cache_ = {};

  /// Options for the `round_to_multiple` Arrow Compute function.
  arrow::compute::RoundTemporalOptions round_temporal_options_{
    1, arrow::compute::CalendarUnit::SECOND};
};

// -- plugin ------------------------------------------------------------------

/// The plugin definition itself is below.
class plugin final : public virtual transform_plugin {
public:
  caf::error initialize(data options) override {
    // We don't use any plugin-specific configuration under
    // vast.plugins.aggregate, so nothing is needed here.
    if (caf::holds_alternative<caf::none_t>(options))
      return caf::none;
    if (const auto* rec = caf::get_if<record>(&options))
      if (rec->empty())
        return caf::none;
    return caf::make_error(ec::invalid_configuration, "expected empty "
                                                      "configuration under "
                                                      "vast.plugins.aggregate");
  }

  /// The name is how the transform step is addressed in a transform definition.
  [[nodiscard]] const char* name() const override {
    return "aggregate";
  };

  /// This is called once for every time this transform step appears in a
  /// transform definition. The configuration for the step is opaquely
  /// passed as the first argument.
  [[nodiscard]] caf::expected<std::unique_ptr<transform_step>>
  make_transform_step(const caf::settings& options) const override {
    // TODO: parse options into some configuration struct (yet tbd).
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

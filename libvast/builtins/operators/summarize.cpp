//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/aggregation_function.hpp>
#include <vast/arrow_table_slice.hpp>
#include <vast/concept/convertible/data.hpp>
#include <vast/concept/convertible/to.hpp>
#include <vast/concept/parseable/core.hpp>
#include <vast/concept/parseable/vast/pipeline.hpp>
#include <vast/concept/parseable/vast/time.hpp>
#include <vast/error.hpp>
#include <vast/hash/hash_append.hpp>
#include <vast/plugin.hpp>
#include <vast/table_slice_builder.hpp>
#include <vast/type.hpp>

#include <arrow/compute/api_scalar.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <caf/expected.hpp>
#include <tsl/robin_map.h>

#include <algorithm>
#include <utility>

namespace vast::plugins::summarize {

namespace {

/// Converts a duration into the options required for Arrow Compute's
/// {Round,Floor,Ceil}Temporal functions.
/// @param time_resolution The multiple to round to.
arrow::compute::RoundTemporalOptions
make_round_temporal_options(duration time_resolution) noexcept {
  // Note that using any calendar unit lower than milliseconds causes Arrow
  // Compute to just not round at all, which is really weird and not documented.
  // You'd expect at least a NotImplemented status, but you actually just get
  // the input back unchanged. Last tested with Arrow 8.0.0. -- DL
  return arrow::compute::RoundTemporalOptions{
    std::chrono::duration_cast<std::chrono::duration<int, std::milli>>(
      time_resolution)
      .count(),
    arrow::compute::CalendarUnit::MILLISECOND,
  };
}

/// The configuration of a summarize pipeline operator, for example:
///
///   summarize:
///     group-by:
///       - community_id
///       - proto
///     time-resolution: 1 hour
///     aggregate:
///       ts: min
///       ips:
///         distinct:
///           - net.src.ip
///           - net.dst.ip
///       max_ts:
///         max: :timestamp
///
struct configuration {
  /// Create a configuration from the operator configuration.
  /// @param config The configuration of the summarize pipeline operator.
  static caf::expected<configuration> make(const record& config) {
    auto result = configuration{};
    for (const auto& [key, value] : config) {
      if (key == "group-by") {
        auto group_by_extractors = parse_group_by_extractors(value);
        if (!group_by_extractors)
          return group_by_extractors.error();
        result.group_by_extractors = std::move(*group_by_extractors);
        continue;
      }
      if (key == "time-resolution") {
        if (const auto* time_resolution = caf::get_if<duration>(&value)) {
          result.time_resolution = *time_resolution;
          continue;
        }
        return caf::make_error(ec::invalid_configuration,
                               fmt::format("unexpected config key: "
                                           "time-resolution {} is not a "
                                           "duration",
                                           value));
      }
      if (key == "aggregate") {
        auto aggregations = parse_aggregations(value);
        if (!aggregations)
          return aggregations.error();
        result.aggregations = std::move(*aggregations);
        continue;
      }
      return caf::make_error(ec::invalid_configuration,
                             fmt::format("unexpected config key: {}", key));
    }
    return result;
  }

  /// The configuration of a single aggregation.
  struct aggregation {
    std::string function_name; ///< The aggregation function name.
    std::vector<std::string> input_extractors; ///< Unresolved input extractors.
    std::string output;                        ///< The output field name.
  };

  /// Unresolved group-by extractors.
  std::vector<std::string> group_by_extractors = {};

  /// Resolution for time-columns in the group-by columns.
  std::optional<duration> time_resolution = {};

  /// Configuration for aggregation columns.
  std::vector<aggregation> aggregations = {};

private:
  /// Parse the unresolved group-by-extractors from their configuration.
  /// @param config The relevant configuration subsection.
  static caf::expected<std::vector<std::string>>
  parse_group_by_extractors(const data& config) {
    if (const auto* extractor = caf::get_if<std::string>(&config))
      return std::vector<std::string>{*extractor};
    if (const auto* extractors = caf::get_if<list>(&config)) {
      auto result = std::vector<std::string>{};
      result.reserve(extractors->size());
      for (const auto& extractor_data : *extractors) {
        if (const auto* extractor = caf::get_if<std::string>(&extractor_data)) {
          result.push_back(*extractor);
          continue;
        }
        return caf::make_error(ec::invalid_configuration,
                               fmt::format("unexpected config key: "
                                           "group-by extractor {} is not a "
                                           "string",
                                           extractor_data));
      }
      return result;
    }
    return caf::make_error(ec::invalid_configuration,
                           fmt::format("unexpected config key: group-by "
                                       "extractors {} are not string or a "
                                       "list of strings",
                                       config));
  }

  /// Parse the aggregation columns from their configuration.
  /// @param config The relevant configuration subsection.
  static caf::expected<std::vector<aggregation>>
  parse_aggregations(const data& config) {
    auto result = std::vector<aggregation>{};
    if (const auto* aggregations = caf::get_if<record>(&config)) {
      for (const auto& [key, value] : *aggregations) {
        auto& aggregation = result.emplace_back();
        aggregation.output = key;
        if (const auto* aggregation_config = caf::get_if<std::string>(&value)) {
          aggregation.function_name = *aggregation_config;
          aggregation.input_extractors = {key};
          continue;
        }
        if (const auto* aggregation_config = caf::get_if<record>(&value)) {
          if (aggregation_config->size() != 1)
            return caf::make_error(ec::invalid_configuration,
                                   fmt::format("unexpected config key: "
                                               "more than one aggregation "
                                               "function specified for {}",
                                               aggregation.output));
          const auto& [key, value] = *aggregation_config->begin();
          aggregation.function_name = key;
          if (const auto* extractor = caf::get_if<std::string>(&value)) {
            aggregation.input_extractors = {*extractor};
            continue;
          }
          if (const auto* extractors = caf::get_if<list>(&value)) {
            aggregation.input_extractors.reserve(extractors->size());
            for (const auto& extractor_data : *extractors) {
              if (const auto* extractor
                  = caf::get_if<std::string>(&extractor_data)) {
                aggregation.input_extractors.push_back(*extractor);
                continue;
              }
              return caf::make_error(ec::invalid_configuration,
                                     fmt::format("unexpected config key: "
                                                 "extractor {} for {} is "
                                                 "not a string",
                                                 extractor_data,
                                                 aggregation.output));
            }
            continue;
          }
          continue;
        }
        return caf::make_error(ec::invalid_configuration,
                               fmt::format("unexpected config key: "
                                           "aggregation configured for {} "
                                           "is not a string or a record",
                                           aggregation.output));
      }
      return result;
    }
    return caf::make_error(ec::invalid_configuration,
                           fmt::format("unexpected config key: aggregate "
                                       "{} is not a record",
                                       config));
  }
};

/// A group-by column that was bound to a given schema.
struct group_by_column {
  /// Creates a set of group-by columns by binding the configuration to a
  /// given schema.
  /// @param schema The schema to bind to.
  /// @param config The configuration to bind.
  static caf::expected<std::vector<group_by_column>>
  make(const type& schema, const configuration& config) {
    auto result = std::vector<group_by_column>{};
    const auto& schema_rt = caf::get<record_type>(schema);
    for (const auto& extractor : config.group_by_extractors) {
      for (auto offset :
           schema_rt.resolve_key_suffix(extractor, schema.name())) {
        auto field = schema_rt.field(offset);
        auto& column = result.emplace_back();
        column.input = offset;
        column.time_resolution
          = config.time_resolution.has_value()
                && caf::holds_alternative<time_type>(field.type)
              ? *config.time_resolution
              : std::optional<duration>{};
        column.name = schema_rt.key(offset);
        column.type = field.type;
      }
    }
    if (result.empty())
      return caf::make_error(ec::invalid_configuration,
                             fmt::format("group-by extractors {} did not "
                                         "resolve for schema {}",
                                         config.group_by_extractors, schema));
    std::sort(result.begin(), result.end(),
              [](const group_by_column& lhs,
                 const group_by_column& rhs) noexcept {
                return lhs.input < rhs.input;
              });
    result.erase(std::unique(result.begin(), result.end()), result.end());
    return result;
  }

  /// Compare two group-by columns for equality. This is required for
  /// deduplication of group-by columns.
  friend bool
  operator==(const group_by_column& lhs, const group_by_column& rhs) noexcept {
    return lhs.input == rhs.input;
  }

  /// The offset describing the input column.
  offset input = {};

  /// The optional time resolution for time columns.
  std::optional<duration> time_resolution = {};

  /// The output field's name.
  std::string name = {};

  /// The output field's type.
  class type type = {};
};

/// An aggregation column that was bound to a given schema.
struct aggregation_column {
  /// Creates a set of aggregation columns by binding the configuration to a
  /// given schema.
  /// @param schema The schema to bind to.
  /// @param config The configuration to bind.
  static caf::expected<std::vector<aggregation_column>>
  make(const type& schema, const configuration& config) {
    auto result = std::vector<aggregation_column>{};
    const auto& schema_rt = caf::get<record_type>(schema);
    for (const auto& aggregation : config.aggregations) {
      // Sanity-check that the configured aggregation function actually
      // exists.
      const auto* aggregation_function
        = plugins::find<aggregation_function_plugin>(aggregation.function_name);
      if (!aggregation_function)
        return caf::make_error(ec::invalid_configuration,
                               fmt::format("unknown aggregation function {}",
                                           aggregation.function_name));
      auto inputs = std::vector<offset>{};
      for (const auto& extractor : aggregation.input_extractors) {
        for (auto offset :
             schema_rt.resolve_key_suffix(extractor, schema.name()))
          inputs.push_back(std::move(offset));
      }
      // If we did not find any input columns we can skip this aggregation for
      // the current schema and don't need to configure it further at all.
      if (inputs.empty())
        continue;
      std::sort(inputs.begin(), inputs.end());
      inputs.erase(std::unique(inputs.begin(), inputs.end()), inputs.end());
      // Check that all columns resolve to the same (pruned) type.
      const auto prune = [](const class type& type) noexcept {
        return caf::visit(
          [](const concrete_type auto& pruned) noexcept {
            return vast::type{pruned};
          },
          type);
      };
      auto input_type = prune(schema_rt.field(inputs.front()).type);
      const auto type_mismatch = std::any_of(
        inputs.begin() + 1, inputs.end(), [&](const offset& input) noexcept {
          return input_type != prune(schema_rt.field(input).type);
        });
      if (type_mismatch)
        return caf::make_error(ec::invalid_configuration,
                               fmt::format("aggregation function {} cannot "
                                           "operate on fields {} of "
                                           "mismatching types",
                                           aggregation.function_name,
                                           aggregation.input_extractors));
      // Attempt to create the aggregation once ahead of time to check if that
      // actually is supported for the input type.
      auto instance
        = aggregation_function->make_aggregation_function(input_type);
      if (!instance)
        return caf::make_error(ec::invalid_configuration,
                               fmt::format("aggregation function {} failed "
                                           "to instantiate for input type "
                                           "{}: {}",
                                           aggregation.function_name,
                                           input_type, instance.error()));
      auto output_type = (*instance)->output_type();
      if (!output_type)
        return caf::make_error(
          ec::logic_error, fmt::format("aggregation function {} returned null "
                                       "type output for input type {}",
                                       aggregation.function_name, input_type));
      auto& column = result.emplace_back();
      column.function_name = aggregation.function_name;
      column.inputs = std::move(inputs);
      column.input_type = std::move(input_type);
      column.output_name = aggregation.output;
      column.output_type = std::move(output_type);
    }
    return result;
  }

  /// The name of the aggregation function to load.
  std::string function_name = {};

  /// The offsets describing the input columns.
  std::vector<offset> inputs = {};

  /// The input field's pruned type.
  class type input_type = {};

  /// The output field's name.
  std::string output_name = {};

  /// The output field's type.
  class type output_type = {};
};

/// The key by which aggregations are grouped. Essentially, this is a vector of
/// data. We create a new type here to support a custom hash and equality
/// operation to support lookups with non-materialized keys.
struct group_by_key : std::vector<data> {
  using vector::vector;
};

/// A view on a group-by key.
struct group_by_key_view : std::vector<data_view> {
  using vector::vector;

  /// Materializes a view on a group-by key.
  /// @param views The group-by key view to materialize.
  friend group_by_key materialize(const group_by_key_view& views) {
    auto result = group_by_key{};
    result.reserve(views.size());
    for (const auto& view : views)
      result.push_back(materialize(view));
    return result;
  }
};

/// The hash functor for enabling use of *group_by_key* as a key in unordered
/// map data structures with transparent lookup.
struct group_by_key_hash {
  size_t operator()(const group_by_key& x) const noexcept {
    auto hasher = xxh64{};
    for (const auto& value : x)
      hash_append(hasher, make_view(value));
    return hasher.finish();
  }

  size_t operator()(const group_by_key_view& x) const noexcept {
    auto hasher = xxh64{};
    for (const auto& value : x)
      hash_append(hasher, value);
    return hasher.finish();
  }
};

/// The equality functor for enabling use of *group_by_key* as a key in
/// unordered map data structures with transparent lookup.
struct group_by_key_equal {
  using is_transparent = void;

  bool
  operator()(const group_by_key_view& x, const group_by_key& y) const noexcept {
    return std::equal(x.begin(), x.end(), y.begin(), y.end(),
                      [](const auto& lhs, const auto& rhs) {
                        return lhs == make_view(rhs);
                      });
  }

  bool
  operator()(const group_by_key& x, const group_by_key_view& y) const noexcept {
    return std::equal(x.begin(), x.end(), y.begin(), y.end(),
                      [](const auto& lhs, const auto& rhs) {
                        return make_view(lhs) == rhs;
                      });
  }

  bool operator()(const group_by_key& x, const group_by_key& y) const noexcept {
    return x == y;
  }

  bool operator()(const group_by_key_view& x,
                  const group_by_key_view& y) const noexcept {
    return x == y;
  }
};

/// A configured aggregation that is bound to a single schema.
class aggregation {
public:
  /// The buckets to aggregate into. Essentially, this is an ordered list of
  /// aggregation functions which are incrementally fed input from rows with
  /// matching group-by keys.
  using bucket = std::vector<std::unique_ptr<aggregation_function>>;

  /// Create an aggregation by binding the summarize pipeline operator
  /// configuration to a given schema.
  [[nodiscard]] static caf::expected<aggregation>
  make(const type& schema, const configuration& config) noexcept {
    auto group_by_columns = group_by_column::make(schema, config);
    if (!group_by_columns)
      return group_by_columns.error();
    auto aggregation_columns = aggregation_column::make(schema, config);
    if (!aggregation_columns)
      return aggregation_columns.error();
    auto result = aggregation{};
    result.group_by_columns = std::move(*group_by_columns);
    result.aggregation_columns = std::move(*aggregation_columns);
    result.output_schema = [&]() noexcept -> type {
      auto fields = std::vector<record_type::field_view>{};
      fields.reserve(result.group_by_columns.size()
                     + result.aggregation_columns.size());
      for (const auto& column : result.group_by_columns)
        fields.emplace_back(column.name, column.type);
      for (const auto& column : result.aggregation_columns)
        fields.emplace_back(column.output_name, column.output_type);
      return {schema.name(), record_type{fields}};
    }();
    return result;
  }

  /// Aggregate a batch.
  /// @param batch The record batch to aggregate. Must exactly match the
  /// configured schema.
  void add(const std::shared_ptr<arrow::RecordBatch>& batch) {
    VAST_ASSERT(batch);
    // Determine the inputs only once ahead of time.
    const auto group_by_arrays = make_group_by_arrays(*batch);
    const auto aggregation_arrays = make_aggregation_arrays(*batch);
    // Helper lambda for updating a bucket.
    const auto update_bucket = [&](bucket& bucket, int offset,
                                   int length) noexcept {
      VAST_ASSERT(length > 0);
      if (length == 1) {
        for (size_t column = 0; column < aggregation_columns.size(); ++column) {
          for (const auto& array : aggregation_arrays[column])
            bucket[column]->add(
              value_at(aggregation_columns[column].input_type, *array, offset));
        }
      } else {
        for (size_t column = 0; column < aggregation_columns.size(); ++column) {
          for (const auto& array : aggregation_arrays[column])
            bucket[column]->add(*array->Slice(offset, length));
        }
      }
    };
    // A key view used to determine the bucket for the current row.
    auto reusable_key_view = group_by_key_view{};
    reusable_key_view.resize(group_by_columns.size(), {});
    // Helper lambda for finding a bucket, or creating a new one lazily.
    const auto find_or_create_bucket = [&](int row) -> bucket* {
      for (size_t column = 0; column < group_by_columns.size(); ++column)
        reusable_key_view[column] = value_at(group_by_columns[column].type,
                                             *group_by_arrays[column], row);
      if (auto bucket = buckets.find(reusable_key_view);
          bucket != buckets.end())
        return bucket.value().get();
      auto new_bucket = std::make_shared<bucket>();
      new_bucket->reserve(aggregation_columns.size());
      for (const auto& column : aggregation_columns) {
        auto function
          = plugins::find<aggregation_function_plugin>(column.function_name)
              ->make_aggregation_function(column.input_type);
        // We check whether it's possible to create the aggregation function for
        // the column's input type ahead of time, so there's no need to check
        // again here.
        VAST_ASSERT(function);
        new_bucket->push_back(std::move(*function));
      }
      auto [it, inserted] = buckets.emplace(materialize(reusable_key_view),
                                            std::move(new_bucket));
      VAST_ASSERT(inserted);
      return it.value().get();
    };
    // Iterate over all rows of the batch, and determine a sliding window of
    // rows beloging to the same batch as large as possible, and then update the
    // corresponding bucket.
    auto first_row = 0;
    auto* first_bucket = find_or_create_bucket(first_row);
    VAST_ASSERT(batch->num_rows() > 0);
    for (auto row = 1; row < batch->num_rows(); ++row) {
      auto* bucket = find_or_create_bucket(row);
      if (bucket == first_bucket)
        continue;
      update_bucket(*first_bucket, first_row, row - first_row);
      first_row = row;
      first_bucket = bucket;
    }
    update_bucket(*first_bucket, first_row,
                  detail::narrow_cast<int>(batch->num_rows()) - first_row);
  }

  /// Finish the buckets into a new batch.
  [[nodiscard]] caf::expected<table_slice> finish() {
    VAST_ASSERT(output_schema);
    auto builder = caf::get<record_type>(output_schema)
                     .make_arrow_builder(arrow::default_memory_pool());
    VAST_ASSERT(builder);
    const auto num_rows = detail::narrow_cast<int>(buckets.size());
    const auto reserve_status = builder->Reserve(num_rows);
    if (!reserve_status.ok())
      return caf::make_error(ec::system_error,
                             fmt::format("failed to reserve: {}",
                                         reserve_status.ToString()));
    for (auto [key, bucket] : std::exchange(buckets, {})) {
      const auto append_row_status = builder->Append();
      if (!append_row_status.ok())
        return caf::make_error(ec::system_error,
                               fmt::format("failed to append row: {}",
                                           append_row_status.ToString()));
      for (size_t column = 0; column < key.size(); ++column) {
        const auto append_status = append_builder(
          group_by_columns[column].type,
          *builder->field_builder(detail::narrow_cast<int>(column)),
          make_data_view(key[column]));
        if (!append_status.ok())
          return caf::make_error(ec::system_error,
                                 fmt::format("failed to append grouped {}: {}",
                                             key[column],
                                             append_status.ToString()));
      }
      for (size_t column = 0; column < bucket->size(); ++column) {
        auto value = std::move(*(*bucket)[column]).finish();
        if (!value)
          return value.error();
        const auto append_status
          = append_builder(aggregation_columns[column].output_type,
                           *builder->field_builder(
                             detail::narrow_cast<int>(key.size() + column)),
                           make_data_view(*value));
        if (!append_status.ok())
          return caf::make_error(
            ec::system_error, fmt::format("failed to append aggregated {}: {}",
                                          *value, append_status.ToString()));
      }
    }
    auto array = builder->Finish();
    if (!array.ok())
      return caf::make_error(ec::system_error,
                             fmt::format("failed to finish: {}",
                                         array.status().ToString()));
    auto batch = arrow::RecordBatch::Make(
      output_schema.to_arrow_schema(), num_rows,
      caf::get<type_to_arrow_array_t<record_type>>(*array.MoveValueUnsafe())
        .fields());
    return table_slice{batch, output_schema};
  }

private:
  /// Read the input arrays for the configured group-by columns.
  /// @param batch The record batch to extract from.
  arrow::ArrayVector
  make_group_by_arrays(const arrow::RecordBatch& batch) noexcept {
    auto result = arrow::ArrayVector{};
    result.reserve(group_by_columns.size());
    for (const auto& group_by_column : group_by_columns) {
      auto array = group_by_column.input.get(batch);
      if (group_by_column.time_resolution) {
        array = arrow::compute::FloorTemporal(
                  array,
                  make_round_temporal_options(*group_by_column.time_resolution))
                  .ValueOrDie()
                  .make_array();
      }
      result.push_back(std::move(array));
    }
    return result;
  };

  /// Read the input arrays for the configured aggregation columns.
  /// @param batch The record batch to extract from.
  std::vector<arrow::ArrayVector>
  make_aggregation_arrays(const arrow::RecordBatch& batch) noexcept {
    auto result = std::vector<arrow::ArrayVector>{};
    result.reserve(aggregation_columns.size());
    for (const auto& column : aggregation_columns) {
      auto sub_result = arrow::ArrayVector{};
      sub_result.reserve(column.inputs.size());
      for (const auto& input : column.inputs)
        sub_result.push_back(input.get(batch));
      result.push_back(std::move(sub_result));
    }
    return result;
  };

  /// The configured and bound group-by columns.
  std::vector<group_by_column> group_by_columns = {};

  /// The configured and bound aggregation columns.
  std::vector<aggregation_column> aggregation_columns = {};

  /// The output schema.
  type output_schema = {};

  /// The buckets for the ongoing aggregation.
  tsl::robin_map<group_by_key, std::shared_ptr<bucket>, group_by_key_hash,
                 group_by_key_equal>
    buckets = {};
};


/// The summarize pipeline operator implementation.
class summarize_operator final
  : public schematic_operator<summarize_operator, std::optional<aggregation>> {
public:
  /// Creates a pipeline operator from its configuration.
  /// @param config The parsed configuration of the summarize operator.
  explicit summarize_operator(configuration config) noexcept
    : config_{std::move(config)} {
    // nop
  }

  auto initialize(const type& schema, operator_control_plane&) const
    -> caf::expected<state_type> override {
    auto result = aggregation::make(schema, config_);
    if (!result) {
      VAST_WARN("summarize operator does not apply to schema {} and discards "
                "events: {}",
                schema, result.error());
      return std::nullopt;
    }
    return *result;
  }

  auto process(table_slice slice, state_type& state) const
    -> output_type override {
    if (state) {
      state->add(to_record_batch(slice));
    }
    return {};
  }

  auto finish(std::unordered_map<type, state_type> states,
              operator_control_plane& ctrl) const
    -> generator<output_type> override {
    for (auto& [_, state] : states) {
      if (state) {
        if (auto batch = state->finish()) {
          co_yield std::move(*batch);
        } else {
          ctrl.abort(batch.error());
          break;
        }
      }
    }
  }

  auto to_string() const -> std::string override {
    auto result = fmt::format("summarize");
    bool first = true;
    for (auto& aggr : config_.aggregations) {
      auto rhs = fmt::format("{}({})", aggr.function_name,
                             fmt::join(aggr.input_extractors, ","));
      if (first) {
        first = false;
        result += ' ';
      } else {
        result += ", ";
      }
      if (rhs == aggr.output) {
        result += fmt::format("{}", aggr.output, rhs);
      } else {
        result += fmt::format("{}={}", aggr.output, rhs);
      }
    }
    result
      += fmt::format(" by {}", fmt::join(config_.group_by_extractors, ", "));
    if (config_.time_resolution) {
      result += fmt::format(" resolution {}", *config_.time_resolution);
    }
    return result;
  }

private:
  /// The underlying configuration of the summary transformation.
  configuration config_ = {};
};

/// The summarize pipeline operator plugin.
class plugin final : public virtual operator_plugin {
public:
  caf::error initialize([[maybe_unused]] const record& plugin_config,
                        [[maybe_unused]] const record& global_config) override {
    // Native plugins never have any plugin-specific configuration, so there's
    // nothing to do here.
    return {};
  }

  [[nodiscard]] std::string name() const override {
    return "summarize";
  };

  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    using parsers::end_of_pipeline_operator, parsers::required_ws_or_comment,
      parsers::optional_ws_or_comment, parsers::duration,
      parsers::extractor_list, parsers::aggregation_function_list;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    const auto p = required_ws_or_comment >> aggregation_function_list
                   >> required_ws_or_comment >> ("by") >> required_ws_or_comment
                   >> extractor_list >> -(required_ws_or_comment >> "resolution"
                                          >> required_ws_or_comment >> duration)
                   >> optional_ws_or_comment >> end_of_pipeline_operator;
    std::tuple<std::vector<std::tuple<caf::optional<std::string>, std::string,
                                      std::vector<std::string>>>,
               std::vector<std::string>, std::optional<vast::duration>>
      parsed_aggregations{};
    if (!p(f, l, parsed_aggregations)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to parse "
                                                      "summarize "
                                                      "operator: '{}'",
                                                      pipeline)),
      };
    }
    auto config = configuration{};
    for (const auto& [output, function_name, arguments] :
         std::get<0>(parsed_aggregations)) {
      configuration::aggregation new_aggregation{};
      if (!plugins::find<aggregation_function_plugin>(function_name)) {
        return {
          std::string_view{f, l},
          caf::make_error(ec::syntax_error, fmt::format("invalid aggregation "
                                                        "name: '{}'",
                                                        function_name)),
        };
      }
      new_aggregation.function_name = function_name;
      new_aggregation.input_extractors = arguments;
      new_aggregation.output
        = (output)
            ? *output
            : fmt::format("{}({})", function_name, fmt::join(arguments, ","));
      config.aggregations.push_back(std::move(new_aggregation));
    }
    config.group_by_extractors = std::move(std::get<1>(parsed_aggregations));
    config.time_resolution = std::move(std::get<2>(parsed_aggregations));

    return {
      std::string_view{f, l},
      std::make_unique<summarize_operator>(std::move(config)),
    };
  }
};

} // namespace

} // namespace vast::plugins::summarize

VAST_REGISTER_PLUGIN(vast::plugins::summarize::plugin)

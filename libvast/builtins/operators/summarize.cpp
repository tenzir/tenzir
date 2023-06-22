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
#include <vast/detail/zip_iterator.hpp>
#include <vast/error.hpp>
#include <vast/hash/hash_append.hpp>
#include <vast/operator_control_plane.hpp>
#include <vast/parser_interface.hpp>
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
auto make_round_temporal_options(duration time_resolution) noexcept
  -> arrow::compute::RoundTemporalOptions {
#define TRY_CAST_EXACTLY(chrono_unit, arrow_unit)                              \
  do {                                                                         \
    if (auto cast_resolution = std::chrono::duration_cast<                     \
          std::chrono::duration<int, std::chrono::chrono_unit::period>>(       \
          time_resolution);                                                    \
        time_resolution                                                        \
        == std::chrono::duration_cast<duration>(cast_resolution)) {            \
      return arrow::compute::RoundTemporalOptions{                             \
        cast_resolution.count(),                                               \
        arrow::compute::CalendarUnit::arrow_unit,                              \
      };                                                                       \
    }                                                                          \
  } while (false)
  TRY_CAST_EXACTLY(years, YEAR);
  TRY_CAST_EXACTLY(months, MONTH);
  TRY_CAST_EXACTLY(weeks, WEEK);
  TRY_CAST_EXACTLY(days, DAY);
  TRY_CAST_EXACTLY(hours, HOUR);
  TRY_CAST_EXACTLY(minutes, MINUTE);
  TRY_CAST_EXACTLY(seconds, SECOND);
  TRY_CAST_EXACTLY(milliseconds, MILLISECOND);
  TRY_CAST_EXACTLY(microseconds, MICROSECOND);
  TRY_CAST_EXACTLY(nanoseconds, NANOSECOND);
#undef TRY_CAST_EXACTLY
  // If neither of these casts are working, then we need nanosecond resolution
  // but have a value so large that it cannot be represented by a signed 32-bit
  // integer. In this case we accept the rounding error and take the closest
  // unit we can without overflow.
#define TRY_CAST_APPROXIMATELY(chrono_unit, arrow_unit)                        \
  do {                                                                         \
    if (auto cast_resolution = std::chrono::duration_cast<                     \
          std::chrono::duration<uint64_t, std::chrono::chrono_unit::period>>(  \
          time_resolution);                                                    \
        cast_resolution.count() <= std::numeric_limits<int>::max()) {          \
      return arrow::compute::RoundTemporalOptions{                             \
        static_cast<int>(cast_resolution.count()),                             \
        arrow::compute::CalendarUnit::arrow_unit,                              \
      };                                                                       \
    }                                                                          \
  } while (false)
  TRY_CAST_APPROXIMATELY(nanoseconds, NANOSECOND);
  TRY_CAST_APPROXIMATELY(microseconds, MICROSECOND);
  TRY_CAST_APPROXIMATELY(milliseconds, MILLISECOND);
  TRY_CAST_APPROXIMATELY(seconds, SECOND);
  TRY_CAST_APPROXIMATELY(minutes, MINUTE);
  TRY_CAST_APPROXIMATELY(hours, HOUR);
  TRY_CAST_APPROXIMATELY(days, DAY);
  TRY_CAST_APPROXIMATELY(weeks, WEEK);
  TRY_CAST_APPROXIMATELY(months, MONTH);
  TRY_CAST_APPROXIMATELY(years, YEAR);
#undef TRY_CAST_APPROXIMATELY
  die("unreachable");
}

/// The configuration of a summarize pipeline operator.
struct configuration {
  /// The configuration of a single aggregation.
  struct aggregation {
    /// The output field name.
    std::string output;

    /// The aggregation function.
    const aggregation_function_plugin* function;

    /// Unresolved input extractor.
    std::string input;

    friend auto inspect(auto& f, aggregation& x) -> bool {
      auto get = [&]() {
        return x.function->name();
      };
      auto set = [&](std::string_view name) {
        x.function = plugins::find<aggregation_function_plugin>(name);
        return x.function != nullptr;
      };
      return f.object(x).fields(f.field("output", x.output),
                                f.field("function", get, set),
                                f.field("input", x.input));
    }
  };

  /// Unresolved group-by extractors.
  std::vector<std::string> group_by_extractors = {};

  /// Resolution for time-columns in the group-by columns.
  std::optional<duration> time_resolution = {};

  /// Configuration for aggregation columns.
  std::vector<aggregation> aggregations = {};

  friend auto inspect(auto& f, configuration& x) -> bool {
    return f.object(x).fields(f.field("group_by_extractors",
                                      x.group_by_extractors),
                              f.field("time_resolution", x.time_resolution),
                              f.field("aggregations", x.aggregations));
  }
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

struct column {
  struct offset offset;
  class type type;
};

/// Stores offsets and types of group-by and aggregation columns.
struct binding {
  std::vector<std::optional<column>> group_by_columns;
  std::vector<std::optional<column>> aggregation_columns;

  /// Try to resolve all aggregation and group-by columns for a given schema.
  static auto make(const type& schema, const configuration& config)
    -> caf::expected<binding> {
    auto result = binding{};
    result.group_by_columns.reserve(config.group_by_extractors.size());
    result.aggregation_columns.reserve(config.aggregations.size());
    auto const& rt = caf::get<record_type>(schema);
    for (auto const& field : config.group_by_extractors) {
      if (auto offset = rt.resolve_key(field)) {
        auto type = rt.field(*offset).type;
        result.group_by_columns.emplace_back(
          column{std::move(*offset), std::move(type)});
      } else {
        result.group_by_columns.emplace_back(std::nullopt);
      }
    }
    for (auto const& aggr : config.aggregations) {
      if (auto offset = rt.resolve_key(aggr.input)) {
        auto type = rt.field(*offset).type;
        // Check that the type of this field is compatible with the function
        // ahead of time. We will use that we know that it is compatiable later
        // when instantiating the function for a given group.
        auto instantiation = aggr.function->make_aggregation_function(type);
        if (!instantiation) {
          return caf::make_error(
            ec::type_clash,
            fmt::format("could not instantiate `{}` with `{}`: {}",
                        aggr.function->name(), type, instantiation.error()));
        }
        result.aggregation_columns.emplace_back(
          column{std::move(*offset), std::move(type)});
      } else {
        result.aggregation_columns.emplace_back(std::nullopt);
      }
    }
    return result;
  };

  /// Read the input arrays for the configured group-by columns.
  auto make_group_by_arrays(const arrow::RecordBatch& batch,
                            const configuration& config) const
    -> std::vector<std::optional<std::shared_ptr<arrow::Array>>> {
    auto result = std::vector<std::optional<std::shared_ptr<arrow::Array>>>{};
    result.reserve(group_by_columns.size());
    for (const auto& column : group_by_columns) {
      if (column) {
        auto array = column->offset.get(batch);
        if (config.time_resolution
            && caf::holds_alternative<time_type>(column->type)) {
          array = arrow::compute::FloorTemporal(
                    array, make_round_temporal_options(*config.time_resolution))
                    .ValueOrDie()
                    .make_array();
        }
        result.emplace_back(std::move(array));
      } else {
        result.emplace_back(std::nullopt);
      }
    }
    return result;
  };

  /// Read the input arrays for the configured aggregation columns.
  auto make_aggregation_arrays(const arrow::RecordBatch& batch) const
    -> std::vector<std::optional<std::shared_ptr<arrow::Array>>> {
    auto result = std::vector<std::optional<std::shared_ptr<arrow::Array>>>{};
    result.reserve(aggregation_columns.size());
    for (const auto& column : aggregation_columns) {
      if (column) {
        result.emplace_back(column->offset.get(batch));
      } else {
        result.emplace_back(std::nullopt);
      }
    }
    return result;
  };
};

/// An instantiation of the inter-schematic aggregation process.
class aggregation {
public:
  /// Divides the input into groups and feeds it to the aggregation function.
  auto add(const table_slice& slice, const configuration& config)
    -> caf::error {
    // Step 1: Resolve extractor names (if possible).
    auto it = bindings.find(slice.schema());
    if (it == bindings.end()) {
      auto bound = binding::make(slice.schema(), config);
      if (!bound) {
        return bound.error();
      }
      it = bindings.try_emplace(it, slice.schema(), std::move(*bound));
    }
    auto const& bound = it->second;
    // Step 2: Collect the aggregation columns and group-by columns into arrays.
    auto batch = to_record_batch(slice);
    auto group_by_arrays = bound.make_group_by_arrays(*batch, config);
    auto aggregation_arrays = bound.make_aggregation_arrays(*batch);
    // A key view used to determine the bucket for a single row.
    auto reusable_key_view = group_by_key_view{};
    reusable_key_view.resize(bound.group_by_columns.size(), {});
    // Returns the group that the given row belongs to, creating new groups
    // whenever necessary.
    auto find_or_create_bucket = [&](int64_t row) -> caf::expected<bucket*> {
      for (size_t col = 0; col < bound.group_by_columns.size(); ++col) {
        if (bound.group_by_columns[col]) {
          VAST_ASSERT(group_by_arrays[col].has_value());
          reusable_key_view[col] = value_at(bound.group_by_columns[col]->type,
                                            **group_by_arrays[col], row);
        } else {
          VAST_ASSERT(!group_by_arrays[col].has_value());
          reusable_key_view[col] = caf::none;
        }
      }
      if (auto it = buckets.find(reusable_key_view); it != buckets.end()) {
        auto&& bucket = *it->second;
        // Check that the group-by values also have matching types.
        VAST_ASSERT(bucket.group_by_types.size()
                    == bound.group_by_columns.size());
        for (auto&& [existing, other] :
             detail::zip{bucket.group_by_types, bound.group_by_columns}) {
          if (!other) {
            // If this group-by column does not exist in the input schema, we
            // assume `null` values and ignore its type.
            continue;
          }
          if (other->type != existing) {
            // If the types mismatch, we have found equal values with not-equal
            // types. This can happen e.g. for `null` values. TODO: Prune type?
            if (existing == type{}) {
              // In this case, we found a bucket for a schema which does not
              // have the group-by extractor at all (hence the type is also
              // `null`). Thus, we can just change the type.
              static_assert(std::is_reference_v<decltype(existing)>);
              existing = other->type;
            } else {
              return caf::make_error(
                ec::type_clash,
                fmt::format("summarize found matching group for key `{}`, but "
                            "the existing type `{}` clashes with `{}`",
                            reusable_key_view, existing, other->type));
            }
          }
        }
        // Check that the aggregation extractors have the same type.
        VAST_ASSERT(bucket.functions.size()
                    == bound.aggregation_columns.size());
        for (auto&& [func, column] :
             detail::zip{bucket.functions, bound.aggregation_columns}) {
          auto func_type = func ? func->input : type{};
          auto col_type = column ? column->type : type{};
          if (func_type != col_type) {
            return caf::make_error(
              ec::type_clash,
              fmt::format("summarize aggregation function for group `{}` "
                          "expected type `{}`, but got `{}`",
                          reusable_key_view, func_type, col_type));
          }
        }
        return it->second.get();
      }
      // Did not find existing bucket, create a new one.
      auto new_bucket = std::make_shared<bucket>();
      new_bucket->group_by_types.reserve(bound.group_by_columns.size());
      for (auto&& column : bound.group_by_columns) {
        if (column) {
          new_bucket->group_by_types.push_back(column->type);
        } else {
          new_bucket->group_by_types.emplace_back();
        }
      }
      new_bucket->functions.reserve(bound.aggregation_columns.size());
      for (auto col = size_t{0}; col < bound.aggregation_columns.size();
           ++col) {
        // If this aggregation column exists, we create an instance of the
        // aggregation function with the type of the column. If it does not
        // exist, we store `std::nullopt` instead of an aggregation function, as
        // we will later use this as a signal to set the result column to null.
        if (bound.aggregation_columns[col]) {
          auto input_type = bound.aggregation_columns[col]->type;
          auto instance
            = config.aggregations[col].function->make_aggregation_function(
              input_type);
          // We checked whether it's possible to create the aggregation function
          // for the column type when binding the schema.
          VAST_ASSERT(instance);
          new_bucket->functions.emplace_back(
            function{std::move(input_type), std::move(*instance)});
        } else {
          new_bucket->functions.emplace_back(std::nullopt);
        }
      }
      auto [it, inserted] = buckets.emplace(materialize(reusable_key_view),
                                            std::move(new_bucket));
      VAST_ASSERT(inserted);
      return it.value().get();
    };
    // This lambda is called for consecutive rows that belong to the same group
    // and updates its aggregation functions.
    auto update_bucket = [&](bucket& bucket, int64_t offset,
                             int64_t length) noexcept {
      VAST_ASSERT(length > 0);
      VAST_ASSERT(bucket.functions.size() == aggregation_arrays.size());
      VAST_ASSERT(bucket.functions.size() == bound.aggregation_columns.size());
      for (size_t col = 0; col < bound.aggregation_columns.size(); ++col) {
        if (bucket.functions[col]) {
          VAST_ASSERT(aggregation_arrays[col]);
          bucket.functions[col]->func->add(
            *(*aggregation_arrays[col])->Slice(offset, length));
        } else {
          VAST_ASSERT(!aggregation_arrays[col]);
        }
      }
    };
    // Step 3: Iterate over all rows of the batch, and determine a slidin window
    // of rows beloging to the same batch that is as large as possible, then
    // update the corresponding bucket.
    auto first_row = int64_t{0};
    auto first_bucket = find_or_create_bucket(first_row);
    if (!first_bucket) {
      return std::move(first_bucket.error());
    }
    VAST_ASSERT(slice.rows() > 0);
    for (auto row = int64_t{1}; row < detail::narrow<int64_t>(slice.rows());
         ++row) {
      auto bucket = find_or_create_bucket(row);
      if (!bucket) {
        return std::move(first_bucket.error());
      }
      if (*bucket == *first_bucket)
        continue;
      update_bucket(**first_bucket, first_row, row - first_row);
      first_row = row;
      first_bucket = bucket;
    }
    update_bucket(**first_bucket, first_row,
                  detail::narrow<int64_t>(slice.rows()) - first_row);
    return {};
  }

  /// Returns the summarization results after the input is done.
  auto finish(
    const configuration& config) && -> generator<caf::expected<table_slice>> {
    // TODO: Must summarizations yield events with equal output schemas. The
    // code below will create a single table slice for each group, but we could
    // use this knowledge to create batches instead.
    for (auto&& [group, bucket] : buckets) {
      VAST_ASSERT(config.aggregations.size() == bucket->functions.size());
      // When building the output schema, we use the `string` type if the
      // associated column was not present in the input schema. This is because
      // we have to pick a type for the `null` values.
      auto output_schema = std::invoke([&]() noexcept -> type {
        auto fields = std::vector<record_type::field_view>{};
        fields.reserve(config.group_by_extractors.size()
                       + config.aggregations.size());
        for (auto&& [extractor, group_type] :
             detail::zip(config.group_by_extractors, bucket->group_by_types)) {
          fields.emplace_back(
            extractor, group_type == type{} ? type{string_type{}} : group_type);
        }
        for (auto&& [aggr, aggr_col] :
             detail::zip(config.aggregations, bucket->functions)) {
          fields.emplace_back(aggr.output, aggr_col
                                             ? aggr_col->func->output_type()
                                             : type{string_type{}});
        }
        return {"tenzir.summarize", record_type{fields}};
      });
      auto builder = caf::get<record_type>(output_schema)
                       .make_arrow_builder(arrow::default_memory_pool());
      VAST_ASSERT(builder);
      auto status = builder->Append();
      if (!status.ok()) {
        co_yield caf::make_error(ec::system_error,
                                 fmt::format("failed to append row: {}",
                                             status.ToString()));
        co_return;
      }
      VAST_ASSERT(bucket->group_by_types.size() == group.size());
      for (auto i = size_t{0}; i < group.size(); ++i) {
        auto col = detail::narrow<int>(i);
        // TODO: group_by_types
        auto ty = bucket->group_by_types[i];
        if (ty == type{}) {
          ty = type{string_type{}};
        }
        status = append_builder(ty, *builder->field_builder(col),
                                make_data_view(group[i]));
        if (!status.ok()) {
          co_yield caf::make_error(
            ec::system_error,
            fmt::format("failed to append group value: {}", status.ToString()));
          co_return;
        }
      }
      for (auto i = size_t{0}; i < bucket->functions.size(); ++i) {
        auto col = detail::narrow<int>(group.size() + i);
        if (bucket->functions[i]) {
          auto& func = bucket->functions[i]->func;
          auto output_type = func->output_type();
          auto value = std::move(*func).finish();
          if (!value) {
            co_yield std::move(value.error());
            co_return;
          }
          status = append_builder(output_type, *builder->field_builder(col),
                                  make_data_view(*value));
        } else {
          status = builder->field_builder(col)->AppendNull();
        }
        if (!status.ok()) {
          co_yield caf::make_error(ec::system_error,
                                   fmt::format("failed to append aggregation "
                                               "value: {}",
                                               status.ToString()));
          co_return;
        }
      }
      auto array = builder->Finish();
      if (!array.ok()) {
        co_yield caf::make_error(ec::system_error,
                                 fmt::format("failed to finish builder: {}",
                                             array.status().ToString()));
        co_return;
      }
      auto batch = arrow::RecordBatch::Make(
        output_schema.to_arrow_schema(), 1,
        caf::get<type_to_arrow_array_t<record_type>>(*array.MoveValueUnsafe())
          .fields());
      co_yield table_slice{batch, output_schema};
    }
  }

private:
  /// An `aggregation_function` together with its parameter type.
  struct function {
    type input;
    std::unique_ptr<aggregation_function> func;
  };

  /// The buckets to aggregate into. Essentially, this is an ordered list of
  /// aggregation functions which are incrementally fed input from rows with
  /// matching group-by keys. We also store the types of the `group_by` clause.
  /// This is because we use only the underlying data for lookup, but need their
  /// type to add the data to the output.
  struct bucket {
    std::vector<type> group_by_types;
    std::vector<std::optional<function>> functions;
  };

  /// We cache the offsets and types of the resolved columns for each schema.
  tsl::robin_map<type, binding> bindings = {};

  /// The buckets for the ongoing aggregation.
  tsl::robin_map<group_by_key, std::shared_ptr<bucket>, group_by_key_hash,
                 group_by_key_equal>
    buckets = {};
};

/// The summarize pipeline operator implementation.
class summarize_operator final : public crtp_operator<summarize_operator> {
public:
  summarize_operator() = default;

  /// Creates a pipeline operator from its configuration.
  /// @param config The parsed configuration of the summarize operator.
  explicit summarize_operator(configuration config) noexcept
    : config_{std::move(config)} {
    // nop
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto aggr = aggregation{};
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      if (auto error = aggr.add(slice, config_)) {
        ctrl.abort(std::move(error));
        co_return;
      }
    }
    for (auto&& result : std::move(aggr).finish(config_)) {
      if (!result) {
        ctrl.abort(std::move(result.error()));
        co_return;
      }
      co_yield std::move(*result);
    }
  }

  auto to_string() const -> std::string override {
    auto result = fmt::format("summarize");
    bool first = true;
    for (auto& aggr : config_.aggregations) {
      auto rhs = fmt::format("{}({})", aggr.function->name(), aggr.input);
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

  auto name() const -> std::string override {
    return "summarize";
  }

  friend auto inspect(auto& f, summarize_operator& x) -> bool {
    return f.apply(x.config_);
  }

private:
  /// The underlying configuration of the summary transformation.
  configuration config_ = {};
};

/// The summarize pipeline operator plugin.
class plugin final : public virtual operator_plugin<summarize_operator> {
public:
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
                                      std::string>>,
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
    for (const auto& [output, function_name, argument] :
         std::get<0>(parsed_aggregations)) {
      configuration::aggregation new_aggregation{};
      auto const* function
        = plugins::find<aggregation_function_plugin>(function_name);
      if (!function) {
        return {
          std::string_view{f, l},
          caf::make_error(ec::syntax_error, fmt::format("invalid "
                                                        "aggregation "
                                                        "function `{}`",
                                                        function_name)),
        };
      }
      new_aggregation.function = function;
      new_aggregation.input = argument;
      new_aggregation.output
        = (output) ? *output : fmt::format("{}({})", function_name, argument);
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

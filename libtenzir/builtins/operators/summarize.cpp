//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/aggregation_function.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/arrow_time_utils.hpp>
#include <tenzir/concept/convertible/data.hpp>
#include <tenzir/concept/convertible/to.hpp>
#include <tenzir/concept/parseable/core.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/concept/parseable/tenzir/time.hpp>
#include <tenzir/detail/zip_iterator.hpp>
#include <tenzir/error.hpp>
#include <tenzir/hash/hash_append.hpp>
#include <tenzir/operator_control_plane.hpp>
#include <tenzir/parser_interface.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/table_slice_builder.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/registry.hpp>
#include <tenzir/type.hpp>

#include <arrow/compute/api_scalar.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <caf/expected.hpp>
#include <caf/sum_type.hpp>
#include <tsl/robin_map.h>

#include <algorithm>
#include <ranges>
#include <utility>
#include <variant>

namespace tenzir::plugins::summarize {

namespace {

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

  /// Maximum lifetime of a bucket, counted from its creation and last update,
  /// respectively.
  std::optional<duration> created_timeout = {};
  std::optional<duration> update_timeout = {};

  /// Configuration for aggregation columns.
  std::vector<aggregation> aggregations = {};

  friend auto inspect(auto& f, configuration& x) -> bool {
    return f.object(x).fields(f.field("group_by_extractors",
                                      x.group_by_extractors),
                              f.field("time_resolution", x.time_resolution),
                              f.field("created_timeout", x.created_timeout),
                              f.field("update_timeout", x.update_timeout),
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
    for (const auto& view : views) {
      result.push_back(materialize(view));
    }
    return result;
  }
};

/// The hash functor for enabling use of *group_by_key* as a key in unordered
/// map data structures with transparent lookup.
struct group_by_key_hash {
  size_t operator()(const group_by_key& x) const noexcept {
    auto hasher = xxh64{};
    for (const auto& value : x) {
      hash_append(hasher, make_view(value));
    }
    return hasher.finish();
  }

  size_t operator()(const group_by_key_view& x) const noexcept {
    auto hasher = xxh64{};
    for (const auto& value : x) {
      hash_append(hasher, value);
    }
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

  /// Resolve all aggregation and group-by columns for a given schema.
  static auto make(const type& schema, const configuration& config,
                   diagnostic_handler& diag) -> binding {
    auto result = binding{};
    result.group_by_columns.reserve(config.group_by_extractors.size());
    result.aggregation_columns.reserve(config.aggregations.size());
    auto const& rt = as<record_type>(schema);
    for (auto const& field : config.group_by_extractors) {
      if (auto offset = schema.resolve_key_or_concept_once(field)) {
        auto type = rt.field(*offset).type;
        result.group_by_columns.emplace_back(
          column{std::move(*offset), std::move(type)});
      } else {
        diagnostic::warning("group-by column `{}` does not exist for schema "
                            "`{}`",
                            field, schema.name())
          .emit(diag);
        result.group_by_columns.emplace_back(std::nullopt);
      }
    }
    for (auto const& aggr : config.aggregations) {
      auto resolved
        = std::invoke([&]() -> std::optional<std::pair<offset, type>> {
            if (aggr.input == ".") {
              // We already checked for `count` earlier. Note that we are using
              // the "wrong type" here. The `.` extractor should have type
              // `schema`, but we later on will use a `int64` array as we cannot
              // resolve to the outermost record yet. Furthermore, this implies
              // that `count(.)` works across multiple schemas.
              TENZIR_ASSERT(aggr.function->name() == "count");
              return {{{}, type{int64_type{}}}};
            } else if (auto offset
                       = schema.resolve_key_or_concept_once(aggr.input)) {
              auto type = rt.field(*offset).type;
              return {{std::move(*offset), std::move(type)}};
            } else {
              return std::nullopt;
            }
          });
      if (resolved) {
        auto& [offset, type] = *resolved;
        // Check that the type of this field is compatible with the function
        // ahead of time. We only use this to emit a warning. We do not set the
        // column to `std::nullopt`, because we will have to differentiate the
        // error and the missing case later on.
        auto instantiation = aggr.function->make_aggregation_function(type);
        if (!instantiation) {
          diagnostic::warning(
            "cannot instantiate `{}` with `{}` for schema `{}`: {}",
            aggr.function->name(), type, schema.name(), instantiation.error())
            .emit(diag);
        }
        result.aggregation_columns.emplace_back(
          column{std::move(offset), std::move(type)});
      } else {
        diagnostic::warning("aggregation column `{}` does not exist for schema "
                            "`{}`",
                            aggr.input, schema.name())
          .emit(diag);
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
        if (column->offset.empty()) {
          // This can currently only happen for `count(.)`. We cannot resolve an
          // empty offset to an `arrow::Array`. Instead, we create a fake
          // `int64` array with the right length. We want to remove this hack as
          // part of the expression revamp.
          auto builder = arrow::Int64Builder{};
          auto status = builder.AppendEmptyValues(batch.num_rows());
          TENZIR_ASSERT(status.ok());
          auto array = builder.Finish();
          TENZIR_ASSERT(array.ok());
          result.emplace_back(array.MoveValueUnsafe());
        } else {
          result.emplace_back(column->offset.get(batch));
        }
      } else {
        result.emplace_back(std::nullopt);
      }
    }
    return result;
  };
};

/// An instantiation of the inter-schematic aggregation process.
class implementation {
public:
  /// Divides the input into groups and feeds it to the aggregation function.
  void add(const table_slice& slice, const configuration& config,
           diagnostic_handler& diag) {
    // Step 1: Resolve extractor names (if possible).
    auto it = bindings.find(slice.schema());
    if (it == bindings.end()) {
      it = bindings.try_emplace(it, slice.schema(),
                                binding::make(slice.schema(), config, diag));
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
    auto find_or_create_bucket = [&](int64_t row) -> bucket* {
      for (size_t col = 0; col < bound.group_by_columns.size(); ++col) {
        if (bound.group_by_columns[col]) {
          TENZIR_ASSERT(group_by_arrays[col].has_value());
          reusable_key_view[col] = value_at(bound.group_by_columns[col]->type,
                                            **group_by_arrays[col], row);
        } else {
          TENZIR_ASSERT(!group_by_arrays[col].has_value());
          reusable_key_view[col] = caf::none;
        }
      }
      if (auto it = buckets.find(reusable_key_view); it != buckets.end()) {
        auto&& bucket = *it->second;
        // Check that the group-by values also have matching types.
        for (auto [existing, other] :
             detail::zip_equal(bucket.group_by_types, bound.group_by_columns)) {
          if (!other) {
            // If this group-by column does not exist in the input schema, we
            // already warned and can ignore it.
            continue;
          }
          if (not other->type) {
            // We can skip `null_type` as that is compatible with every other.
            continue;
          }
          if (existing.is_dead()) {
            continue;
          }
          if (existing.is_empty()) {
            // If the group-by column did not have a type before (because the
            // column was missing when the group was created), we can set it here.
            existing.set_active(other->type);
            continue;
          }
          auto existing_type = existing.get_active();
          if (other->type == existing_type) {
            // No conflict, nothing to do.
            continue;
          }
          // Otherwise, there is a type mismatch for the same data. This can
          // only happen with `null` or metadata mismatches.
          auto pruned = existing_type.prune();
          if (other->type.prune() == pruned) {
            // If the type mismatch is only caused by metadata, we remove
            // it. This for example can unify `:port` and `:uint64` into
            // `:uint64`, which we consider an acceptable conversion.
            existing.set_active(std::move(pruned));
          } else {
            // Otherwise, we have a bucket (and thus matching data) where
            // the types are conflicting. This can only happen if the
            // conflicting group columns both have `null` values.
            diagnostic::warning("summarize found matching group for key `{}`, "
                                "but the existing type `{}` clashes with `{}`",
                                reusable_key_view, existing_type, other->type)
              .emit(diag);
            existing.set_dead();
          }
        }
        // Check that the aggregation extractors have the same type.
        for (auto&& [aggr, column, cfg] :
             detail::zip_equal(bucket.aggregations, bound.aggregation_columns,
                               config.aggregations)) {
          if (aggr.is_dead()) {
            continue;
          }
          if (!column) {
            // We already warned that this column does not exist. Since we
            // assume `null` values for it, and also assume that `nulls` don't
            // change the function value, we ignore it.
            continue;
          }
          if (aggr.is_empty()) {
            // We can now instantiate the missing function because we have a type.
            if (auto instance
                = cfg.function->make_aggregation_function(column->type)) {
              aggr.set_active(std::move(*instance));
            } else {
              // We already noticed this and emitted a warning previously.
              aggr.set_dead();
            }
            continue;
          }
          auto& func = aggr.get_active();
          TENZIR_ASSERT(func);
          if (func->input_type() != column->type) {
            diagnostic::warning("summarize aggregation function for group `{}` "
                                "expected type `{}`, but got `{}`",
                                reusable_key_view, func->input_type(),
                                column->type)
              .emit(diag);
            aggr.set_dead();
          }
        }
        return it->second.get();
      }
      // Did not find existing bucket, create a new one.
      auto new_bucket = std::make_shared<bucket>();
      new_bucket->group_by_types.reserve(bound.group_by_columns.size());
      for (auto&& column : bound.group_by_columns) {
        if (column and column->type) {
          new_bucket->group_by_types.push_back(
            group_type::make_active(column->type));
        } else {
          new_bucket->group_by_types.push_back(group_type::make_empty());
        }
      }
      new_bucket->aggregations.reserve(bound.aggregation_columns.size());
      for (auto col = size_t{0}; col < bound.aggregation_columns.size();
           ++col) {
        // If this aggregation column exists, we create an instance of the
        // aggregation function with the type of the column. If it does not
        // exist, we store `std::nullopt` instead of an aggregation function, as
        // we will later use this as a signal to set the result column to null.
        if (bound.aggregation_columns[col].has_value()) {
          auto input_type = bound.aggregation_columns[col]->type;
          if (auto instance
              = config.aggregations[col].function->make_aggregation_function(
                input_type)) {
            new_bucket->aggregations.push_back(
              aggregation::make_active(std::move(*instance)));
          } else {
            // We already emitted a warning for this earlier.
            new_bucket->aggregations.push_back(aggregation::make_dead());
          }
        } else {
          // If the column does not exist, we cannot instantiate the function
          // yet because we don't know which type to use.
          new_bucket->aggregations.emplace_back(aggregation::make_empty());
        }
      }
      auto [it, inserted] = buckets.emplace(materialize(reusable_key_view),
                                            std::move(new_bucket));
      TENZIR_ASSERT(inserted);
      return it.value().get();
    };
    // This lambda is called for consecutive rows that belong to the same group
    // and updates its aggregation functions.
    auto update_bucket = [&](bucket& bucket, int64_t offset, int64_t length) {
      bucket.updated_at = std::chrono::steady_clock::now();
      for (auto [aggr, input] :
           detail::zip_equal(bucket.aggregations, aggregation_arrays)) {
        if (!input) {
          // If the input column does not exist, we have nothing to do.
          continue;
        }
        if (!aggr.is_active()) {
          // If the aggregation is dead, we have nothing to do. If it is
          // empty, we know that the aggregation column does not exist in
          // this schema, and thus have nothing to do as well. The only
          // remaining case to handle is where it is a function.
          continue;
        }
        aggr.get_active()->add(*(*input)->Slice(offset, length));
      }
    };
    // Step 3: Iterate over all rows of the batch, and determine a sliding
    // window of rows belonging to the same batch that is as large as possible,
    // then update the corresponding bucket.
    auto first_row = int64_t{0};
    auto* first_bucket = find_or_create_bucket(first_row);
    TENZIR_ASSERT(slice.rows() > 0);
    for (auto row = int64_t{1}; row < detail::narrow<int64_t>(slice.rows());
         ++row) {
      auto* bucket = find_or_create_bucket(row);
      if (bucket == first_bucket) {
        continue;
      }
      update_bucket(*first_bucket, first_row, row - first_row);
      first_row = row;
      first_bucket = bucket;
    }
    update_bucket(*first_bucket, first_row,
                  detail::narrow<int64_t>(slice.rows()) - first_row);
  }

  auto check_timeouts(const configuration& config)
    -> generator<caf::expected<table_slice>> {
    if (not config.created_timeout and not config.update_timeout) {
      co_return;
    }
    const auto now = std::chrono::steady_clock::now();
    auto copy = implementation{};
    if (config.created_timeout) {
      const auto threshold = now - *config.created_timeout;
      for (const auto& [key, bucket] : buckets) {
        if (bucket->created_at < threshold) {
          copy.buckets.try_emplace(key, bucket);
        }
      }
    }
    if (config.update_timeout) {
      TENZIR_ASSERT(config.update_timeout);
      const auto threshold = now - *config.update_timeout;
      for (const auto& [key, bucket] : buckets) {
        if (bucket->updated_at < threshold) {
          copy.buckets.try_emplace(key, bucket);
        }
      }
    }
    if (copy.buckets.empty()) {
      co_return;
    }
    for (const auto& [key, _] : copy.buckets) {
      const auto num_erased = buckets.erase(key);
      TENZIR_ASSERT(num_erased == 1);
    }
    for (auto&& result : std::move(copy).finish(config)) {
      co_yield std::move(result);
    }
  }

  /// Returns the summarization results after the input is done.
  auto finish(
    const configuration& config) && -> generator<caf::expected<table_slice>> {
    if (config.group_by_extractors.empty() && buckets.empty()) {
      // This `summarize` has no `by` clause. In the case where the operator
      // did not receive any input, the user still expects a result. For
      // example, `summarize count(foo)` should return 0.
      auto b = series_builder{};
      auto r = b.record();
      for (auto& aggr : config.aggregations) {
        r.field(aggr.output, aggr.function->aggregation_default());
      }
      for (auto&& slice : b.finish_as_table_slice("tenzir.summarize")) {
        co_yield std::move(slice);
      }
      co_return;
    }
    // Most summarizations yield events with equal output schemas. Hence, we
    // first "group the groups" by their output schema, and then create one
    // builder with potentially multiple rows for each output schema.
    auto output_schemas
      = tsl::robin_map<type, std::vector<decltype(buckets)::iterator>>{};
    for (auto it = buckets.begin(); it != buckets.end(); ++it) {
      const auto& bucket = it->second;
      TENZIR_ASSERT(config.aggregations.size() == bucket->aggregations.size());
      auto fields = std::vector<record_type::field_view>{};
      fields.reserve(config.group_by_extractors.size()
                     + config.aggregations.size());
      for (auto&& [extractor, group] : detail::zip_equal(
             config.group_by_extractors, bucket->group_by_types)) {
        fields.emplace_back(extractor, group.is_active() ? group.get_active()
                                                         : type{null_type{}});
      }
      for (auto&& [aggr, cfg] :
           detail::zip_equal(bucket->aggregations, config.aggregations)) {
        // Same as above.
        fields.emplace_back(cfg.output, aggr.is_active()
                                          ? aggr.get_active()->output_type()
                                          : type{null_type{}});
      }
      auto output_schema = type{"tenzir.summarize", record_type{fields}};
      // This creates a new entry if it does not exist yet.
      output_schemas[std::move(output_schema)].push_back(it);
    }
    for (const auto& [output_schema, groups] : output_schemas) {
      auto builder = as<record_type>(output_schema)
                       .make_arrow_builder(arrow::default_memory_pool());
      TENZIR_ASSERT(builder);
      for (auto it : groups) {
        const auto& group = it->first;
        const auto& bucket = it->second;
        auto status = builder->Append();
        if (!status.ok()) {
          co_yield caf::make_error(ec::system_error,
                                   fmt::format("failed to append row: {}",
                                               status.ToString()));
          co_return;
        }
        // Assign data of group-by fields.
        for (auto i = size_t{0}; i < group.size(); ++i) {
          auto col = detail::narrow<int>(i);
          auto ty = as<record_type>(output_schema).field(i).type;
          status = append_builder(ty, *builder->field_builder(col),
                                  make_data_view(group[i]));
          if (!status.ok()) {
            co_yield caf::make_error(
              ec::system_error, fmt::format("failed to append group value: {}",
                                            status.ToString()));
            co_return;
          }
        }
        // Assign data of aggregations.
        for (auto i = size_t{0}; i < bucket->aggregations.size(); ++i) {
          auto col = detail::narrow<int>(group.size() + i);
          if (bucket->aggregations[i].is_active()) {
            auto& func = bucket->aggregations[i].get_active();
            auto output_type = func->output_type();
            auto value = std::move(*func).finish();
            if (!value) {
              // TODO: We could warn instead and insert `null`.
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
      }
      auto array = builder->Finish();
      if (!array.ok()) {
        co_yield caf::make_error(ec::system_error,
                                 fmt::format("failed to finish builder: {}",
                                             array.status().ToString()));
        co_return;
      }
      auto batch = arrow::RecordBatch::Make(
        output_schema.to_arrow_schema(), detail::narrow<int64_t>(groups.size()),
        as<type_to_arrow_array_t<record_type>>(*array.MoveValueUnsafe())
          .fields());
      co_yield table_slice{batch, output_schema};
    }
  }

private:
  /// This class takes a `T` that is contextually convertible to `bool`. It
  /// exposes three states: The state is `empty` if the underlying value is
  /// false. This class does not allow access to the value in that case. Other
  /// values of `T` correspond to the state `active`. This class also adds a
  /// third state, `dead`, which also does not allow accessing the value.
  ///
  /// To show how this is used, let us consider the aggregation columns, which
  /// use `T = std::unique_ptr<aggregation_function>`.
  ///
  /// - `dead`: There was an error, which we only get if there was a type clash
  ///   in the input columns. We never change away from this state once we are
  ///   there. The result of the aggregation will be `null`.
  ///
  /// - `active: An active aggregation function for a specific type. Can
  ///   change to `dead` if an error occurs.
  ///
  /// - `empty`: If we create a group, but the input column is missing, then we
  ///   don't know how to instantiate the function yet. This state can change to
  ///   `function` once the group receives a schema where the column exists. If
  ///   the aggregation stays `empty` until the end, we emit `null`.
  template <class T>
  class dead_empty_or {
  public:
    static auto make_dead() -> dead_empty_or {
      return dead_empty_or{std::nullopt};
    }

    static auto make_empty() -> dead_empty_or {
      return dead_empty_or{T{}};
    }

    static auto make_active(T x) -> dead_empty_or {
      TENZIR_ASSERT(x);
      return dead_empty_or{std::move(x)};
    }

    auto is_dead() const -> bool {
      return !state_.has_value();
    }

    auto is_active() const -> bool {
      return state_ && *state_;
    }

    auto is_empty() const -> bool {
      return state_ && !*state_;
    }

    void set_active(T x) {
      TENZIR_ASSERT(x);
      state_ = std::move(x);
    }

    auto get_active() -> T& {
      TENZIR_ASSERT(is_active());
      return *state_;
    }

    void set_dead() {
      state_ = std::nullopt;
    }

  private:
    explicit dead_empty_or(std::optional<T> state) : state_{std::move(state)} {
    }

    std::optional<T> state_;
  };

  using group_type = dead_empty_or<type>;
  using aggregation = dead_empty_or<std::unique_ptr<aggregation_function>>;

  /// The buckets to aggregate into. Essentially, this is an ordered list of
  /// aggregation functions which are incrementally fed input from rows with
  /// matching group-by keys. We also store the types of the `group_by` clause.
  /// This is because we use only the underlying data for lookup, but need their
  /// type to add the data to the output.
  struct bucket {
    /// The type of the grouping extractors, where `type{}` denotes a missing
    /// column (which can get upgraded to another type if we encounter a column
    /// that has a `null` value but exists), and `std::nullopt` denotes a type
    /// conflict (which always results in `null` and cannot get upgraded.)
    std::vector<group_type> group_by_types;

    /// The aggregation column functions. The optional is empty if there was an
    /// error that forces the output to be `null`, for example because there was
    /// a type clash between columns. We store `nullptr` if we have only seen
    /// schemas where the input column is missing, which means that we don't
    /// know which type to use until we get schema where the column exists.
    std::vector<aggregation> aggregations;

    /// The time when this bucket was created and last updated, respectively.
    std::chrono::steady_clock::time_point created_at
      = std::chrono::steady_clock::now();
    std::chrono::steady_clock::time_point updated_at
      = std::chrono::steady_clock::now();
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
    co_yield {};
    auto impl = implementation{};
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        for (auto&& result : impl.check_timeouts(config_)) {
          if (not result) {
            diagnostic::error(result.error()).emit(ctrl.diagnostics());
            co_return;
          }
          co_yield std::move(*result);
        }
        co_yield {};
        continue;
      }
      impl.add(slice, config_, ctrl.diagnostics());
    }
    for (auto&& result : std::move(impl).finish(config_)) {
      if (not result) {
        diagnostic::error(result.error()).emit(ctrl.diagnostics());
        co_return;
      }
      co_yield std::move(*result);
    }
  }

  auto name() const -> std::string override {
    return "summarize";
  }

  auto idle_after() const -> duration override {
    // Returning zero here is technically incorrect when using summarize with
    // timeouts. However, the handling of input-independent non-source operators
    // in the execution nodes is so bad, that we accept a potential delay here
    // over excess CPU usage.
    // TODO: Fix this properly in the execution nodes.
    return duration::zero();
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    // Note: The `unordered` relies on commutativity of the aggregation functions.
    (void)filter, (void)order;
    return optimize_result{std::nullopt, event_order::unordered, copy()};
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
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    using parsers::end_of_pipeline_operator, parsers::required_ws_or_comment,
      parsers::optional_ws_or_comment, parsers::duration,
      parsers::extractor_list, parsers::aggregation_function_list;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    const auto p = required_ws_or_comment >> aggregation_function_list
                   >> -(required_ws_or_comment >> "by" >> required_ws_or_comment
                        >> extractor_list)
                   >> -(required_ws_or_comment >> "resolution"
                        >> required_ws_or_comment >> duration)
                   >> -(required_ws_or_comment >> "timeout"
                        >> required_ws_or_comment >> duration)
                   >> -(required_ws_or_comment >> "update-timeout"
                        >> required_ws_or_comment >> duration)
                   >> optional_ws_or_comment >> end_of_pipeline_operator;
    std::tuple<std::vector<std::tuple<caf::optional<std::string>, std::string,
                                      std::string>>,
               std::vector<std::string>, std::optional<tenzir::duration>,
               std::optional<tenzir::duration>, std::optional<tenzir::duration>>
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
      if (argument == ".") {
        if (function_name != "count") {
          return {
            std::string_view{f, l},
            caf::make_error(ec::syntax_error,
                            fmt::format("the `.` extractor is currently not "
                                        "supported for `{}`",
                                        function_name)),
          };
        }
      }
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
      auto new_aggregation = configuration::aggregation{};
      new_aggregation.function = function;
      new_aggregation.input = argument;
      new_aggregation.output
        = (output) ? *output : fmt::format("{}({})", function_name, argument);
      config.aggregations.push_back(std::move(new_aggregation));
    }
    config.group_by_extractors = std::move(std::get<1>(parsed_aggregations));
    config.time_resolution = std::move(std::get<2>(parsed_aggregations));
    config.created_timeout = std::move(std::get<3>(parsed_aggregations));
    config.update_timeout = std::move(std::get<4>(parsed_aggregations));
    if (config.time_resolution and config.group_by_extractors.empty()) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, "found `resolution` specifier "
                                          "without `by` clause"),
      };
    }
    return {
      std::string_view{f, l},
      std::make_unique<summarize_operator>(std::move(config)),
    };
  }
};

struct aggregate_t {
  std::optional<ast::simple_selector> dest;
  ast::function_call call;

  friend auto inspect(auto& f, aggregate_t& x) -> bool {
    return f.object(x).fields(f.field("dest", x.dest), f.field("call", x.call));
  }
};

struct group_t {
  std::optional<ast::simple_selector> dest;
  ast::simple_selector expr;

  friend auto inspect(auto& f, group_t& x) -> bool {
    return f.object(x).fields(f.field("dest", x.dest), f.field("expr", x.expr));
  }
};

struct config {
  std::vector<aggregate_t> aggregates;
  std::vector<group_t> groups;

  /// Because we allow mixing aggregates and groups and want to emit them in the
  /// same order, we need to store some additional information, unless we use
  /// something like `vector<variant<aggregate_t, ast::selector>>` instead. But
  /// that makes it more tricky to `zip`. If the index is positive, it
  /// corresponds to `aggregates`, otherwise `groups[-index - 1]`.
  std::vector<int64_t> indices;

  friend auto inspect(auto& f, config& x) -> bool {
    return f.object(x).fields(f.field("aggregates", x.aggregates),
                              f.field("groups", x.groups),
                              f.field("indices", x.indices));
  }
};

template <class Value>
using group_map
  = tsl::robin_map<group_by_key, Value, group_by_key_hash, group_by_key_equal>;

struct bucket2 {
  std::vector<std::unique_ptr<aggregation_instance>> aggregations{};
};

class implementation2 {
public:
  explicit implementation2(const config& cfg, session ctx)
    : cfg_{cfg}, ctx_{ctx} {
  }

  auto make_bucket() -> std::unique_ptr<bucket2> {
    auto bucket = std::make_unique<bucket2>();
    for (const auto& aggr : cfg_.aggregates) {
      // We already checked the cast and instantiation before.
      const auto* fn
        = dynamic_cast<const aggregation_plugin*>(&ctx_.reg().get(aggr.call));
      TENZIR_ASSERT(fn);
      bucket->aggregations.push_back(
        fn->make_aggregation(aggregation_plugin::invocation{aggr.call}, ctx_)
          .unwrap());
    }
    return bucket;
  }

  void add(const table_slice& slice) {
    auto group_values = std::vector<series>{};
    for (auto& group : cfg_.groups) {
      group_values.push_back(eval(group.expr.inner(), slice, ctx_));
    }
    auto key = group_by_key_view{};
    key.resize(cfg_.groups.size());
    auto update_group = [&](bucket2& group, int64_t begin, int64_t end) {
      for (auto&& aggr : group.aggregations) {
        aggr->update(subslice(slice, begin, end), ctx_);
      }
    };
    auto find_or_create_group = [&](int64_t row) -> bucket2* {
      for (auto&& [key_value, group] : detail::zip_equal(key, group_values)) {
        key_value = value_at(group.type, *group.array, row);
      }
      auto it = groups_.find(key);
      if (it == groups_.end()) {
        it = groups_.emplace_hint(it, materialize(key), make_bucket());
      }
      return &*it->second;
    };
    auto total_rows = detail::narrow<int64_t>(slice.rows());
    auto current_group = find_or_create_group(0);
    auto current_begin = int64_t{0};
    for (auto row = int64_t{1}; row < total_rows; ++row) {
      auto group = find_or_create_group(row);
      if (current_group != group) {
        update_group(*current_group, current_begin, row);
        current_group = group;
        current_begin = row;
      }
    }
    update_group(*current_group, current_begin, total_rows);
  }

  auto finish() -> std::vector<table_slice> {
    auto emplace
      = [](record& root, const ast::simple_selector& sel, data value) {
          if (sel.path().empty()) {
            // TODO
            if (auto rec = try_as<record>(&value)) {
              root = std::move(*rec);
            }
            return;
          }
          auto current = &root;
          for (auto& segment : sel.path()) {
            auto& val = (*current)[segment.name];
            if (&segment == &sel.path().back()) {
              val = std::move(value);
            } else {
              current = try_as<record>(&val);
              if (not current) {
                val = record{};
                current = &as<record>(val);
              }
            }
          }
        };
    const auto finish_group = [&](const auto& key, const auto& group) {
      auto result = record{};
      for (auto index : cfg_.indices) {
        if (index >= 0) {
          auto& dest = cfg_.aggregates[index].dest;
          auto value = group->aggregations[index]->get();
          if (dest) {
            emplace(result, *dest, value);
          } else {
            auto& call = cfg_.aggregates[index].call;
            // TODO: Decide and properly implement this.
            auto arg = std::invoke([&]() -> std::string {
              if (call.args.empty()) {
                return "";
              }
              if (call.args.size() > 1) {
                return "...";
              }
              auto sel = ast::simple_selector::try_from(call.args[0]);
              if (not sel) {
                return "...";
              }
              auto arg = std::string{};
              if (sel->has_this()) {
                arg = "this";
              }
              for (auto& segment : sel->path()) {
                // TODO: This is wrong if the path contains special characters.
                if (not arg.empty()) {
                  arg += '.';
                }
                arg += segment.name;
              }
              return arg;
            });
            result.emplace(fmt::format("{}({})", call.fn.path[0].name, arg),
                           value);
          }
        } else {
          index = -index - 1;
          auto& group = cfg_.groups[index];
          auto& dest = group.dest ? *group.dest : group.expr;
          auto& value = key[index];
          emplace(result, dest, value);
        }
      }
      return result;
    };
    // Special case: if there are no configured groups, and no groups were
    // created because we didn't get any input events, then we create a new
    // bucket and just finish it. That way, `from [] | summarize count()` will
    // return a single event showing a count of zero.
    if (cfg_.groups.empty() and groups_.empty()) {
      auto b = series_builder{};
      b.data(finish_group(group_by_key{}, make_bucket()));
      return b.finish_as_table_slice();
    }
    // TODO: Group by schema again to make this more efficient.
    auto b = series_builder{};
    for (const auto& [key, group] : groups_) {
      b.data(finish_group(key, group));
    }
    return b.finish_as_table_slice();
  }

private:
  const config& cfg_;
  session ctx_;
  group_map<std::unique_ptr<bucket2>> groups_;
};

class summarize_operator2 final : public crtp_operator<summarize_operator2> {
public:
  summarize_operator2() = default;

  explicit summarize_operator2(config cfg) : cfg_{std::move(cfg)} {
  }

  auto name() const -> std::string override {
    return "tql2.summarize";
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    // TODO: Do not create a new session here.
    auto provider = session_provider::make(ctrl.diagnostics());
    auto impl = implementation2{cfg_, provider.as_session()};
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      impl.add(slice);
    }
    for (auto slice : impl.finish()) {
      co_yield std::move(slice);
    }
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, summarize_operator2& x) -> bool {
    return f.apply(x.cfg_);
  }

private:
  config cfg_;
};

class plugin2 final : public operator_plugin2<summarize_operator2> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto cfg = config{};
    auto add_aggregate = [&](std::optional<ast::simple_selector> dest,
                             ast::function_call call) {
      // TODO: Improve this and try to forward function handle directly.
      auto fn = dynamic_cast<const aggregation_plugin*>(&ctx.reg().get(call));
      if (not fn) {
        diagnostic::error("function does not support aggregations")
          .primary(call.fn)
          .hint("if you want to group by this, use assignment before")
          .docs("https://docs.tenzir.com/operators/summarize")
          .emit(ctx);
        return;
      }
      // We test the arguments by making and discarding it. This is a bit
      // hacky and should be improved in the future.
      if (fn->make_aggregation(aggregation_plugin::invocation{call}, ctx)) {
        auto index = detail::narrow<int64_t>(cfg.aggregates.size());
        cfg.indices.push_back(index);
        cfg.aggregates.emplace_back(std::move(dest), std::move(call));
      }
    };
    auto add_group = [&](std::optional<ast::simple_selector> dest,
                         ast::simple_selector expr) {
      auto index = -detail::narrow<int64_t>(cfg.groups.size()) - 1;
      cfg.indices.push_back(index);
      cfg.groups.emplace_back(std::move(dest), std::move(expr));
    };
    for (auto& arg : inv.args) {
      arg.match(
        [&](ast::function_call& arg) {
          add_aggregate(std::nullopt, std::move(arg));
        },
        [&](ast::assignment& arg) {
          auto left = std::get_if<ast::simple_selector>(&arg.left);
          if (not left) {
            // TODO
            diagnostic::error("expected data selector, not meta")
              .primary(arg.left)
              .emit(ctx);
            return;
          }
          arg.right.match(
            [&](ast::function_call& right) {
              add_aggregate(std::move(*left), std::move(right));
            },
            [&](auto&) {
              auto right = ast::simple_selector::try_from(arg.right);
              if (right) {
                add_group(std::move(*left), std::move(*right));
              } else {
                diagnostic::error(
                  "expected selector or aggregation function call")
                  .primary(arg.right)
                  .emit(ctx);
              }
            });
        },
        [&](auto&) {
          auto selector = ast::simple_selector::try_from(arg);
          if (selector) {
            add_group(std::nullopt, std::move(*selector));
          } else {
            diagnostic::error(
              "expected selector, assignment or aggregation function call")
              .primary(arg)
              .emit(ctx);
          }
        });
    }
    return std::make_unique<summarize_operator2>(std::move(cfg));
  }
};

} // namespace

} // namespace tenzir::plugins::summarize

TENZIR_REGISTER_PLUGIN(tenzir::plugins::summarize::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::summarize::plugin2)

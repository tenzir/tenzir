//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/fbs/type.hpp"

#include <vast/arrow_table_slice.hpp>
#include <vast/arrow_table_slice_builder.hpp>
#include <vast/concept/convertible/data.hpp>
#include <vast/concept/convertible/to.hpp>
#include <vast/concept/parseable/to.hpp>
#include <vast/concept/parseable/vast/time.hpp>
#include <vast/data.hpp>
#include <vast/detail/narrow.hpp>
#include <vast/detail/passthrough.hpp>
#include <vast/hash/hash_append.hpp>
#include <vast/plugin.hpp>
#include <vast/transform_step.hpp>
#include <vast/type.hpp>
#include <vast/view.hpp>

#include <arrow/builder.h>
#include <arrow/compute/api.h>
#include <arrow/config.h>
#include <arrow/table.h>
#include <arrow/table_builder.h>
#include <tsl/robin_map.h>

namespace vast {

const fbs::Type* resolve_transparent_prime(const fbs::Type* root,
                                           enum type::transparent transparent
                                           = type::transparent::yes) {
  VAST_ASSERT(root);
  while (transparent == type::transparent::yes) {
    switch (root->type_type()) {
      case fbs::type::Type::NONE:
      case fbs::type::Type::bool_type:
      case fbs::type::Type::integer_type:
      case fbs::type::Type::count_type:
      case fbs::type::Type::real_type:
      case fbs::type::Type::duration_type:
      case fbs::type::Type::time_type:
      case fbs::type::Type::string_type:
      case fbs::type::Type::pattern_type:
      case fbs::type::Type::address_type:
      case fbs::type::Type::subnet_type:
      case fbs::type::Type::enumeration_type:
      case fbs::type::Type::list_type:
      case fbs::type::Type::map_type:
      case fbs::type::Type::record_type:
        transparent = type::transparent::no;
        break;
      case fbs::type::Type::enriched_type:
        root = root->type_as_enriched_type()->type_nested_root();
        VAST_ASSERT(root);
        break;
    }
  }
  return root;
}

detail::generator<record_type::leaf_view>
leaves_prime(const record_type& rt) noexcept {
  auto index = offset{0};
  auto history = detail::stack_vector<const fbs::type::RecordType*, 64>{
    rt.table().type_as_record_type()};
  while (!index.empty()) {
    const auto* record = history.back();
    VAST_ASSERT(record);
    const auto* fields = record->fields();
    VAST_ASSERT(fields);
    // This is our exit condition: If we arrived at the end of a record, we need
    // to step out one layer. We must also reset the target key at this point.
    if (index.back() >= fields->size()) {
      history.pop_back();
      index.pop_back();
      if (!index.empty())
        ++index.back();
      continue;
    }
    const auto* field = record->fields()->Get(index.back());
    VAST_ASSERT(field);
    const auto* field_type
      = resolve_transparent_prime(field->type_nested_root());
    VAST_ASSERT(field_type);
    switch (field_type->type_type()) {
      case fbs::type::Type::NONE:
      case fbs::type::Type::bool_type:
      case fbs::type::Type::integer_type:
      case fbs::type::Type::count_type:
      case fbs::type::Type::real_type:
      case fbs::type::Type::duration_type:
      case fbs::type::Type::time_type:
      case fbs::type::Type::string_type:
      case fbs::type::Type::pattern_type:
      case fbs::type::Type::address_type:
      case fbs::type::Type::subnet_type:
      case fbs::type::Type::enumeration_type:
      case fbs::type::Type::list_type:
      case fbs::type::Type::map_type: {
        auto leaf = record_type::leaf_view{
          {
            field->name()->string_view(),
            type{rt.table_->slice(as_bytes(*field->type()))},
          },
          index,
        };
        co_yield std::move(leaf);
        ++index.back();
        break;
      }
      case fbs::type::Type::record_type: {
        history.emplace_back(field_type->type_as_record_type());
        index.push_back(0);
        break;
      }
      case fbs::type::Type::enriched_type:
        __builtin_unreachable();
        break;
    }
  }
  co_return;
}

record_type flatten_prime(const record_type& type) noexcept {
  auto fields = std::vector<struct record_type::field>{};
  for (const auto& [field, offset] : leaves_prime(type))
    fields.push_back({
      type.key(offset),
      field.type,
    });
  return record_type{fields};
}

type flatten_prime(const type& t) noexcept {
  if (const auto* rt = caf::get_if<record_type>(&t)) {
    auto result = type{flatten_prime(*rt)};
    result.assign_metadata(t);
    return result;
  }
  return t;
}

} // namespace vast

namespace vast::plugins::summarize {

std::shared_ptr<arrow::RecordBatch>
flatten_batch(const vast::type& layout,
              const arrow::RecordBatch& batch) noexcept {
  auto flattened_layout = flatten_prime(layout);
  auto columns = arrow::ArrayVector{};
  columns.reserve(caf::get<record_type>(flattened_layout).num_fields());
  for (auto&& leaf : caf::get<record_type>(layout).leaves()) {
    auto path = std::vector<int>{};
    path.reserve(leaf.index.size());
    for (auto layer : leaf.index)
      path.push_back(detail::narrow_cast<int>(layer));
    auto column = arrow::FieldPath{std::move(path)}.Get(batch);
    VAST_ASSERT(column.ok(), column.status().ToString().c_str());
    columns.push_back(column.MoveValueUnsafe());
  }
  return arrow::RecordBatch::Make(flattened_layout.to_arrow_schema(),
                                  batch.num_rows(), std::move(columns));
}

std::shared_ptr<arrow::RecordBatch>
unflatten_batch(const vast::type& layout,
                const arrow::RecordBatch& batch) noexcept {
  auto columns = arrow::ArrayVector{};
  columns.reserve(caf::get<record_type>(layout).num_fields());
  const auto& flattened_columns = batch.columns();
  auto current_flattened_column = flattened_columns.begin();
  auto f = [&](auto&& f, const type& t) -> std::shared_ptr<arrow::Array> {
    if (const auto* rt = caf::get_if<record_type>(&t)) {
      auto arrays = arrow::ArrayVector{};
      auto fields = arrow::FieldVector{};
      arrays.reserve(rt->num_fields());
      fields.reserve(rt->num_fields());
      for (auto&& field : rt->fields()) {
        arrays.push_back(f(f, field.type));
        fields.push_back(field.type.to_arrow_field(field.name));
      }
      auto column = arrow::StructArray::Make(arrays, fields);
      VAST_ASSERT(column.ok(), column.status().ToString().c_str());
      return column.MoveValueUnsafe();
    }
    VAST_ASSERT(current_flattened_column != flattened_columns.end());
    auto result = *current_flattened_column;
    ++current_flattened_column;
    return result;
  };
  for (auto&& field : caf::get<record_type>(layout).fields())
    columns.push_back(f(f, field.type));
  VAST_ASSERT(current_flattened_column == flattened_columns.end());
  return arrow::RecordBatch::Make(layout.to_arrow_schema(), batch.num_rows(),
                                  std::move(columns));
}

/// The configuration of an summarize transform step.
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

/// The layout-specific state for an summary.
struct summary {
  /// The action to take for a given column. Columns without an action are
  /// dropped as part of the summary.
  enum class action {
    group_by, ///< Group identical values.
    sum,      ///< Accumulate values within the same group.
    min,      ///< Use the minimum value within the same group.
    max,      ///< Use the maximum value within the same group.
    any,      ///< Disjoin values within the same group.
    all,      ///< Conjoin values within the same group.
  };

  /// The key by which summaries are grouped. Essentially, this is a
  /// vector of data.
  struct group_by_key : std::vector<data> {
    using vector::vector;
  };

  struct group_by_key_view : std::vector<data_view> {
    using vector::vector;
  };

  /// The hash functor for enabling use of *group_by_key* as a key in unordered
  /// map data structures.
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

  struct group_by_key_equal {
    using is_transparent = void;

    bool operator()(const group_by_key_view& x,
                    const group_by_key& y) const noexcept {
      return std::equal(x.begin(), x.end(), y.begin(), y.end(),
                        [](const auto& lhs, const auto& rhs) {
                          return lhs == make_view(rhs);
                        });
    }

    bool operator()(const group_by_key& x,
                    const group_by_key_view& y) const noexcept {
      return std::equal(x.begin(), x.end(), y.begin(), y.end(),
                        [](const auto& lhs, const auto& rhs) {
                          return make_view(lhs) == rhs;
                        });
    }

    bool
    operator()(const group_by_key& x, const group_by_key& y) const noexcept {
      return x == y;
    }

    bool operator()(const group_by_key_view& x,
                    const group_by_key_view& y) const noexcept {
      return x == y;
    }
  };

  /// Groups accumulators for slices of incoming record batches with a matching
  /// set of configured set of configured group-by columns.
  using bucket_map = tsl::robin_map<group_by_key, std::vector<data>,
                                    group_by_key_hash, group_by_key_equal>;

  /// Creates a new summary given a configuration and a layout.
  static caf::expected<summary>
  make(const configuration& config, const type& layout) {
    auto result = summary{};
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
                             fmt::format("summary detected ambiguous "
                                         "action configuration for layout {}",
                                         layout));
    auto drop_transformations = std::vector<record_type::transformation>{};
    for (int flat_index = 0; const auto& leaf : rt.leaves()) {
      if (result.selected_columns_.size() >= unflattened_actions.size()
          || leaf.index
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
    result.flattened_adjusted_layout_ = flatten_prime(result.adjusted_layout_);
    result.flattened_adjusted_schema_
      = result.flattened_adjusted_layout_.to_arrow_schema();
    result.num_group_by_columns_ = std::count(
      result.actions_.begin(), result.actions_.end(), action::group_by);
    result.time_resolution_ = config.time_resolution;
    return result;
  }

  /// Summaries are expensive to copy because they hold a lot of state, so we
  /// make them non-copyable. Not because they cannot be copied theoretically,
  /// but because doing so would most certainly be a design issue within the code.
  summary(const summary&) = delete;
  summary& operator=(const summary&) = delete;

  /// Destruction and move-operations can simply be defaulted for an summary.
  ~summary() noexcept = default;
  summary(summary&&) noexcept = default;
  summary& operator=(summary&&) noexcept = default;

  /// Adds a record batch to the summary. Unless disabled, this performs an
  /// eager summary already.
  caf::error add(std::shared_ptr<arrow::RecordBatch> batch) {
    // First, adjust the record batch: We only want to summarize a subset of
    // rows, and the remaining rows can just be dropped eagerly. It is important
    // that we do this first to avoid unnecessary overhead first, and also
    // because all the indices calculated from the configuration in the
    // constructor are for the selected columns only.
    auto select_columns_result = batch->SelectColumns(selected_columns_);
    if (!select_columns_result.ok())
      return caf::make_error(
        ec::unspecified,
        fmt::format("summarize transform failed to select columns: {}",
                    select_columns_result.status().ToString()));
    batch = select_columns_result.MoveValueUnsafe();
    VAST_ASSERT(batch->num_columns()
                == detail::narrow_cast<int>(actions_.size()));
    // Create a builder if we don't have one already.
    if (!builder_) {
      builder_ = caf::get<record_type>(flattened_adjusted_layout_)
                   .make_arrow_builder(arrow::default_memory_pool());
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
            fmt::format("summarize transform failed to round time column {} "
                        "to multiple of {}: {}",
                        batch->column_name(column), *time_resolution_,
                        round_temporal_result.status().ToString()));
        auto set_column_result = batch->SetColumn(
          column, batch->schema()->field(column),
          round_temporal_result.MoveValueUnsafe().make_array());
        if (!set_column_result.ok())
          return caf::make_error(
            ec::unspecified,
            fmt::format("summarize transform failed to replace column: {}",
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
    auto [bucket, next_bucket]
      = try_emplace_bucket(buckets_.end(), batch, start);
    for (auto bucket = next_bucket; next_bucket != buckets_.end();
         bucket = next_bucket) {
      start = next_start;
      do {
        std::tie(bucket, next_bucket)
          = try_emplace_bucket(bucket, batch, ++next_start);
      } while (bucket == next_bucket);
      VAST_ASSERT(bucket != buckets_.end());
      VAST_ASSERT(bucket.value().size() == actions_.size());
      for (int column = 0; column < batch->num_columns(); ++column) {
        auto f = [&]<concrete_type Type>(
                   [[maybe_unused]] const Type& type,
                   [[maybe_unused]] const arrow::Array& array) -> caf::error {
          constexpr auto is_non_primitive_array
            = detail::is_any_v<Type, string_type, pattern_type, address_type,
                               subnet_type, enumeration_type, list_type,
                               map_type, record_type>;
          auto make_non_primitive_error = [&]() {
            return caf::make_error(ec::invalid_configuration,
                                   fmt::format("summarize transform step "
                                               "cannot handle non-primitive "
                                               "field {}",
                                               array.type()->ToString()));
          };
          auto& value = bucket.value()[column];
          for (int row = start; row < next_start; ++row) {
            if (array.IsNull(row))
              continue;
            if (caf::holds_alternative<caf::none_t>(value)) {
              value = materialize(value_at(type, array, row));
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
                } else if constexpr (requires(type_to_data_t<Type> value) {
                                       {value + value};
                                     })
                  value = detail::narrow_cast<type_to_data_t<Type>>(
                    caf::get<type_to_data_t<Type>>(value)
                    + materialize(value_at(type, array, row)));
                else
                  return caf::make_error(
                    ec::invalid_configuration,
                    fmt::format("summarize transform step cannot "
                                "calculate 'sum' of field {}",
                                batch->schema()->field(column)->ToString()));
                break;
              }
              case action::min: {
                if constexpr (is_non_primitive_array) {
                  return make_non_primitive_error();
                } else if constexpr (requires(type_to_data_t<Type> value) {
                                       {value < value};
                                     }) {
                  value = std::min(caf::get<type_to_data_t<Type>>(value),
                                   materialize(value_at(type, array, row)));
                } else {
                  return caf::make_error(
                    ec::invalid_configuration,
                    fmt::format("summarize transform step cannot "
                                "calculate 'min' of field {}",
                                batch->schema()->field(column)->ToString()));
                }
                break;
              }
              case action::max: {
                if constexpr (is_non_primitive_array) {
                  return make_non_primitive_error();
                } else if constexpr (requires(type_to_data_t<Type> value) {
                                       {value > value};
                                     }) {
                  value = std::max(caf::get<type_to_data_t<Type>>(value),
                                   materialize(value_at(type, array, row)));
                } else {
                  return caf::make_error(
                    ec::invalid_configuration,
                    fmt::format("summarize transform step cannot "
                                "calculate 'max' of field {}",
                                batch->schema()->field(column)->ToString()));
                }
                break;
              }
              case action::any: {
                if constexpr (is_non_primitive_array) {
                  return make_non_primitive_error();
                } else if constexpr (std::is_same_v<Type, bool_type>) {
                  value = caf::get<type_to_data_t<Type>>(value)
                          || materialize(value_at(type, array, row));
                } else {
                  return caf::make_error(
                    ec::invalid_configuration,
                    fmt::format("summarize transform step cannot "
                                "calculate 'any' of field {}",
                                batch->schema()->field(column)->ToString()));
                }
                break;
              }
              case action::all: {
                if constexpr (is_non_primitive_array) {
                  return make_non_primitive_error();
                } else if constexpr (std::is_same_v<Type, bool_type>) {
                  value = caf::get<type_to_data_t<Type>>(value)
                          && materialize(value_at(type, array, row));
                } else {
                  return caf::make_error(
                    ec::invalid_configuration,
                    fmt::format("summarize transform step cannot "
                                "calculate 'all' of field {}",
                                batch->schema()->field(column)->ToString()));
                }
                break;
              }
            }
          }
          return caf::none;
        };
        if (auto err
            = caf::visit(f,
                         caf::get<record_type>(flattened_adjusted_layout_)
                           .field(column)
                           .type,
                         detail::passthrough(*batch->column(column))))
          return err;
      }
    }
    return caf::none;
  }

  /// Returns the summarized batches.
  caf::expected<transform_batch> finish() {
    VAST_ASSERT(builder_);
    auto cast_requirements
      = std::vector<std::optional<bool>>(builder_->num_fields(), std::nullopt);
    const auto bucket_size = detail::narrow_cast<int64_t>(buckets_.size());
    auto resize_status = builder_->Reserve(bucket_size);
    VAST_ASSERT(resize_status.ok(), resize_status.ToString().c_str());
    for (auto&& bucket : std::exchange(buckets_, {})) {
      auto row_append_result = builder_->Append();
      VAST_ASSERT(row_append_result.ok(), row_append_result.ToString().c_str());
      for (int column = 0; auto& value : bucket.second) {
        auto* column_builder = builder_->field_builder(column);
        auto append_result = append_builder(
          caf::get<record_type>(flattened_adjusted_layout_).field(column).type,
          *column_builder, make_view(value));
        VAST_ASSERT(append_result.ok(), append_result.ToString().c_str());
        column++;
      }
    }
    auto finish_result
      = std::shared_ptr<arrow::ArrayBuilder>(builder_)->Finish();
    VAST_ASSERT(finish_result.ok(), finish_result.status().ToString().c_str());
    const auto& columns_struct = caf::get<type_to_arrow_array_t<record_type>>(
      *finish_result.ValueUnsafe());
    auto batch = arrow::RecordBatch::Make(flattened_adjusted_schema_,
                                          columns_struct.length(),
                                          columns_struct.fields());
    return transform_batch{adjusted_layout_, std::move(batch)};
  }

private:
  /// Default-constructor for internal use in `make(...)`.
  summary() = default;

  /// Finds or creates the bucket for a given row in a record batch.
  /// The returned pair contains both the newly inserted bucket and the
  std::pair<bucket_map::iterator, bucket_map::iterator>
  try_emplace_bucket(bucket_map::iterator previous_bucket,
                     const std::shared_ptr<arrow::RecordBatch>& batch,
                     int row) {
    // If our row goes beyond the end of the batch, signal that we do not
    // have a bucket.
    if (row >= batch->num_rows())
      return {previous_bucket, buckets_.end()};
    // Create current bucket key.
    auto key_view = group_by_key_view{};
    key_view.reserve(num_group_by_columns_);
    for (int column = 0; column < batch->num_columns(); ++column) {
      if (actions_[column] == action::group_by) {
        key_view.push_back(value_at(
          caf::get<record_type>(flattened_adjusted_layout_).field(column).type,
          *batch->column(column), row));
      }
    }
    // Try to find an existing bucket.
    if (auto iterator = buckets_.find(key_view); iterator != buckets_.end())
      return {previous_bucket, iterator};
    // Create a new bucket.
    auto key = group_by_key{};
    key.reserve(key_view.size());
    for (const auto& x : key_view)
      key.push_back(materialize(x));
    auto value = bucket_map::mapped_type{};
    value.resize(actions_.size(), caf::none);
    if (previous_bucket == buckets_.end()) {
      auto [iterator, inserted]
        = buckets_.insert({std::move(key), std::move(value)});
      VAST_ASSERT(inserted);
      return {buckets_.end(), iterator};
    }
    // Copy the previous bucket's key, then find it again to work around
    // iterator invalidation on insert for the bucket map.
    const auto previous_key = previous_bucket.key();
    auto [iterator, inserted]
      = buckets_.insert({std::move(key), std::move(value)});
    VAST_ASSERT(inserted);
    return {buckets_.find(previous_key), iterator};
  }

  /// The action to take during summary for every individual column in
  /// the incoming record batches.
  std::vector<action> actions_ = {};

  /// The columnns that are selected from the incoming record batches as
  /// part of the data transformation.
  std::vector<int> selected_columns_ = {};

  /// The group-by columns from the record batches that hold time values.
  /// These need to be handled with special care, as we round them to a
  /// multiple of a configured value.
  std::vector<int> round_temporal_columns_ = {};

  /// The duration used as the multiple value when rounding grouped temporal
  /// values.
  std::optional<duration> time_resolution_ = {};

  /// Multiple versions of the adjusted layout with the dropped columns removed
  /// needed throughout the summary.
  type adjusted_layout_ = {};
  type flattened_adjusted_layout_ = {};
  std::shared_ptr<arrow::Schema> flattened_adjusted_schema_ = {};

  /// The buckets holding the intemediate accumulators.
  bucket_map buckets_ = {};

  /// The builder used as part of the summary. Stored since it only
  /// needs to be created once per layout effectively, and we can do so
  /// lazily. NOTE: This is not a single record batch builder because that
  /// does not support extension types. :shrug:
  std::shared_ptr<type_to_arrow_builder_t<record_type>> builder_ = {};

  /// The number of columns to group by.
  size_t num_group_by_columns_ = {};
};

/// The summarize transform step, which holds applies an summary to
/// every incoming record batch, which is configured per-type. The
/// summary configuration is resolved eagerly and then executed eagerly
/// and/or lazily per type.
class summarize_step : public transform_step {
public:
  /// Create a new summarize step from an already parsed configuration.
  explicit summarize_step(configuration config) : config_{std::move(config)} {
    // nop
  }

  /// Marks this transform step as an aggregating transform step.
  [[nodiscard]] bool is_aggregate() const override {
    return true;
  }

  /// Applies the transformation to an Arrow Record Batch with a
  /// corresponding VAST layout; this creates a layout-specific summary
  /// lazily.
  [[nodiscard]] caf::error
  add(type layout, std::shared_ptr<arrow::RecordBatch> batch) override {
    auto summary = summaries_.find(layout);
    if (summary == summaries_.end()) {
      auto make_summary_result = summary::make(config_, layout);
      if (!make_summary_result)
        return make_summary_result.error();
      auto [new_summary, ok]
        = summaries_.try_emplace(layout, std::move(*make_summary_result));
      VAST_ASSERT(ok);
      summary = new_summary;
    }
    batch = flatten_batch(layout, *batch);
    return summary->second.add(std::move(batch));
  }

  /// Retrieves the result of the transformation.
  [[nodiscard]] caf::expected<std::vector<transform_batch>> finish() override {
    auto result = std::vector<transform_batch>{};
    result.reserve(summaries_.size());
    for (auto&& [layout, summary] : summaries_) {
      auto summary_result = summary.finish();
      if (!summary_result)
        return summary_result.error();
      summary_result->batch
        = unflatten_batch(summary_result->layout, *summary_result->batch);
      result.push_back(std::move(*summary_result));
    }
    return result;
  }

private:
  /// The underlying configuration of the transformation.
  configuration config_ = {};

  /// A mapping of layout to the configured summary.
  std::unordered_map<type, summary> summaries_ = {};
};

/// The plugin entrypoint for the summarize transform plugin.
class plugin final : public transform_plugin {
public:
  /// Initializes the summarize plugin. This plugin has no general
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
                           "vast.plugins.summarize");
  }

  /// Returns the unique name of the plugin, which also equals the transform
  /// step name that is used to refer to instantiations of the summarize
  /// step when configuring transforms.
  [[nodiscard]] const char* name() const override {
    return "summarize";
  };

  /// This is called once for every time this transform step appears in a
  /// transform definition. The configuration for the step is opaquely
  /// passed as the first argument.
  [[nodiscard]] caf::expected<std::unique_ptr<transform_step>>
  make_transform_step(const vast::record& options) const override {
    auto config = to<configuration>(options);
    if (!config)
      return config.error();
    return std::make_unique<summarize_step>(std::move(*config));
  }
};

} // namespace vast::plugins::summarize

// Finally, register our plugin.
VAST_REGISTER_PLUGIN(vast::plugins::summarize::plugin)

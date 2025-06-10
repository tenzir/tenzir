//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "clickhouse/transformers.hpp"

#include "clickhouse/arguments.hpp"
#include "tenzir/detail/enumerate.hpp"
#include "tenzir/view3.hpp"

#include <clickhouse/columns/array.h>
#include <clickhouse/columns/date.h>
#include <clickhouse/columns/ip6.h>
#include <clickhouse/columns/nullable.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/tuple.h>

using namespace clickhouse;
using namespace std::string_view_literals;

/// The implementation works via multiple customization points:
/// * Implementing the entire transformer by hand. This is done for Tuple and
///   Array
/// * A common implementation for non-structural types that works via
///   * A trait to handle names and allocations.
///     * This trait is auto-implemented for most types via a X-macro.
///     * It is specialized for some types with special requirements
///   * A `value_transform` function that translates between a tenzir::data
///     value and the expected clickhouse API value.

namespace tenzir::plugins::clickhouse {

namespace {

void emit_unknown_column_warning(const path_type& path,
                                 diagnostic_handler& dh) {
  diagnostic::warning("`{}` does not exist in ClickHouse table",
                      fmt::join(path, "."))
    .note("column will be dropped")
    .emit(dh);
}

void emit_missing_column_warning(const path_type& path,
                                 diagnostic_handler& dh) {
  diagnostic::warning("required column missing in input, event will be dropped")
    .note("column `{}` is missing", fmt::join(path, "."))
    .emit(dh);
}
} // namespace

transformer_record::transformer_record(std::string clickhouse_typename,
                                       schema_transformations transformations)
  : transformer{std::move(clickhouse_typename), true},
    transformations{std::move(transformations)} {
  // We initialized `clickhouse_nullable` with true, but now we need to check if
  // actually all columns are nullable. If they are not, the record isnt nullable
  for (const auto& [_, t] : transformations) {
    if (not t->clickhouse_nullable) {
      clickhouse_nullable = false;
      break;
    }
  }
  found_column.resize(this->transformations.size());
}

auto transformer_record::update_dropmask(
  path_type& path, const tenzir::type& type, const arrow::Array& array,
  dropmask_ref dropmask, tenzir::diagnostic_handler& dh) -> drop {
  if (clickhouse_nullable) {
    return drop::none;
  }
  my_array = &array;
  const auto* rt = try_as<record_type>(type);
  if (not rt) {
    diagnostic::warning("incompatible type for column `{}",
                        fmt::join(path, "."))
      .note("expected `{}`, got `{}`", type_kind{tag_v<record_type>},
            type.kind())
      .emit(dh);
    return drop::all;
  }
  std::fill(found_column.begin(), found_column.end(), false);
  /// Update the dropmask based of the record itself. If we are here, we know
  /// that we cannot null every subcolumn, so a "top level" null requires us
  /// to drop the event.
  for (int64_t i = 0; i < array.length(); ++i) {
    if (array.IsNull(i)) {
      dropmask[i] = true;
    }
  }
  const auto& struct_array = as<arrow::StructArray>(array);
  auto updated = drop::none;
  /// Update the dropmasks from all nested columns
  for (auto [k, t, arr] : columns_of(*rt, struct_array)) {
    path.push_back(k);
    const auto [trafo, out_idx] = transfrom_and_index_for(k);
    if (not trafo) {
      emit_unknown_column_warning(path, dh);
      path.pop_back();
      continue;
    }
    found_column[out_idx] = true;
    updated = updated | trafo->update_dropmask(path, t, arr, dropmask, dh);
    path.pop_back();
    if (updated == drop::all) {
      return drop::all;
    }
  }
  if (clickhouse_nullable) {
    return updated;
  }
  /// Detect missing columns
  for (const auto& [i, kvp] : detail::enumerate(transformations)) {
    const auto& [k, t] = kvp;
    if (found_column[i]) {
      continue;
    }
    if (t->clickhouse_nullable) {
      continue;
    }
    path.push_back(k);
    emit_missing_column_warning(path, dh);
    path.pop_back();
    std::ranges::fill(dropmask, true);
    updated = drop::all;
    break;
  }
  return updated;
}

auto transformer_record::create_null_column(size_t n) const
  -> ::clickhouse::ColumnRef {
  if (not clickhouse_nullable) {
    return nullptr;
  }
  auto columns = std::vector<ColumnRef>(transformations.size());
  for (const auto& [_, t] : transformations) {
    auto c = t->create_null_column(n);
    if (not c) {
      return nullptr;
    }
    columns.push_back(std::move(c));
  }
  return std::make_shared<ColumnTuple>(std::move(columns));
}

auto transformer_record::create_column(
  path_type& path, const tenzir::type& type, const arrow::Array& array,
  dropmask_cref dropmask, int64_t dropcount, tenzir::diagnostic_handler& dh)
  -> ::clickhouse::ColumnRef {
  auto columns = std::vector<ColumnRef>(transformations.size());
  if (type.kind().is<null_type>()) {
    return create_null_column(array.length() - dropcount);
  }
  auto* rt = try_as<record_type>(type);
  if (not rt) {
    diagnostic::warning("incompatible type for column `{}",
                        fmt::join(path, "."))
      .note("expected `{}`, got `{}`", type_kind{tag_v<record_type>},
            type.kind())
      .emit(dh);
    return nullptr;
  }
  const auto& struct_array = as<arrow::StructArray>(array);
  // If `update_dropmask` was not called
  const bool did_update_dropmask = my_array == &array;
  if (not did_update_dropmask) {
    std::fill(found_column.begin(), found_column.end(), false);
  }
  for (auto [k, t, arr] : columns_of(*rt, struct_array)) {
    const auto [trafo, out_idx] = transfrom_and_index_for(k);
    path.push_back(k);
    if (not trafo) {
      emit_unknown_column_warning(path, dh);
      path.pop_back();
      continue;
    }
    auto this_column
      = trafo->create_column(path, t, arr, dropmask, dropcount, dh);
    path.pop_back();
    if (this_column == nullptr) {
      return nullptr;
    }
    columns[out_idx] = std::move(this_column);
    found_column[out_idx] = true;
  }
  // Check for required columns
  if (not did_update_dropmask) {
    for (const auto& [i, kvp] : detail::enumerate(transformations)) {
      const auto& [k, t] = kvp;
      if (found_column[i]) {
        continue;
      }
      if (t->clickhouse_nullable) {
        continue;
      }
      path.push_back(k);
      emit_missing_column_warning(path, dh);
      path.pop_back();
      return nullptr;
    }
  }
  return std::make_shared<ColumnTuple>(std::move(columns));
}

auto transformer_record::transfrom_and_index_for(std::string_view name) const
  -> find_result {
  const auto it = transformations.find(name);
  if (it == transformations.end()) {
    return {nullptr, 0};
  }
  return {&*it->second, std::distance(transformations.begin(), it)};
}

auto remove_non_significant_whitespace(std::string_view str) -> std::string {
  std::string ret;
  ret.reserve(str.size());
  auto can_skip = false;
  constexpr static auto syntax_characters = "(),"sv;
  for (size_t i = 0; i < str.size(); ++i) {
    const auto is_space = std::isspace(str[i]);
    if (can_skip and std::isspace(str[i])) {
      continue;
    }
    ret += str[i];
    const auto is_syntax
      = syntax_characters.find(str[i]) != std::string_view::npos;
    can_skip = is_space or is_syntax;
    // Remove space *before* the current syntax token. Handles e.g. `text )`
    if (is_syntax and i > 0 and std::isspace(str[i - 1])) {
      ret.pop_back();
    }
  }
  return ret;
}

namespace {

auto value_transform(auto v) {
  return v;
}

auto value_transform(tenzir::time v) -> int64_t {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
           v.time_since_epoch())
    .count();
}

auto value_transform(tenzir::duration v) -> int64_t {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(v).count();
}

auto value_transform(tenzir::ip v) -> in6_addr {
  return std::bit_cast<in6_addr>(v);
}

auto value_transform(tenzir::subnet v) -> std::tuple<in6_addr, uint8_t> {
  auto res = std::tuple<in6_addr, uint8_t>{};
  std::memcpy(&std::get<0>(res), &v.network(), sizeof(tenzir::ip));
  std::get<1>(res) = v.length();
  return res;
}

template <typename>
struct tenzir_to_clickhouse_trait;

#define X(TENZIR_TYPENAME, CLICKHOUSE_COLUMN, CLICKHOUSE_NAME)                 \
  template <>                                                                  \
  struct tenzir_to_clickhouse_trait<TENZIR_TYPENAME> {                         \
    constexpr static std::string_view name = CLICKHOUSE_NAME;                  \
    using column_type = CLICKHOUSE_COLUMN;                                     \
                                                                               \
    constexpr static auto null_value = std::nullopt;                           \
                                                                               \
    static auto clickhouse_typename(bool nullable) -> std::string {            \
      if (nullable) {                                                          \
        return std::string{"Nullable("}.append(name).append(1, ')');           \
      }                                                                        \
      return std::string{name};                                                \
    }                                                                          \
                                                                               \
    template <bool nullable>                                                   \
    static auto allocate(size_t n) {                                           \
      using Column_Type                                                        \
        = std::conditional_t<nullable, ColumnNullableT<column_type>,           \
                             column_type>;                                     \
      auto res = std::make_shared<Column_Type>();                              \
      res->Reserve(n);                                                         \
      return res;                                                              \
    }                                                                          \
  }

X(bool_type, ColumnUInt8, "UInt8");
X(int64_type, ColumnInt64, "Int64");
X(uint64_type, ColumnUInt64, "UInt64");
X(double_type, ColumnFloat64, "Float64");
X(string_type, ColumnString, "String");
X(duration_type, ColumnInt64, "Int64");
X(ip_type, ColumnIPv6, "IPv6");
#undef X

template <>
struct tenzir_to_clickhouse_trait<time_type> {
  constexpr static std::string_view name = "DateTime64(9)";
  using column_type = ColumnDateTime64;

  constexpr static auto null_value = std::nullopt;

  static auto clickhouse_typename(bool nullable) -> std::string {
    if (nullable) {
      return std::string{"Nullable("}.append(name).append(")");
    }
    return std::string{name};
  }

  template <bool nullable>
  static auto allocate(size_t n) {
    using Column_Type
      = std::conditional_t<nullable, ColumnNullableT<column_type>, column_type>;
    auto res = std::make_shared<Column_Type>(9);
    res->Reserve(n);
    return res;
  }
};

template <>
struct tenzir_to_clickhouse_trait<subnet_type> {
  static auto clickhouse_typename(bool nullable) -> std::string {
    if (nullable) {
      return "Tuple(ip Nullable(IPv6),length Nullable(UInt8))";
    }
    return "Tuple(ip IPv6,length UInt8)";
  }

  constexpr static auto null_value = std::tuple{std::nullopt, std::nullopt};

  template <bool nullable>
  static auto allocate(size_t n) {
    using ip_t
      = std::conditional_t<nullable, ColumnNullableT<ColumnIPv6>, ColumnIPv6>;
    using length_t
      = std::conditional_t<nullable, ColumnNullableT<ColumnUInt8>, ColumnUInt8>;
    using Column_Type = ColumnTupleT<ip_t, length_t>;
    auto ip_c = std::make_shared<ip_t>();
    ip_c->Reserve(n);
    auto length_c = std::make_shared<length_t>();
    length_c->Reserve(n);
    auto res = std::make_shared<Column_Type>(
      std::tuple{std::move(ip_c), std::move(length_c)});
    return res;
  }
};

template <typename Actual, typename Expected>
concept convertible_hack = std::same_as<Expected, Actual>
                           or (std::same_as<Expected, int64_type>
                               and std::same_as<Actual, duration_type>);

template <typename T, bool Nullable>
  requires requires { tenzir_to_clickhouse_trait<T>{}; }
struct transformer_from_trait : transformer {
  using traits = tenzir_to_clickhouse_trait<T>;

  transformer_from_trait()
    : transformer{traits::clickhouse_typename(Nullable), Nullable} {
  }

  virtual auto update_dropmask(path_type& path, const tenzir::type& type,
                               const arrow::Array& array, dropmask_ref dropmask,
                               tenzir::diagnostic_handler& dh)
    -> drop override {
    TENZIR_UNUSED(path);
    if constexpr (Nullable) {
      return drop::none;
    }
    const auto correct_type = match(
      type,
      [&]<typename U>(const U&) {
        // error case. Potentially do more conversions?
        diagnostic::warning("incompatible type for column `{}",
                            fmt::join(path, "."))
          .note("expected `{}`, got `{}`\n", type_kind{tag_v<T>},
                type_kind{tag_v<U>})
          .emit(dh);
        return false;
      },
      [&]<convertible_hack<T> U>(const U&) {
        return true;
      });
    if (not correct_type) {
      return drop::all;
    }
    if (not array.null_bitmap()) {
      return drop::none;
    }
    if (array.null_count() == 0) {
      return drop::none;
    }
    if (array.null_count() == array.length()) {
      return drop::all;
    }
    for (int64_t i = 0; i < array.length(); ++i) {
      if (array.IsNull(i)) {
        dropmask[i] = true;
      }
    }
    return drop::some;
  }

  virtual auto create_null_column(size_t n) const
    -> ::clickhouse::ColumnRef override {
    if constexpr (Nullable) {
      auto columns = traits::template allocate<Nullable>(n);
      for (size_t i = 0; i < n; ++n) {
        columns->Append(traits::null_value);
      }
      return columns;
    }
    return nullptr;
  }

  virtual auto create_column(path_type& path, const tenzir::type& type,
                             const arrow::Array& array, dropmask_cref dropmask,
                             int64_t dropcount, tenzir::diagnostic_handler& dh)
    -> ::clickhouse::ColumnRef override {
    const auto f = detail::overload{
      [&](const null_type&) -> std::shared_ptr<Column> {
        return create_null_column(array.length() - dropcount);
      },
      [&]<typename U>(const U&) -> std::shared_ptr<Column> {
        // error case. Potentially do more conversions?
        diagnostic::warning("incompatible type for column `{}",
                            fmt::join(path, "."))
          .note("expected `{}`, got `{}`\n", type_kind{tag_v<T>},
                type_kind{tag_v<U>})
          .emit(dh);
        return nullptr;
      },
      [&]<convertible_hack<T> U>(const U&) -> std::shared_ptr<Column> {
        auto column = traits::template allocate<Nullable>(array.length());
        auto cast_array = dynamic_cast<const type_to_arrow_array_t<U>*>(&array);
        TENZIR_ASSERT(cast_array);
        for (int64_t i = 0; i < cast_array->length(); ++i) {
          if (dropmask[i]) {
            continue;
          }
          auto v = view_at(*cast_array, i);
          if constexpr (Nullable) {
            if (not v) {
              column->Append(traits::null_value);
              continue;
            }
          }
          TENZIR_ASSERT(v.has_value());
          column->Append(value_transform(*v));
        }
        return column;
      },
    };
    return match(type, f);
  }
};

template <typename T>
auto make_transformer_impl(bool nullable) -> std::unique_ptr<transformer> {
  if (nullable) {
    return std::make_unique<transformer_from_trait<T, true>>();
  } else {
    return std::make_unique<transformer_from_trait<T, false>>();
  }
}

struct transformer_blob : transformer {
  transformer_blob() : transformer("Array(UInt8)", true) {
  }
  virtual auto update_dropmask(path_type& path, const tenzir::type& type,
                               const arrow::Array& array, dropmask_ref dropmask,
                               tenzir::diagnostic_handler& dh)
    -> drop override {
    TENZIR_UNUSED(path, type, array, dropmask, dh);
    const auto* bt = try_as<blob_type>(type);
    if (not bt) {
      diagnostic::warning("incompatible type for column `{}`",
                          fmt::join(path, "."))
        .note("expected `{}`, got `{}`", type_kind{tag_v<blob_type>},
              type.kind())
        .emit(dh);
      return drop::all;
    }
    return drop::none;
  }

  virtual auto create_null_column(size_t n) const
    -> ::clickhouse::ColumnRef override {
    auto clickhouse_columns = std::make_shared<ColumnUInt8>();
    auto clickhouse_offsets = std::make_shared<ColumnUInt64>();
    auto& offsets = clickhouse_offsets->GetWritableData();
    offsets.resize(n, 0);
    return std::make_shared<ColumnArray>(std::move(clickhouse_columns),
                                         std::move(clickhouse_offsets));
  }

  virtual auto create_column(path_type& path, const tenzir::type& type,
                             const arrow::Array& array, dropmask_cref dropmask,
                             int64_t dropcount, tenzir::diagnostic_handler& dh)
    -> ::clickhouse::ColumnRef override {
    TENZIR_UNUSED(path, dh);
    if (not type.kind().is<blob_type>()) {
      return create_null_column(array.length() - dropcount);
    }
    auto clickhouse_columns = std::make_shared<ColumnUInt8>();
    auto& data = clickhouse_columns->GetWritableData();
    auto clickhouse_offsets = std::make_shared<ColumnUInt64>();
    auto& offsets = clickhouse_offsets->GetWritableData();
    auto last_offset = uint64_t{0};
    auto& cast_array = as<type_to_arrow_array_t<blob_type>>(array);
    for (auto [i, v] : detail::enumerate(cast_array)) {
      if (dropmask[i]) {
        continue;
      }
      if (not v) {
        offsets.push_back(last_offset);
        continue;
      }
      last_offset += v->size();
      auto old_data_size = data.size();
      data.resize(old_data_size + v->size());
      std::copy(v->begin(), v->end(), data.begin() + old_data_size);
      offsets.push_back(last_offset);
    }
    return std::make_shared<ColumnArray>(std::move(clickhouse_columns),
                                         std::move(clickhouse_offsets));
  }
};

struct transformer_array : transformer {
  std::unique_ptr<transformer> data_transform;
  dropmask_type my_mask;
  const arrow::ListArray* my_list_array;

  transformer_array(std::string clickhouse_typename,
                    std::unique_ptr<transformer> data_transform)
    : transformer{std::move(clickhouse_typename),
                  data_transform->clickhouse_nullable},
      data_transform{std::move(data_transform)} {
  }

  static auto values_size(const arrow::ListArray& list_array) -> int64_t {
    const auto length = list_array.length();
    if (length == 0) {
      return 0;
    }
    return list_array.value_offset(length) - list_array.value_offset(0);
  }

  /// Slices the actually relevant values for this list array.
  static auto sliced_values(const arrow::ListArray& list_array)
    -> std::shared_ptr<arrow::Array> {
    // The actual values start at value_offset and end after the end of the
    // last list
    const auto length = list_array.length();
    if (length == 0) {
      return list_array.values()->Slice(list_array.value_offset(0), 0);
    }
    return list_array.values()->Slice(list_array.value_offset(0),
                                      values_size(list_array));
  }

  auto apply_dropmask_to_my_mask(const arrow::ListArray& list_array,
                                 dropmask_cref dropmask) -> void {
    my_mask.clear();
    my_mask.resize(values_size(list_array), false);
    my_list_array = &list_array;
    auto write_index = int64_t{0};
    for (int64_t i = 0; i < list_array.length(); ++i) {
      auto length = list_array.value_offset(i + 1) - list_array.value_offset(i);
      if (not dropmask[i] and list_array.IsValid(i)) {
        write_index += length;
        continue;
      }
      const auto end = write_index + length;
      for (int64_t j = write_index; j < end; ++j) {
        my_mask[j] = true;
      }
      write_index += length;
    }
    TENZIR_ASSERT(static_cast<size_t>(write_index) == my_mask.size());
  }

  virtual auto update_dropmask(path_type& path, const tenzir::type& type,
                               const arrow::Array& array, dropmask_ref dropmask,
                               tenzir::diagnostic_handler& dh)
    -> drop override {
    if (is<null_type>(type)) {
      return drop::none;
    }
    const auto* lt = try_as<list_type>(type);
    if (not lt) {
      diagnostic::warning("incompatible type for column `{}",
                          fmt::join(path, "."))
        .note("expected `{}`, got `{}`", type_kind{tag_v<list_type>},
              type.kind())
        .emit(dh);
      return drop::all;
    }
    const auto value_type = lt->value_type();
    const auto& list_array = as<arrow::ListArray>(array);
    apply_dropmask_to_my_mask(list_array, dropmask);
    if (clickhouse_nullable) {
      return drop::none;
    }
    const auto value_array = sliced_values(list_array);
    path.push_back("[]");
    auto updated = data_transform->update_dropmask(path, value_type,
                                                   *value_array, my_mask, dh);
    path.pop_back();
    if (updated == drop::none) {
      return drop::none;
    }
    if (updated == drop::all) {
      return drop::all;
    }
    auto all_should_be_dropped = true;
    const auto* offsets = list_array.raw_value_offsets();
    for (int64_t i = 0; i < array.length(); ++i) {
      if (array.IsNull(i)) {
        dropmask[i] = true;
        updated = drop::some;
        continue;
      }
      const auto begin = my_mask.begin() + offsets[i];
      const auto end = my_mask.begin() + offsets[i + 1];
      dropmask[i] |= std::any_of(begin, end, std::identity{});
      if (dropmask[i]) {
        updated = drop::some;
      }
      all_should_be_dropped &= dropmask[i];
    }
    return all_should_be_dropped ? drop::all : updated;
  }

  virtual auto create_null_column(size_t n) const
    -> ::clickhouse::ColumnRef override {
    if (not clickhouse_nullable) {
      return nullptr;
    }
    auto column = data_transform->create_null_column(0);
    if (not column) {
      return nullptr;
    }
    auto column_offsets = std::make_shared<ColumnUInt64>();
    auto& offsets = column_offsets->GetWritableData();
    offsets.resize(n, 0);
    return std::make_shared<ColumnArray>(std::move(column),
                                         std::move(column_offsets));
  }

  /// Translates Arrow Array offsets to ClickHouse Array offsets
  /// arrow offsets are [ start1 , past1/start2, ... ]
  /// clickhouse offsets are [end1, end2, ...]
  /// See e.g. `::clickhouse::ColumnArray::GetSize`
  static auto
  make_offsets(const arrow::ListArray& list_array, dropmask_cref dropmask)
    -> std::shared_ptr<ColumnUInt64> {
    const auto size = list_array.length();
    auto res = std::make_shared<ColumnUInt64>();
    auto& output = res->GetWritableData();
    output.resize(size);
    auto actual_size = size_t{0};
    for (int64_t i = 0; i < size; ++i) {
      if (dropmask[i]) {
        continue;
      }
      const auto start = actual_size > 0 ? output[actual_size - 1] : 0;
      auto length = list_array.IsValid(i) ? list_array.value_length(i) : 0;
      output[actual_size] = start + length;
      ++actual_size;
    }
    output.resize(actual_size);
    return res;
  }

  virtual auto create_column(path_type& path, const tenzir::type& type,
                             const arrow::Array& array, dropmask_cref dropmask,
                             int64_t dropcount, tenzir::diagnostic_handler& dh)
    -> ::clickhouse::ColumnRef override {
    if (is<null_type>(type)) {
      return create_null_column(array.length() - dropcount);
    }
    const auto* lt = try_as<list_type>(type);
    if (not lt) {
      diagnostic::warning("incompatible type for column `{}",
                          fmt::join(path, "."))
        .note("expected `{}`, got `{}`", type_kind{tag_v<list_type>},
              type.kind())
        .emit(dh);
      return nullptr;
    }
    const auto value_type = lt->value_type();
    const auto& list_array = as<arrow::ListArray>(array);
    // Either this is fully nullable, or update_dropmask must have been called.
    if (not clickhouse_nullable) {
      TENZIR_ASSERT(my_list_array == &list_array, "`{}!={}` in `{}` ({})",
                    (void*)my_list_array, (void*)&array, fmt::join(path, "."),
                    clickhouse_typename);
    }
    apply_dropmask_to_my_mask(list_array, dropmask);
    const auto my_dropcount = pop_count(my_mask);
    auto clickhouse_offsets = make_offsets(list_array, dropmask);
    const auto value_array = sliced_values(list_array);
    path.push_back("[]");
    auto clickhouse_columns = data_transform->create_column(
      path, value_type, *value_array, my_mask, my_dropcount, dh);
    path.pop_back();
    if (not clickhouse_columns) {
      return nullptr;
    }
    return std::make_shared<ColumnArray>(std::move(clickhouse_columns),
                                         std::move(clickhouse_offsets));
  }
};

auto make_record_functions_from_clickhouse(path_type& path,
                                           std::string_view clickhouse_typename,
                                           diagnostic_handler& dh)
  -> std::unique_ptr<transformer> {
  TENZIR_ASSERT(clickhouse_typename.starts_with("Tuple("));
  TENZIR_ASSERT(clickhouse_typename.ends_with(")"));
  auto tuple_elements = clickhouse_typename;
  tuple_elements.remove_prefix("Tuple("sv.size());
  tuple_elements.remove_suffix(1);
  auto transformations = transformer_record::schema_transformations{};
  if (tuple_elements.empty()) {
    diagnostic::error("ClickHouse column `{}` is an empty record, which is not "
                      "supported",
                      fmt::join(path, "."))
      .emit(dh);
    return nullptr;
  }
  auto fields = std::vector<std::pair<std::string_view, std::string_view>>{};
  auto open_count = 0;
  size_t part_start_index = 0;
  const auto add_field = [&](size_t start, size_t size) {
    const auto part = detail::trim(tuple_elements.substr(start, size));
    const auto split = part.find(' ');
    const auto name = part.substr(0, split);
    const auto type = part.substr(split + 1);
    fields.emplace_back(name, type);
  };
  for (size_t i = 0; i < tuple_elements.size(); ++i) {
    const auto c = tuple_elements[i];
    if (c == ')') {
      TENZIR_ASSERT(open_count > 0);
      --open_count;
      continue;
    }
    if (c == '(') {
      ++open_count;
      continue;
    }
    if (c == ',' and open_count == 0) {
      add_field(part_start_index, i - part_start_index);
      part_start_index = i + 1;
      continue;
    }
  }
  add_field(part_start_index, clickhouse_typename.npos);
  for (const auto& [k, t] : fields) {
    if (not validate_identifier(k)) {
      emit_invalid_identifier("nested column name", k, location::unknown, dh);
      return nullptr;
    }
    path.push_back(k);
    auto functions = make_functions_from_clickhouse(path, t, dh);
    path.pop_back();
    if (not functions) {
      return nullptr;
    }
    auto [_, success]
      = transformations.try_emplace(std::string{k}, std::move(functions));
    TENZIR_ASSERT(success);
  }
  return std::make_unique<transformer_record>(std::string{clickhouse_typename},
                                              std::move(transformations));
}

auto make_array_functions_from_clickhouse(path_type& path,
                                          std::string_view clickhouse_typename,
                                          diagnostic_handler& dh)
  -> std::unique_ptr<transformer> {
  TENZIR_ASSERT(clickhouse_typename.starts_with("Array("));
  TENZIR_ASSERT(clickhouse_typename.ends_with(")"));
  auto value_typename = clickhouse_typename;
  value_typename.remove_prefix("Array("sv.size());
  value_typename.remove_suffix(1);
  path.push_back("[]");
  auto data_transform
    = make_functions_from_clickhouse(path, value_typename, dh);
  path.pop_back();
  if (not data_transform) {
    return nullptr;
  }
  return std::make_unique<transformer_array>(std::string{clickhouse_typename},
                                             std::move(data_transform));
}

} // namespace

auto type_to_clickhouse_typename(path_type& path, tenzir::type t, bool nullable,
                                 diagnostic_handler& dh)
  -> failure_or<std::string> {
  const auto f = detail::overload{
    [nullable]<typename T>(const T&) -> failure_or<std::string>
      requires requires {
        tenzir_to_clickhouse_trait<T>::clickhouse_typename(true);
      }
    {
      return tenzir_to_clickhouse_trait<T>::clickhouse_typename(nullable);
    },
    [&dh, &path](const record_type& r) -> failure_or<std::string> {
      TRY(auto tup, plain_clickhouse_tuple_elements(path, r, dh));
      if (tup == "()") {
        diagnostic::error("column `{}` is an empty record, which is not "
                          "supported",
                          fmt::join(path, "."))
          .note("empty `Tuple`s cannot be send to ClickHouse")
          .emit(dh);
        return failure::promise();
      }
      return "Tuple" + tup;
    },
    [&dh, nullable, &path](const list_type& l) -> failure_or<std::string> {
      TRY(auto vt,
          type_to_clickhouse_typename(path, l.value_type(), nullable, dh));
      TENZIR_ASSERT(not vt.empty());
      return "Array(" + vt + ")";
    },
    [&dh, &path](const null_type&) -> failure_or<std::string> {
      diagnostic::error("column `{}` has type `null`", fmt::join(path, "."))
        .note("untyped nulls are not supported when creating a table")
        .hint("cast all columns to their intended type beforehand:\n"
              "`column_that_should_be_int = int(column_that_should_be_int)`")
        .emit(dh);
      return failure::promise();
    },
    [](const map_type&) -> failure_or<std::string> {
      TENZIR_UNREACHABLE();
    },
    [](const enumeration_type&) -> failure_or<std::string> {
      TENZIR_UNREACHABLE();
    },
    [](const blob_type&) -> failure_or<std::string> {
      return "Array(UInt8)";
    },
    [&dh, &path](const secret_type&) -> failure_or<std::string> {
      diagnostic::error("column `{}` has type `secret`", fmt::join(path, "."))
        .note("secrets cannot be sent to ClickHouse")
        .emit(dh);
      return failure::promise();
    },
    };
  return match(t, f);
}

auto plain_clickhouse_tuple_elements(path_type& path, const record_type& record,
                                     diagnostic_handler& dh,
                                     std::string_view primary)
  -> failure_or<std::string> {
  auto res = std::string{"("};
  auto first = true;
  for (auto [k, t] : record.fields()) {
    const auto is_primary = k == primary;
    path.push_back(k);
    TRY(auto nested, type_to_clickhouse_typename(path, t, not is_primary, dh));
    path.pop_back();
    TENZIR_ASSERT(not nested.empty());
    if (not first) {
      res += ", ";
    } else {
      first = false;
    }
    fmt::format_to(std::back_inserter(res), "{} {}", k, nested);
  }
  res += ")";
  return res;
}

auto emit_unsupported_clickhouse_type_diagnostic(
  path_type& path, std::string_view clickhouse_typename, diagnostic_handler& dh)
  -> void {
  auto diag = diagnostic::error("ClickHouse column `{}` has unsupported "
                                "ClickHouse type `{}`",
                                fmt::join(path, "."), clickhouse_typename);
  // A few helpful suggestions for the types that we do support
  if (clickhouse_typename.starts_with("Date")) {
    diag = std::move(diag).note("use `DateTime64(8)` instead");
  } else if (clickhouse_typename.starts_with("UInt")) {
    diag = std::move(diag).note("use `UInt64` instead");
  } else if (clickhouse_typename.starts_with("Int")) {
    diag = std::move(diag).note("use `Int64` instead");
  } else if (clickhouse_typename.starts_with("Float")) {
    diag = std::move(diag).note("use `Float64` instead");
  } else if (clickhouse_typename == "IPv4") {
    diag = std::move(diag).note("use `IPv6` instead");
  }
  std::move(diag).emit(dh);
}

auto make_functions_from_clickhouse(path_type& path,
                                    const std::string_view clickhouse_typename,
                                    diagnostic_handler& dh)
  -> std::unique_ptr<transformer> {
  // Array(T)
  const bool is_nullable = clickhouse_typename.starts_with("Nullable(");
  TENZIR_ASSERT(not is_nullable or clickhouse_typename.ends_with(')'));
#define X(TENZIR_TYPE)                                                         \
  if (clickhouse_typename                                                      \
      == tenzir_to_clickhouse_trait<TENZIR_TYPE>::clickhouse_typename(         \
        false)) {                                                              \
    return make_transformer_impl<TENZIR_TYPE>(false);                          \
  }                                                                            \
  if (clickhouse_typename                                                      \
      == tenzir_to_clickhouse_trait<TENZIR_TYPE>::clickhouse_typename(true)) { \
    return make_transformer_impl<TENZIR_TYPE>(true);                           \
  }
  X(bool_type);
  X(int64_type);
  X(uint64_type);
  X(double_type);
  X(string_type);
  X(time_type);
  X(duration_type);
  X(ip_type);
  X(subnet_type);
#undef X
  if (clickhouse_typename == "Array(UInt8)") {
    return std::make_unique<transformer_blob>();
  }
  if (clickhouse_typename.starts_with("Tuple(")) {
    return make_record_functions_from_clickhouse(path, clickhouse_typename, dh);
  }
  if (clickhouse_typename.starts_with("Array(")) {
    return make_array_functions_from_clickhouse(path, clickhouse_typename, dh);
  }
  emit_unsupported_clickhouse_type_diagnostic(path, clickhouse_typename, dh);
  return nullptr;
}

} // namespace tenzir::plugins::clickhouse

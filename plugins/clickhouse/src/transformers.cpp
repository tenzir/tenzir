//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "clickhouse/transformers.hpp"

#include "clickhouse/arguments.hpp"
#include "tenzir/concept/printable/tenzir/json2.hpp"
#include "tenzir/detail/enumerate.hpp"
#include "tenzir/view3.hpp"

#include <clickhouse/columns/array.h>
#include <clickhouse/columns/bool.h>
#include <clickhouse/columns/date.h>
#include <clickhouse/columns/ip6.h>
#include <clickhouse/columns/json.h>
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

/// Emitted when a non-nullable ClickHouse column receives `null` values. Such
/// rows cannot be written and are dropped from the slice; `easy_client::insert`
/// relies on the drop having been reported here.
void emit_null_in_non_nullable_warning(const path_type& path,
                                       diagnostic_handler& dh) {
  diagnostic::warning("column `{}` contains `null`, but the ClickHouse column "
                      "is not nullable",
                      fmt::join(path, "."))
    .note("affected events will be dropped")
    .emit(dh);
}

/// Applies the standard null-drop policy for a non-nullable *leaf* column: no
/// nulls (or no null bitmap) yields `drop::none`; an all-null array yields
/// `drop::all`; otherwise the null rows are marked in `dropmask` and
/// `drop::some` is returned. `easy_client::insert` relies on this drop having
/// been reported. Not for record columns, which reconstruct from nested values
/// and thus do not short-circuit to `drop::all` on a top-level null.
///
/// The warning is emitted at most once, and only when this column introduces a
/// null for a row not *already* dropped upstream. A null nested record marks
/// its rows dropped before recursing into its children (see
/// `transformer_record::update_dropmask`), so a null that merely mirrors the
/// parent's null does not warn again here; only a genuinely leaf-level null in
/// an otherwise-present row does.
auto apply_null_dropmask(const arrow::Array& array, dropmask_ref dropmask,
                         const path_type& path, diagnostic_handler& dh)
  -> transformer::drop {
  if (not array.null_bitmap() or array.null_count() == 0) {
    return transformer::drop::none;
  }
  if (array.null_count() == array.length()) {
    // Report only if the caller has not already dropped every row upstream.
    auto newly_dropped = false;
    for (int64_t i = 0; i < array.length(); ++i) {
      if (not dropmask[i]) {
        newly_dropped = true;
        break;
      }
    }
    if (newly_dropped) {
      emit_null_in_non_nullable_warning(path, dh);
    }
    return transformer::drop::all;
  }
  auto newly_dropped = false;
  for (int64_t i = 0; i < array.length(); ++i) {
    if (array.IsNull(i)) {
      if (not dropmask[i]) {
        newly_dropped = true;
      }
      dropmask[i] = true;
    }
  }
  if (newly_dropped) {
    emit_null_in_non_nullable_warning(path, dh);
  }
  return transformer::drop::some;
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
    diagnostic::warning("incompatible type for column `{}`",
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
  if (array.null_count() > 0) {
    emit_null_in_non_nullable_warning(path, dh);
  }
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
  for (const auto& [i, kvp] : detail::enumerate(transformations)) {
    const auto& [_, t] = kvp;
    auto c = t->create_null_column(n);
    if (not c) {
      return nullptr;
    }
    columns[i] = std::move(c);
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
    diagnostic::warning("incompatible type for column `{}`",
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
  for (const auto& [i, kvp] : detail::enumerate(transformations)) {
    const auto& [k, t] = kvp;
    if (found_column[i]) {
      continue;
    }
    path.push_back(k);
    auto this_column = t->create_null_column(array.length() - dropcount);
    path.pop_back();
    if (not this_column) {
      return nullptr;
    }
    columns[i] = std::move(this_column);
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
  auto quote = char{};
  constexpr static auto syntax_characters = "(),"sv;
  for (size_t i = 0; i < str.size(); ++i) {
    auto c = str[i];
    if (quote != '\0') {
      ret += c;
      can_skip = false;
      if (c == '\\' and i + 1 < str.size()) {
        ret += str[++i];
        continue;
      }
      if (c == quote) {
        if (i + 1 < str.size() and str[i + 1] == quote) {
          ret += str[++i];
          continue;
        }
        quote = '\0';
      }
      continue;
    }
    if (c == '\'' or c == '"' or c == '`') {
      ret += c;
      quote = c;
      can_skip = false;
      continue;
    }
    const auto is_space = std::isspace(static_cast<unsigned char>(c));
    if (can_skip and is_space) {
      continue;
    }
    const auto is_syntax = syntax_characters.find(c) != std::string_view::npos;
    // Remove space *before* the current syntax token. Handles e.g. `text )`
    // Pop before appending so we remove the trailing space, not `c` itself.
    if (is_syntax and not ret.empty()
        and std::isspace(static_cast<unsigned char>(ret.back()))) {
      ret.pop_back();
    }
    ret += c;
    can_skip = is_space or is_syntax;
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

X(bool_type, ColumnBool, "Bool");
X(int64_type, ColumnInt64, "Int64");
X(uint64_type, ColumnUInt64, "UInt64");
X(double_type, ColumnFloat64, "Float64");
X(string_type, ColumnString, "String");
X(duration_type, ColumnInt64, "Int64");
X(ip_type, ColumnIPv6, "IPv6");
#undef X

/// Tenzir `bool` columns are now created as ClickHouse `Bool`, but tables
/// created by older Tenzir versions (and any table that stores boolean data as
/// `UInt8`) describe the column as `UInt8`. This trait lets us keep appending
/// `bool` values to such legacy columns by sending them as `UInt8`, matching
/// the column type the server already has.
struct legacy_bool_trait {
  constexpr static std::string_view name = "UInt8";
  using column_type = ColumnUInt8;

  constexpr static auto null_value = std::nullopt;

  static auto clickhouse_typename(bool nullable) -> std::string {
    if (nullable) {
      return std::string{"Nullable("}.append(name).append(1, ')');
    }
    return std::string{name};
  }

  template <bool nullable>
  static auto allocate(size_t n) {
    using Column_Type
      = std::conditional_t<nullable, ColumnNullableT<column_type>, column_type>;
    auto res = std::make_shared<Column_Type>();
    res->Reserve(n);
    return res;
  }
};

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

template <typename T, bool Nullable,
          typename Traits = tenzir_to_clickhouse_trait<T>>
  requires requires { Traits{}; }
struct transformer_from_trait : transformer {
  using traits = Traits;

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
        diagnostic::warning("incompatible type for column `{}`",
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
    return apply_null_dropmask(array, dropmask, path, dh);
  }

  virtual auto create_null_column(size_t n) const
    -> ::clickhouse::ColumnRef override {
    if constexpr (Nullable) {
      auto columns = traits::template allocate<Nullable>(n);
      for (size_t i = 0; i < n; ++i) {
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
        diagnostic::warning("incompatible type for column `{}`",
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

template <typename T, typename Traits = tenzir_to_clickhouse_trait<T>>
auto make_transformer_impl(bool nullable) -> std::unique_ptr<transformer> {
  if (nullable) {
    return std::make_unique<transformer_from_trait<T, true, Traits>>();
  } else {
    return std::make_unique<transformer_from_trait<T, false, Traits>>();
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

/// Sends data to a ClickHouse `JSON` column by serializing each row's record to
/// JSON text. We only support sending to an existing JSON column; we never
/// create one (there is no Tenzir `json` type). ClickHouse `JSON` columns only
/// accept objects at the top level, so non-record columns are written as empty
/// objects with a warning. Absent rows become `{}`; null rows become `{}` for a
/// plain `JSON` column and SQL `NULL` for a `Nullable(JSON)` column (ClickHouse
/// rejects empty strings for JSON but accepts the empty object).
struct transformer_json : transformer {
  bool nullable;
  json_printer2 printer = json_printer2{json_printer_options{
    .style = no_style(),
    .oneline = true,
  }};

  explicit transformer_json(bool nullable)
    : transformer(nullable ? "Nullable(JSON)" : "JSON", /*nullable=*/true),
      nullable{nullable} {
  }

  auto update_dropmask(path_type& path, const tenzir::type& type,
                       const arrow::Array& array, dropmask_ref dropmask,
                       tenzir::diagnostic_handler& dh) -> drop override {
    TENZIR_UNUSED(path, type, array, dropmask, dh);
    // A JSON column accepts any value, so we never drop rows.
    return drop::none;
  }

  auto create_null_column(size_t n) const -> ::clickhouse::ColumnRef override {
    return build(n, [n](auto&& append) {
      for (size_t i = 0; i < n; ++i) {
        append(std::nullopt);
      }
    });
  }

  auto create_column(path_type& path, const tenzir::type& type,
                     const arrow::Array& array, dropmask_cref dropmask,
                     int64_t dropcount, tenzir::diagnostic_handler& dh)
    -> ::clickhouse::ColumnRef override {
    // ClickHouse `JSON` columns only accept JSON objects at the top level. A
    // record maps to an object; anything else (scalars, lists) would be
    // rejected by the server, so we substitute empty objects and warn.
    const auto unsupported
      = not type.kind().is<record_type>() and not type.kind().is<null_type>();
    if (unsupported) {
      diagnostic::warning("cannot write `{}` into a ClickHouse JSON column",
                          fmt::join(path, "."))
        .note("expected a record, but got `{}`", type.kind())
        .note("values will be written as empty objects (`{}`)")
        .emit(dh);
    }
    return build(array.length() - dropcount, [&](auto&& append) {
      for (int64_t i = 0; i < array.length(); ++i) {
        if (dropmask[i]) {
          continue;
        }
        auto v = view_at(array, i);
        if (is<caf::none_t>(v)) {
          // A null row becomes SQL NULL for a nullable column and `{}` for a
          // plain one; check this before substituting unsupported values so
          // nulls are not silently turned into empty objects.
          append(std::nullopt);
          continue;
        }
        if (unsupported) {
          // Non-record value: write an explicit empty object, not NULL.
          append(std::string_view{"{}"});
          continue;
        }
        printer.load_new(v);
        auto bytes = printer.bytes();
        append(std::string_view{reinterpret_cast<const char*>(bytes.data()),
                                bytes.size()});
      }
    });
  }

private:
  /// Allocates a plain or nullable `ColumnJSON` (depending on `nullable`),
  /// reserves `n` rows, and invokes `fill` with an `append` callback. The
  /// callback takes an optional JSON string: `std::nullopt` becomes `{}` for a
  /// plain column and SQL `NULL` for a nullable one.
  auto build(size_t n, auto&& fill) const -> ::clickhouse::ColumnRef {
    if (nullable) {
      auto column = std::make_shared<ColumnNullableT<ColumnJSON>>();
      column->Reserve(n);
      fill([&](std::optional<std::string_view> v) {
        column->Append(v);
      });
      return column;
    }
    auto column = std::make_shared<ColumnJSON>();
    column->Reserve(n);
    fill([&](std::optional<std::string_view> v) {
      column->Append(v.value_or(std::string_view{"{}"}));
    });
    return column;
  }
};

/// Sends Tenzir `time` values to a ClickHouse `DateTime64(N[, 'tz'])` column of
/// arbitrary scale `N`. The built-in `time_type` trait only handles the
/// canonical `DateTime64(9)` (no timezone) that `to_clickhouse` creates itself;
/// this transformer lets us append to pre-existing tables whose column uses a
/// different scale or carries a timezone. We build a column with the table's
/// exact scale and timezone so the inserted block type matches the target
/// column and no server-side conversion is required. Nanosecond values are
/// divided down to the column's tick unit (`10^(9-N)`), flooring toward
/// negative infinity just as a server-side scale conversion would (so
/// pre-epoch, sub-tick values round down rather than toward zero).
struct transformer_datetime64 : transformer {
  size_t scale;
  Option<std::string> timezone;
  bool nullable;
  int64_t divisor;

  static auto type_name(size_t scale, const Option<std::string>& tz,
                        bool nullable) -> std::string {
    auto inner = fmt::format("DateTime64({}", scale);
    if (tz) {
      fmt::format_to(std::back_inserter(inner), ",'{}'", *tz);
    }
    inner += ')';
    if (nullable) {
      return fmt::format("Nullable({})", inner);
    }
    return inner;
  }

  transformer_datetime64(size_t scale, Option<std::string> tz, bool nullable)
    : transformer{type_name(scale, tz, nullable), nullable},
      scale{scale},
      timezone{std::move(tz)},
      nullable{nullable},
      divisor{[&] {
        auto d = int64_t{1};
        for (auto i = scale; i < 9; ++i) {
          d *= 10;
        }
        return d;
      }()} {
  }

  auto update_dropmask(path_type& path, const tenzir::type& type,
                       const arrow::Array& array, dropmask_ref dropmask,
                       tenzir::diagnostic_handler& dh) -> drop override {
    // Validate the input type up front for both the nullable and non-nullable
    // case, so a mismatch produces the targeted "incompatible type" diagnostic
    // here rather than a generic "failed to add column" from `create_column`.
    // A `null_type` input is accepted: it is materialized via
    // `create_null_column` (nullable) or dropped as all-null (non-nullable).
    if (not type.kind().is<time_type>() and not type.kind().is<null_type>()) {
      diagnostic::warning("incompatible type for column `{}`",
                          fmt::join(path, "."))
        .note("expected `{}`, got `{}`", type_kind{tag_v<time_type>},
              type.kind())
        .emit(dh);
      return drop::all;
    }
    if (nullable) {
      return drop::none;
    }
    return apply_null_dropmask(array, dropmask, path, dh);
  }

  auto create_null_column(size_t n) const -> ::clickhouse::ColumnRef override {
    if (not nullable) {
      return nullptr;
    }
    return build(n, [n](auto&& append) {
      for (size_t i = 0; i < n; ++i) {
        append(std::nullopt);
      }
    });
  }

  auto create_column(path_type& path, const tenzir::type& type,
                     const arrow::Array& array, dropmask_cref dropmask,
                     int64_t dropcount, tenzir::diagnostic_handler& dh)
    -> ::clickhouse::ColumnRef override {
    const auto n = array.length() - dropcount;
    if (type.kind().is<null_type>()) {
      return create_null_column(n);
    }
    if (not type.kind().is<time_type>()) {
      diagnostic::warning("incompatible type for column `{}`",
                          fmt::join(path, "."))
        .note("expected `{}`, got `{}`", type_kind{tag_v<time_type>},
              type.kind())
        .emit(dh);
      return nullptr;
    }
    const auto& cast_array = as<type_to_arrow_array_t<time_type>>(array);
    return build(n, [&](auto&& append) {
      for (int64_t i = 0; i < cast_array.length(); ++i) {
        if (dropmask[i]) {
          continue;
        }
        auto v = view_at(cast_array, i);
        if (not v) {
          append(std::nullopt);
          continue;
        }
        append(to_ticks(value_transform(*v)));
      }
    });
  }

private:
  /// Converts a nanosecond timestamp to the column's tick unit, flooring toward
  /// negative infinity to match ClickHouse's server-side scale conversion.
  /// Integer division alone truncates toward zero, which would round pre-epoch
  /// (negative) sub-tick values up. `divisor` is always positive.
  auto to_ticks(int64_t nanos) const -> int64_t {
    auto q = nanos / divisor;
    if (nanos % divisor != 0 and nanos < 0) {
      --q;
    }
    return q;
  }

  /// Allocates a plain or nullable `ColumnDateTime64` with the configured scale
  /// and timezone, reserves `n` rows, and invokes `fill` with an `append`
  /// callback taking an optional tick value. For a plain column a
  /// `std::nullopt` cannot legitimately occur (null rows are dropped) but is
  /// written as 0 defensively.
  auto build(size_t n, auto&& fill) const -> ::clickhouse::ColumnRef {
    if (nullable) {
      auto column
        = timezone
            ? std::make_shared<ColumnNullableT<ColumnDateTime64>>(scale,
                                                                  *timezone)
            : std::make_shared<ColumnNullableT<ColumnDateTime64>>(scale);
      column->Reserve(n);
      fill([&](std::optional<int64_t> v) {
        column->Append(v);
      });
      return column;
    }
    auto column = timezone
                    ? std::make_shared<ColumnDateTime64>(scale, *timezone)
                    : std::make_shared<ColumnDateTime64>(scale);
    column->Reserve(n);
    fill([&](std::optional<int64_t> v) {
      column->Append(v.value_or(0));
    });
    return column;
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
      diagnostic::warning("incompatible type for column `{}`",
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
      diagnostic::warning("incompatible type for column `{}`",
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

/// Transformer for an existing ClickHouse column whose type we cannot represent
/// but which has a default value. It is only ever created at the top level (in
/// `remote_fetch_schema_transformations`), because column defaults are a
/// top-level concept in ClickHouse; it is never nested inside a `Tuple`/`Array`.
///
/// It reports itself as nullable so that, when the column is absent from the
/// input, `easy_client::insert`'s missing-column check skips it and ClickHouse
/// fills the default. `update_dropmask` is only ever invoked when the input
/// *does* contain the column -- and since we cannot convert an unsupported
/// type, its mere invocation means the event must be dropped.
struct transformer_default_only : transformer {
  explicit transformer_default_only(std::string clickhouse_typename)
    : transformer{std::move(clickhouse_typename),
                  /*clickhouse_nullable=*/true} {
  }

  auto update_dropmask(path_type& path, const tenzir::type& type,
                       const arrow::Array& array, dropmask_ref dropmask,
                       tenzir::diagnostic_handler& dh) -> drop override {
    TENZIR_UNUSED(type, array, dropmask);
    diagnostic::warning("column `{}` has an unsupported ClickHouse type `{}` "
                        "and cannot be "
                        "written",
                        fmt::join(path, "."), clickhouse_typename)
      .note("the column has a default value and must be omitted from the input")
      .note("event will be dropped")
      .emit(dh);
    return drop::all;
  }

  auto create_null_column(size_t n) const -> ::clickhouse::ColumnRef override {
    // Unreachable: an absent column is never materialized at the top level.
    TENZIR_UNUSED(n);
    return nullptr;
  }

  auto create_column(path_type& path, const tenzir::type& type,
                     const arrow::Array& array, dropmask_cref dropmask,
                     int64_t dropcount, tenzir::diagnostic_handler& dh)
    -> ::clickhouse::ColumnRef override {
    // Unreachable: if the column is present, `update_dropmask` returned
    // `drop::all` and `insert` skips the slice before creating any columns.
    TENZIR_UNUSED(path, type, array, dropmask, dropcount, dh);
    return nullptr;
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
  auto fields = std::vector<std::pair<std::string, std::string_view>>{};
  for (auto part : split_top_level_clickhouse_type_arguments(tuple_elements)) {
    auto split = find_top_level_clickhouse_type_space(part);
    if (split == std::string_view::npos) {
      diagnostic::error("ClickHouse column `{}` has malformed tuple element "
                        "`{}`",
                        fmt::join(path, "."), part)
        .emit(dh);
      return nullptr;
    }
    auto name
      = unquote_identifier_component(detail::trim(part.substr(0, split)));
    auto type = detail::trim(part.substr(split + 1));
    fields.emplace_back(std::move(name), type);
  }
  for (const auto& [k, t] : fields) {
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
    fmt::format_to(std::back_inserter(res), "{} {}",
                   quote_identifier_component(k), nested);
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
    diag = std::move(diag).hint("use `DateTime64(9)` instead");
  } else if (clickhouse_typename.starts_with("UInt")) {
    diag = std::move(diag).hint("use `UInt64` instead");
  } else if (clickhouse_typename.starts_with("Int")) {
    diag = std::move(diag).hint("use `Int64` instead");
  } else if (clickhouse_typename.starts_with("Float")) {
    diag = std::move(diag).hint("use `Float64` instead");
  } else if (clickhouse_typename == "IPv4") {
    diag = std::move(diag).hint("use `IPv6` instead");
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
  // Accept `UInt8` as a legacy boolean target: tables created by older Tenzir
  // versions stored `bool` columns as `UInt8`. New tables use `Bool` (see
  // `tenzir_to_clickhouse_trait<bool_type>`), but appends must keep working
  // against existing `UInt8` columns.
  if (clickhouse_typename == "UInt8") {
    return make_transformer_impl<bool_type, legacy_bool_trait>(false);
  }
  if (clickhouse_typename == "Nullable(UInt8)") {
    return make_transformer_impl<bool_type, legacy_bool_trait>(true);
  }
  if (clickhouse_typename == "Array(UInt8)") {
    return std::make_unique<transformer_blob>();
  }
  if (clickhouse_typename == "JSON"
      or clickhouse_typename.starts_with("JSON(")) {
    return std::make_unique<transformer_json>(/*nullable=*/false);
  }
  if (clickhouse_typename == "Nullable(JSON)"
      or clickhouse_typename.starts_with("Nullable(JSON(")) {
    return std::make_unique<transformer_json>(/*nullable=*/true);
  }
  // `LowCardinality(X)` is a storage optimization that does not change the
  // logical type. ClickHouse's native INSERT path transparently wraps a plain
  // inner column into `LowCardinality`, so we strip the wrapper and recurse on
  // the inner type, sending the plain column. The clickhouse-cpp client only
  // supports this for `String`; it rejects or mishandles fixed-size inner types
  // such as numerics, so we reject those here with a clear error.
  if (auto inner
      = unwrap_clickhouse_type_call(clickhouse_typename, "LowCardinality")) {
    if (not is_lowcardinality_supported_inner(*inner)) {
      diagnostic::error("ClickHouse column `{}` has unsupported type `{}`",
                        fmt::join(path, "."), clickhouse_typename)
        .note("`LowCardinality` is only supported for `String` columns")
        .emit(dh);
      return nullptr;
    }
    return make_functions_from_clickhouse(path, *inner, dh);
  }
  // `DateTime64(N[, 'tz'])` of any scale/timezone (other than the canonical
  // `DateTime64(9)` already handled above). We build a column matching the
  // table's exact scale and timezone.
  const auto strip_quotes = [](std::string_view s) -> std::string {
    if (s.size() >= 2 and s.front() == '\'' and s.back() == '\'') {
      return std::string{s.substr(1, s.size() - 2)};
    }
    return std::string{s};
  };
  const auto make_datetime64
    = [&](std::string_view tn, bool nullable) -> std::unique_ptr<transformer> {
    auto inner = unwrap_clickhouse_type_call(tn, "DateTime64");
    if (not inner) {
      return nullptr;
    }
    auto parts = split_top_level_clickhouse_type_arguments(*inner);
    auto scale = size_t{0};
    if (parts.empty() or not parse_clickhouse_size(parts[0], scale)
        or scale > 9) {
      return nullptr;
    }
    auto timezone = Option<std::string>{};
    if (parts.size() > 1) {
      timezone = strip_quotes(parts[1]);
    }
    return std::make_unique<transformer_datetime64>(scale, std::move(timezone),
                                                    nullable);
  };
  if (auto t = make_datetime64(clickhouse_typename, false)) {
    return t;
  }
  if (is_nullable) {
    auto bare = clickhouse_typename;
    bare.remove_prefix("Nullable("sv.size());
    bare.remove_suffix(1);
    if (auto t = make_datetime64(bare, true)) {
      return t;
    }
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

auto make_default_only_transformer(std::string clickhouse_typename)
  -> std::unique_ptr<transformer> {
  return std::make_unique<transformer_default_only>(
    std::move(clickhouse_typename));
}

} // namespace tenzir::plugins::clickhouse

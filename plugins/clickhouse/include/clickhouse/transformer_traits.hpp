// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause
#pragma once
#include "clickhouse/transformers.hpp"
#include "tenzir/arrow_utils.hpp"
#include "tenzir/view3.hpp"

#include <clickhouse/columns/bool.h>
#include <clickhouse/columns/date.h>
#include <clickhouse/columns/ip6.h>
#include <clickhouse/columns/nullable.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/tuple.h>

namespace tenzir::plugins::clickhouse::transformer_detail {
using namespace ::clickhouse;
/// Emitted when a non-nullable ClickHouse column receives `null` values. Such
/// rows cannot be written and are dropped from the slice; `easy_client::insert`
/// relies on the drop having been reported here.
inline void emit_null_in_non_nullable_warning(const path_type& path,
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
inline auto apply_null_dropmask(const arrow::Array& array,
                                dropmask_ref dropmask, const path_type& path,
                                diagnostic_handler& dh) -> transformer::drop {
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
auto value_transform(auto v) {
  return v;
}

inline auto value_transform(tenzir::time v) -> int64_t {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
           v.time_since_epoch())
    .count();
}

inline auto value_transform(tenzir::duration v) -> int64_t {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(v).count();
}

inline auto value_transform(tenzir::ip v) -> in6_addr {
  return std::bit_cast<in6_addr>(v);
}

inline auto value_transform(tenzir::subnet v) -> std::tuple<in6_addr, uint8_t> {
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

} // namespace tenzir::plugins::clickhouse::transformer_detail

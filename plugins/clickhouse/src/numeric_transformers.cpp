//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause
#include "clickhouse/numeric_transformers.hpp"

#include "clickhouse/transformer_traits.hpp"

#include <clickhouse/columns/ip4.h>

namespace tenzir::plugins::clickhouse {
using namespace transformer_detail;
namespace {
template <class Column>
struct CheckedNumericTrait {
  using column_type = Column;
  // ClickHouse requires std::optional at this boundary.
  // NOLINTNEXTLINE(custom-prefer-none)
  static constexpr auto null_value = std::nullopt;

  static auto clickhouse_typename(bool nullable) -> std::string {
    auto column = Column{};
    auto name = column.Type()->GetName();
    return nullable ? fmt::format("Nullable({})", name) : name;
  }

  template <bool Nullable>
  static auto allocate(size_t n) -> std::shared_ptr<
    std::conditional_t<Nullable, ColumnNullableT<Column>, Column>> {
    using C = std::conditional_t<Nullable, ColumnNullableT<Column>, Column>;
    auto result = std::make_shared<C>();
    result->Reserve(n);
    return result;
  }
};

template <class Column, class T, class Value, bool Nullable>
struct CheckedNumericTransformer
  : transformer_from_trait<T, Nullable, CheckedNumericTrait<Column>> {
  using Base = transformer_from_trait<T, Nullable, CheckedNumericTrait<Column>>;
  auto update_dropmask(path_type& path, tenzir::type const& type,
                       arrow::Array const& array, dropmask_ref mask,
                       diagnostic_handler& dh) -> transformer::drop override {
    auto result = Base::update_dropmask(path, type, array, mask, dh);
    if (result == transformer::drop::all) {
      return result;
    }
    auto warned = false;
    auto index = size_t{0};
    for (auto value : values3(array)) {
      if (mask[index]) {
        ++index;
        continue;
      }
      auto const valid = match(
        value,
        [](caf::none_t) {
          return true;
        },
        [](auto x) {
          if constexpr (std::is_arithmetic_v<decltype(x)>) {
            if constexpr (std::is_integral_v<Value>
                          and std::is_integral_v<decltype(x)>) {
              return std::in_range<Value>(+x);
            } else if constexpr (std::is_integral_v<Value>) {
              return false;
            } else {
              return std::isfinite(x)
                     and static_cast<long double>(x)
                           >= std::numeric_limits<Value>::lowest()
                     and static_cast<long double>(x)
                           <= std::numeric_limits<Value>::max()
                     and static_cast<long double>(x)
                           == static_cast<long double>(static_cast<Value>(x));
            }
          }
          return false;
        });
      if (not valid) {
        if (not warned) {
          diagnostic::warning("value out of range for ClickHouse column `{}`",
                              fmt::join(path, "."))
            .emit(dh);
          warned = true;
        }
        mask[index] = true;
        result = result | transformer::drop::some;
      }
      ++index;
    }
    return result;
  }
};

template <class Column, class T, class Value>
auto make_checked_numeric(bool nullable) -> std::unique_ptr<transformer> {
  if (nullable) {
    return std::make_unique<CheckedNumericTransformer<Column, T, Value, true>>();
  }
  return std::make_unique<CheckedNumericTransformer<Column, T, Value, false>>();
}

template <bool Nullable>
struct Ipv4Transformer : transformer_from_trait<ip_type, Nullable> {
  using Base = transformer_from_trait<ip_type, Nullable>;

  Ipv4Transformer() {
    this->clickhouse_typename = Nullable ? "Nullable(IPv4)" : "IPv4";
  }

  auto update_dropmask(path_type& path, tenzir::type const& type,
                       arrow::Array const& array, dropmask_ref mask,
                       diagnostic_handler& dh) -> transformer::drop override {
    auto result = Base::update_dropmask(path, type, array, mask, dh);
    if (result == transformer::drop::all) {
      return result;
    }
    auto index = size_t{0};
    for (auto value : values3(array)) {
      if (mask[index]) {
        ++index;
        continue;
      }
      if (auto address = try_as<ip>(value); address and not address->is_v4()) {
        diagnostic::warning("IPv6 value cannot be stored in IPv4 column `{}`",
                            fmt::join(path, "."))
          .emit(dh);
        mask[index] = true;
        result = result | transformer::drop::some;
      }
      ++index;
    }
    return result;
  }

  auto create_null_column(size_t n) const -> ::clickhouse::ColumnRef override {
    if constexpr (Nullable) {
      auto column = std::make_shared<ColumnNullableT<ColumnIPv4>>();
      for (auto i = size_t{0}; i < n; ++i) {
        // ClickHouse requires std::optional at this boundary.
        // NOLINTNEXTLINE(custom-prefer-none)
        column->Append(std::nullopt);
      }
      return column;
    }
    return nullptr;
  }

  auto
  create_column(path_type&, tenzir::type const& type, arrow::Array const& array,
                dropmask_cref mask, int64_t dropcount, diagnostic_handler&)
    -> ::clickhouse::ColumnRef override {
    if (is<null_type>(type)) {
      return create_null_column(array.length() - dropcount);
    }
    if (not is<ip_type>(type)) {
      return nullptr;
    }
    using C
      = std::conditional_t<Nullable, ColumnNullableT<ColumnIPv4>, ColumnIPv4>;
    auto column = std::make_shared<C>();
    auto index = size_t{0};
    for (auto value : values3(array)) {
      if (mask[index++]) {
        continue;
      }
      if constexpr (Nullable) {
        if (is<caf::none_t>(value)) {
          // ClickHouse requires std::optional at this boundary.
          // NOLINTNEXTLINE(custom-prefer-none)
          column->Append(std::nullopt);
          continue;
        }
      }
      auto address = as<ip>(value);
      if (not address.is_v4()) {
        return nullptr;
      }
      auto native = in_addr{};
      std::memcpy(&native, as_bytes(address).data() + 12, sizeof(native));
      column->Append(native);
    }
    return column;
  }
};

} // namespace

auto make_checked_transformer(std::string_view clickhouse_typename,
                              MappingMode mode)
  -> std::unique_ptr<transformer> {
  auto is_nullable = clickhouse_typename.starts_with("Nullable(");
  auto numeric_name = clickhouse_typename;
  if (is_nullable) {
    numeric_name.remove_prefix(9);
    numeric_name.remove_suffix(1);
  }
#define CHECKED(NAME, COLUMN, TYPE, VALUE)                                     \
  if (numeric_name == NAME) {                                                  \
    return make_checked_numeric<COLUMN, TYPE, VALUE>(is_nullable);             \
  }
  if (mode == MappingMode::lossless and numeric_name == "IPv4") {
    if (is_nullable) {
      return std::make_unique<Ipv4Transformer<true>>();
    }
    return std::make_unique<Ipv4Transformer<false>>();
  }
  // Lossless integer mapping must not shadow the legacy boolean fallback.
  if (mode == MappingMode::lossless) {
    CHECKED("UInt8", ColumnUInt8, uint64_type, uint8_t);
  }
  CHECKED("UInt16", ColumnUInt16, uint64_type, uint16_t);
  CHECKED("UInt32", ColumnUInt32, uint64_type, uint32_t);
  CHECKED("Int8", ColumnInt8, int64_type, int8_t);
  CHECKED("Int16", ColumnInt16, int64_type, int16_t);
  CHECKED("Int32", ColumnInt32, int64_type, int32_t);
  CHECKED("Float32", ColumnFloat32, double_type, float);
#undef CHECKED
  return nullptr;
}
} // namespace tenzir::plugins::clickhouse

//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "clickhouse/block_decoding.hpp"

#include <clickhouse/columns/factory.h>
#include <fmt/format.h>

#include <algorithm>
#include <cstring>
#include <limits>

namespace tenzir::plugins::clickhouse {

namespace {

auto pow10(size_t exponent) -> int64_t {
  auto result = int64_t{1};
  for (auto i = size_t{0}; i < exponent; ++i) {
    result *= 10;
  }
  return result;
}

auto check_time_nanos_range(::clickhouse::Int128 value) -> Option<int64_t> {
  auto min = ::clickhouse::Int128{std::numeric_limits<int64_t>::min()};
  auto max = ::clickhouse::Int128{std::numeric_limits<int64_t>::max()};
  if (value < min or value > max) {
    return None{};
  }
  return static_cast<int64_t>(value);
}

auto leaf(DecodedKind kind) -> Option<DecodedType> {
  return DecodedType{.kind = kind, .children = {}, .names = {}};
}

} // namespace

auto classify(::clickhouse::TypeRef const& type) -> Option<DecodedType> {
  auto unwrapped = unwrap_type(type);
  switch (unwrapped.type->GetCode()) {
    case ::clickhouse::Type::Void:
      return leaf(DecodedKind::null);
    case ::clickhouse::Type::Bool:
      return leaf(DecodedKind::bool_);
    case ::clickhouse::Type::Int8:
    case ::clickhouse::Type::Int16:
    case ::clickhouse::Type::Int32:
    case ::clickhouse::Type::Int64:
      return leaf(DecodedKind::int64);
    case ::clickhouse::Type::UInt8:
    case ::clickhouse::Type::UInt16:
    case ::clickhouse::Type::UInt32:
    case ::clickhouse::Type::UInt64:
      return leaf(DecodedKind::uint64);
    case ::clickhouse::Type::Float32:
    case ::clickhouse::Type::Float64:
      return leaf(DecodedKind::double_);
    case ::clickhouse::Type::String:
    case ::clickhouse::Type::FixedString:
    case ::clickhouse::Type::UUID:
    case ::clickhouse::Type::Int128:
    case ::clickhouse::Type::UInt128:
    case ::clickhouse::Type::Enum8:
    case ::clickhouse::Type::Enum16:
    case ::clickhouse::Type::JSON:
      return leaf(DecodedKind::string);
    case ::clickhouse::Type::Date:
    case ::clickhouse::Type::Date32:
    case ::clickhouse::Type::DateTime:
    case ::clickhouse::Type::DateTime64:
      return leaf(DecodedKind::time);
    case ::clickhouse::Type::Time:
    case ::clickhouse::Type::Time64:
      return leaf(DecodedKind::duration);
    case ::clickhouse::Type::IPv4:
    case ::clickhouse::Type::IPv6:
      return leaf(DecodedKind::ip);
    case ::clickhouse::Type::Decimal:
    case ::clickhouse::Type::Decimal32:
    case ::clickhouse::Type::Decimal64:
    case ::clickhouse::Type::Decimal128: {
      auto precision
        = unwrapped.type->As<::clickhouse::DecimalType>()->GetPrecision();
      if (precision > 38) {
        return None{};
      }
      return leaf(DecodedKind::string);
    }
    case ::clickhouse::Type::Array: {
      auto item_type
        = unwrapped.type->As<::clickhouse::ArrayType>()->GetItemType();
      auto child = unwrap_type(item_type);
      if (not child.nullable and not child.low_cardinality
          and child.type->GetCode() == ::clickhouse::Type::UInt8) {
        return leaf(DecodedKind::blob);
      }
      auto element = classify(item_type);
      if (not element) {
        return None{};
      }
      auto result
        = DecodedType{.kind = DecodedKind::list, .children = {}, .names = {}};
      result.children.push_back(std::move(*element));
      return result;
    }
    case ::clickhouse::Type::Tuple: {
      auto tuple = unwrapped.type->As<::clickhouse::TupleType>();
      auto item_types = tuple->GetTupleType();
      if (item_types.empty()) {
        return None{};
      }
      auto result = DecodedType{
        .kind = DecodedKind::record,
        .children = {},
        .names = tuple_field_names(*tuple),
      };
      result.children.reserve(item_types.size());
      for (auto const& item_type : item_types) {
        auto field = classify(item_type);
        if (not field) {
          return None{};
        }
        result.children.push_back(std::move(*field));
      }
      return result;
    }
    case ::clickhouse::Type::Map:
    case ::clickhouse::Type::Point:
    case ::clickhouse::Type::Ring:
    case ::clickhouse::Type::Polygon:
    case ::clickhouse::Type::MultiPolygon:
    case ::clickhouse::Type::Nullable:
    case ::clickhouse::Type::LowCardinality:
      return None{};
  }
  return None{};
}

auto is_decodable_type(std::string_view type) -> bool {
  // The client builds the columns it receives with the same factory, so a type
  // it rejects never reaches the decoder either.
  try {
    auto column = ::clickhouse::CreateColumnByType(std::string{type});
    if (not column) {
      return false;
    }
    return classify(column->Type()).has_value();
  } catch (std::exception const&) {
    return false;
  }
}

auto unwrap_type(::clickhouse::TypeRef type) -> UnwrappedType {
  auto result = UnwrappedType{.type = std::move(type)};
  while (result.type) {
    switch (result.type->GetCode()) {
      case ::clickhouse::Type::Nullable:
        result.nullable = true;
        result.type
          = result.type->As<::clickhouse::NullableType>()->GetNestedType();
        continue;
      case ::clickhouse::Type::LowCardinality:
        result.low_cardinality = true;
        result.type
          = result.type->As<::clickhouse::LowCardinalityType>()->GetNestedType();
        continue;
      default:
        return result;
    }
  }
  return result;
}

auto tuple_field_names(::clickhouse::TupleType const& type)
  -> std::vector<std::string> {
  auto names = std::vector<std::string>{};
  // Unnamed tuples (e.g. `Tuple(UInt8, String)`) expose an empty
  // `GetItemNames()` while `GetTupleType()` still carries the element types, so
  // we drive the loop off the tuple arity and synthesize `field0`, `field1`,
  // ... when names are missing.
  auto arity = type.GetTupleType().size();
  auto const& item_names = type.GetItemNames();
  names.reserve(arity);
  for (auto i = size_t{0}; i < arity; ++i) {
    if (i < item_names.size() and not item_names[i].empty()) {
      names.push_back(item_names[i]);
    } else {
      names.push_back(fmt::format("field{}", i));
    }
  }
  return names;
}

auto format_uint128(::clickhouse::UInt128 value) -> std::string {
  auto result = std::string{};
  while (value != 0) {
    auto digit = static_cast<uint8_t>(value % absl::uint128{10});
    result += static_cast<char>('0' + digit);
    value /= absl::uint128{10};
  }
  if (result.empty()) {
    result = "0";
  }
  std::reverse(result.begin(), result.end());
  return result;
}

auto format_int128(::clickhouse::Int128 value) -> std::string {
  if (value >= 0) {
    return format_uint128(static_cast<absl::uint128>(value));
  }
  auto magnitude = static_cast<absl::uint128>(-(value + 1));
  ++magnitude;
  return fmt::format("-{}", format_uint128(magnitude));
}

auto format_scaled_integer(std::string digits, size_t scale) -> std::string {
  auto negative = false;
  if (not digits.empty() and digits.front() == '-') {
    negative = true;
    digits.erase(digits.begin());
  }
  if (scale > 0) {
    if (digits.size() <= scale) {
      digits.insert(0, scale - digits.size() + 1, '0');
    }
    digits.insert(digits.size() - scale, 1, '.');
  }
  if (negative) {
    digits.insert(digits.begin(), '-');
  }
  return digits;
}

auto format_uuid(::clickhouse::UUID value) -> std::string {
  return fmt::format("{:08x}-{:04x}-{:04x}-{:04x}-{:012x}", value.first >> 32,
                     (value.first >> 16) & 0xffff, value.first & 0xffff,
                     value.second >> 48, value.second & 0xffffffffffffULL);
}

auto format_uuid(std::string_view data) -> std::string {
  auto first = uint64_t{0};
  auto second = uint64_t{0};
  std::memcpy(&first, data.data(), sizeof(first));
  std::memcpy(&second, data.data() + sizeof(first), sizeof(second));
  return format_uuid(::clickhouse::UUID{first, second});
}

auto decimal_from_bytes(std::string_view bytes, size_t scale)
  -> Option<std::string> {
  switch (bytes.size()) {
    case 4: {
      auto value = int32_t{0};
      std::memcpy(&value, bytes.data(), sizeof(value));
      return format_scaled_integer(std::to_string(value), scale);
    }
    case 8: {
      auto value = int64_t{0};
      std::memcpy(&value, bytes.data(), sizeof(value));
      return format_scaled_integer(std::to_string(value), scale);
    }
    case 16: {
      auto value = ::clickhouse::Int128{};
      std::memcpy(&value, bytes.data(), sizeof(value));
      return format_scaled_integer(format_int128(value), scale);
    }
    default:
      return None{};
  }
}

auto rescale_decimal_to_nanos(int64_t value, size_t precision)
  -> Option<int64_t> {
  if (precision == 9) {
    return value;
  }
  if (precision < 9) {
    auto factor = ::clickhouse::Int128{pow10(9 - precision)};
    return check_time_nanos_range(::clickhouse::Int128{value} * factor);
  }
  return value / pow10(precision - 9);
}

auto seconds_to_nanos(int64_t seconds) -> Option<int64_t> {
  return check_time_nanos_range(::clickhouse::Int128{seconds}
                                * ::clickhouse::Int128{1'000'000'000});
}

auto days_to_nanos(int64_t days) -> Option<int64_t> {
  auto seconds = ::clickhouse::Int128{days} * ::clickhouse::Int128{86400};
  return check_time_nanos_range(seconds * ::clickhouse::Int128{1'000'000'000});
}

auto emit_unsupported_column_warning(value_path path, std::string_view text,
                                     diagnostic_handler& dh) -> void {
  diagnostic::warning("dropping ClickHouse column `{}` with unsupported type "
                      "`{}`",
                      path, text)
    .hint("cast unsupported columns in SQL or omit them from the result")
    .emit(dh);
}

auto emit_empty_block_warning(std::string_view schema_name,
                              diagnostic_handler& dh) -> void {
  diagnostic::warning("dropping ClickHouse block for schema `{}` because no "
                      "supported columns remained",
                      schema_name)
    .emit(dh);
}

} // namespace tenzir::plugins::clickhouse

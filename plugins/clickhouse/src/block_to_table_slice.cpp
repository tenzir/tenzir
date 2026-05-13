//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "clickhouse/block_to_table_slice.hpp"

#include "clickhouse/column_variant_traits.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/series.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/value_path.hpp"

#include <arpa/inet.h>
#include <clickhouse/columns/array.h>
#include <clickhouse/columns/date.h>
#include <clickhouse/columns/decimal.h>
#include <clickhouse/columns/enum.h>
#include <clickhouse/columns/ip4.h>
#include <clickhouse/columns/ip6.h>
#include <clickhouse/columns/nullable.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/tuple.h>
#include <fmt/format.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <type_traits>
#include <unordered_map>
#include <utility>

namespace tenzir::plugins::clickhouse {

namespace {

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

auto duration_from_nanos(Option<int64_t> nanos) -> Option<duration> {
  if (not nanos) {
    return None{};
  }
  return std::chrono::duration_cast<duration>(std::chrono::nanoseconds{*nanos});
}

auto time_from_nanos(Option<int64_t> nanos) -> Option<time> {
  if (not nanos) {
    return None{};
  }
  return time{
    std::chrono::duration_cast<duration>(std::chrono::nanoseconds{*nanos})};
}

auto duration_from_clickhouse_time(int64_t seconds) -> Option<duration> {
  return duration_from_nanos(seconds_to_nanos(seconds));
}

auto time_from_unix_seconds(int64_t seconds) -> Option<time> {
  return time_from_nanos(seconds_to_nanos(seconds));
}

auto time_from_unix_days(int64_t days) -> Option<time> {
  return time_from_nanos(days_to_nanos(days));
}

template <class Address>
auto parse_ip(std::string_view text, int family) -> Option<ip> {
  constexpr auto width = std::same_as<Address, in_addr> ? 4UZ : 16UZ;
  auto addr = Address{};
  auto owned = std::string{text};
  if (inet_pton(family, owned.c_str(), &addr) != 1) {
    return None{};
  }
  auto view = std::span<const uint8_t, width>{
    reinterpret_cast<const uint8_t*>(&addr), width};
  if constexpr (width == 4UZ) {
    return ip::v4(view);
  } else {
    return ip::v6(view);
  }
}

template <size_t Width>
auto parse_ip_bytes(std::string_view bytes) -> Option<ip> {
  if (bytes.size() != Width) {
    return None{};
  }
  auto view = std::span<const uint8_t, Width>{
    reinterpret_cast<const uint8_t*>(bytes.data()), Width};
  if constexpr (Width == 4UZ) {
    return ip::v4(view);
  } else {
    return ip::v6(view);
  }
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

struct unwrapped_type {
  ::clickhouse::TypeRef type;
  bool nullable = false;
  bool low_cardinality = false;
};

auto unwrap_type(::clickhouse::TypeRef type) -> unwrapped_type {
  auto result = unwrapped_type{.type = std::move(type)};
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
  auto const& item_names = type.GetItemNames();
  names.reserve(item_names.size());
  for (auto i = size_t{0}; i < item_names.size(); ++i) {
    if (not item_names[i].empty()) {
      names.push_back(item_names[i]);
    } else {
      names.push_back(fmt::format("field{}", i));
    }
  }
  return names;
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

auto infer_type(::clickhouse::TypeRef const& type_ref, value_path path,
                diagnostic_handler& dh) -> Option<type> {
  auto unwrapped = unwrap_type(type_ref);
  auto code = unwrapped.type->GetCode();
  switch (code) {
    case ::clickhouse::Type::Void:
      return type{null_type{}};
    case ::clickhouse::Type::Bool:
      return type{bool_type{}};
    case ::clickhouse::Type::Int8:
    case ::clickhouse::Type::Int16:
    case ::clickhouse::Type::Int32:
    case ::clickhouse::Type::Int64:
      return type{int64_type{}};
    case ::clickhouse::Type::UInt8:
    case ::clickhouse::Type::UInt16:
    case ::clickhouse::Type::UInt32:
    case ::clickhouse::Type::UInt64:
      return type{uint64_type{}};
    case ::clickhouse::Type::Float32:
    case ::clickhouse::Type::Float64:
      return type{double_type{}};
    case ::clickhouse::Type::String:
    case ::clickhouse::Type::FixedString:
    case ::clickhouse::Type::UUID:
    case ::clickhouse::Type::Int128:
    case ::clickhouse::Type::UInt128:
    case ::clickhouse::Type::Enum8:
    case ::clickhouse::Type::Enum16:
      return type{string_type{}};
    case ::clickhouse::Type::Date:
    case ::clickhouse::Type::Date32:
    case ::clickhouse::Type::DateTime:
    case ::clickhouse::Type::DateTime64:
      return type{time_type{}};
    case ::clickhouse::Type::Time:
    case ::clickhouse::Type::Time64:
      return type{duration_type{}};
    case ::clickhouse::Type::IPv4:
    case ::clickhouse::Type::IPv6:
      return type{ip_type{}};
    case ::clickhouse::Type::Decimal:
    case ::clickhouse::Type::Decimal32:
    case ::clickhouse::Type::Decimal64:
    case ::clickhouse::Type::Decimal128: {
      auto precision
        = unwrapped.type->As<::clickhouse::DecimalType>()->GetPrecision();
      if (precision > 38) {
        return None{};
      }
      return type{string_type{}};
    }
    case ::clickhouse::Type::Array: {
      auto child = unwrap_type(
        unwrapped.type->As<::clickhouse::ArrayType>()->GetItemType());
      if (not child.nullable and not child.low_cardinality
          and child.type->GetCode() == ::clickhouse::Type::UInt8) {
        return type{blob_type{}};
      }
      auto value_type = infer_type(
        unwrapped.type->As<::clickhouse::ArrayType>()->GetItemType(),
        path.list(), dh);
      if (not value_type) {
        return None{};
      }
      return type{list_type{*value_type}};
    }
    case ::clickhouse::Type::Tuple: {
      auto tuple = unwrapped.type->As<::clickhouse::TupleType>();
      auto item_types = tuple->GetTupleType();
      if (item_types.empty()) {
        return None{};
      }
      auto names = tuple_field_names(*tuple);
      auto fields = std::vector<struct record_type::field>{};
      fields.reserve(item_types.size());
      for (auto i = size_t{0}; i < item_types.size(); ++i) {
        auto field_type = infer_type(item_types[i], path.field(names[i]), dh);
        if (not field_type) {
          return None{};
        }
        fields.emplace_back(names[i], *field_type);
      }
      return type{record_type{fields}};
    }
    case ::clickhouse::Type::Map:
      return None{};
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

struct series_matcher {
  struct normalized_column {
    ::clickhouse::ColumnRef original;
    ::clickhouse::ColumnRef effective;
    std::shared_ptr<::clickhouse::ColumnNullable> nullable = nullptr;
    unwrapped_type logical_type = {};
    type output_type = {};
  };

  value_path path;
  diagnostic_handler& dh;
  builder_ref field;
  normalized_column column;
  bool warned_malformed = false;

  template <class F>
  auto for_each_present_row(F&& f) -> void {
    for (auto row = size_t{0}; row < column.original->Size(); ++row) {
      if (row_is_null(row)) {
        field.null();
        continue;
      }
      f(row);
    }
  }

  template <class... Ts>
  auto warn_malformed(fmt::format_string<Ts...> str, Ts&&... xs) -> void {
    if (warned_malformed) {
      return;
    }
    diagnostic::warning("malformed ClickHouse values in `{}`: {}", path,
                        fmt::format(str, std::forward<Ts>(xs)...))
      .emit(dh);
    warned_malformed = true;
  }

  template <class... Ts>
  auto null_with_warning(fmt::format_string<Ts...> str, Ts&&... xs) -> void {
    warn_malformed(str, std::forward<Ts>(xs)...);
    field.null();
  }

  template <class... Ts>
  auto
  null_entire_column_with_warning(fmt::format_string<Ts...> str, Ts&&... xs)
    -> void {
    warn_malformed(str, std::forward<Ts>(xs)...);
    for (auto row = size_t{0}; row < column.original->Size(); ++row) {
      field.null();
    }
  }

  template <class T, class... Ts>
  auto
  append_or_null(Option<T> value, fmt::format_string<Ts...> str, Ts&&... xs)
    -> void {
    if (value) {
      field.data(*value);
    } else {
      null_with_warning(str, std::forward<Ts>(xs)...);
    }
  }

  static auto make(value_path path, diagnostic_handler& dh, builder_ref field,
                   ::clickhouse::ColumnRef const& column)
    -> Option<series_matcher> {
    auto result = normalized_column{
      .original = column,
      .effective = column,
      .logical_type = unwrap_type(column->Type()),
    };
    if (auto actual_nullable = column->As<::clickhouse::ColumnNullable>()) {
      result.nullable = actual_nullable;
      result.effective = actual_nullable->Nested();
    }
    auto output_type = infer_type(column->Type(), path, dh);
    if (not output_type) {
      return None{};
    }
    result.output_type = *output_type;
    return series_matcher{
      .path = std::move(path),
      .dh = dh,
      .field = field,
      .column = std::move(result),
    };
  }

  auto build() -> void {
    match(*column.effective, [&](auto const& matched) -> void {
      append(matched);
    });
  }

  auto row_is_null(size_t row) const -> bool {
    if (column.nullable) {
      return column.nullable->IsNull(row);
    }
    auto code = column.logical_type.type->GetCode();
    return column.logical_type.nullable and code != ::clickhouse::Type::Tuple
           and code != ::clickhouse::Type::Array
           and column.original->GetItem(row).type == ::clickhouse::Type::Void;
  }

  auto append(::clickhouse::ColumnNothing const&) -> void {
    for (auto row = size_t{0}; row < column.original->Size(); ++row) {
      field.null();
    }
  }

  template <class T>
    requires(std::integral<T> or std::floating_point<T>
             or std::same_as<T, ::clickhouse::Bool>)
  auto append(::clickhouse::ColumnVector<T> const& values) -> void {
    for_each_present_row([&](size_t row) {
      if constexpr (std::same_as<T, ::clickhouse::Bool>) {
        field.data(values.At(row) == ::clickhouse::true_);
      } else if constexpr (std::signed_integral<T>) {
        field.data(int64_t{values.At(row)});
      } else if constexpr (std::floating_point<T>) {
        field.data(double{values.At(row)});
      } else {
        field.data(uint64_t{values.At(row)});
      }
    });
  }

  template <class Column>
    requires concepts::one_of<Column, ::clickhouse::ColumnString,
                              ::clickhouse::ColumnFixedString>
  auto append(Column const& values) -> void {
    for_each_present_row([&](size_t row) {
      field.data(values.At(row));
    });
  }

  template <class Column>
    requires concepts::one_of<Column, ::clickhouse::ColumnEnum8,
                              ::clickhouse::ColumnEnum16>
  auto append(Column const& values) -> void {
    for_each_present_row([&](size_t row) {
      field.data(values.NameAt(row));
    });
  }

  auto append(::clickhouse::ColumnInt128 const& values) -> void {
    for_each_present_row([&](size_t row) {
      field.data(format_int128(values.At(row)));
    });
  }

  auto append(::clickhouse::ColumnUInt128 const& values) -> void {
    for_each_present_row([&](size_t row) {
      field.data(format_uint128(values.At(row)));
    });
  }

  auto append(::clickhouse::ColumnUUID const& values) -> void {
    for_each_present_row([&](size_t row) {
      field.data(format_uuid(values.At(row)));
    });
  }

  template <class Column>
    requires concepts::one_of<Column, ::clickhouse::ColumnDate,
                              ::clickhouse::ColumnDate32>
  auto append_days(Column const& values, std::string_view kind) -> void {
    for_each_present_row([&](size_t row) {
      append_or_null(time_from_unix_days(int64_t{values.At(row)}),
                     "{} value is out of range after rescaling to nanoseconds",
                     kind);
    });
  }

  auto append(::clickhouse::ColumnDate const& values) -> void {
    append_days(values, "Date");
  }

  auto append(::clickhouse::ColumnDate32 const& values) -> void {
    append_days(values, "Date32");
  }

  auto append(::clickhouse::ColumnDateTime const& values) -> void {
    for_each_present_row([&](size_t row) {
      append_or_null(time_from_unix_seconds(int64_t{values.At(row)}),
                     "DateTime value is out of range after rescaling to "
                     "nanoseconds");
    });
  }

  auto append(::clickhouse::ColumnDateTime64 const& values) -> void {
    for_each_present_row([&](size_t row) {
      append_or_null(time_from_nanos(rescale_decimal_to_nanos(
                       values.At(row), values.GetPrecision())),
                     "DateTime64 value is out of range after rescaling to "
                     "nanoseconds");
    });
  }

  auto append(::clickhouse::ColumnTime const& values) -> void {
    for_each_present_row([&](size_t row) {
      append_or_null(duration_from_clickhouse_time(int64_t{values.At(row)}),
                     "Time value is out of range after rescaling to "
                     "nanoseconds");
    });
  }

  auto append(::clickhouse::ColumnTime64 const& values) -> void {
    for_each_present_row([&](size_t row) {
      append_or_null(duration_from_nanos(rescale_decimal_to_nanos(
                       values.At(row), values.GetPrecision())),
                     "Time64 value is out of range after rescaling to "
                     "nanoseconds");
    });
  }

  template <class Column, class Address>
    requires concepts::one_of<Column, ::clickhouse::ColumnIPv4,
                              ::clickhouse::ColumnIPv6>
  auto append_ip(Column const& values, int family) -> void {
    for_each_present_row([&](size_t row) {
      append_or_null(parse_ip<Address>(values.AsString(row), family),
                     "expected valid IP data for ClickHouse type `{}`",
                     values.GetType().GetName());
    });
  }

  auto append(::clickhouse::ColumnIPv4 const& values) -> void {
    return append_ip<::clickhouse::ColumnIPv4, in_addr>(values, AF_INET);
  }

  auto append(::clickhouse::ColumnIPv6 const& values) -> void {
    return append_ip<::clickhouse::ColumnIPv6, in6_addr>(values, AF_INET6);
  }

  auto append(::clickhouse::ColumnDecimal const& values) -> void {
    for_each_present_row([&](size_t row) {
      field.data(format_scaled_integer(format_int128(values.At(row)),
                                       values.GetScale()));
    });
  }

  auto append(::clickhouse::ColumnTuple const& values) -> void {
    auto tuple = values.GetType().template As<::clickhouse::TupleType>();
    auto names = tuple_field_names(*tuple);
    if (values.TupleSize() != names.size()) {
      null_entire_column_with_warning("unexpected tuple size");
      return;
    }
    auto record = field.record();
    for (auto i = size_t{0}; i < names.size(); ++i) {
      auto child
        = make(path.field(names[i]), dh, record.field(names[i]), values.At(i));
      if (child) {
        child->build();
      } else {
        diagnostic::warning("malformed ClickHouse values in `{}`: unsupported "
                            "nested ClickHouse type `{}`",
                            path.field(names[i]),
                            values.At(i)->Type()->GetName())
          .emit(dh);
        record.field(names[i]).null();
      }
    }
  }

  auto append(::clickhouse::ColumnArray const& values) -> void {
    auto item_type
      = values.GetType().template As<::clickhouse::ArrayType>()->GetItemType();
    auto child_type = unwrap_type(item_type);
    for (auto row = size_t{0}; row < column.original->Size(); ++row) {
      if (row_is_null(row)) {
        field.null();
        continue;
      }
      if (not child_type.nullable and not child_type.low_cardinality
          and child_type.type->GetCode() == ::clickhouse::Type::UInt8) {
        auto nested = values.GetAsColumn(row);
        auto bytes = nested->template As<::clickhouse::ColumnUInt8>();
        if (not bytes) {
          null_with_warning("expected Array(UInt8) data");
          continue;
        }
        auto value = blob{};
        value.reserve(bytes->Size());
        for (auto i = size_t{0}; i < bytes->Size(); ++i) {
          value.push_back(static_cast<std::byte>(bytes->At(i)));
        }
        field.data(std::move(value));
        continue;
      }
      auto child = make(path.list(), dh, field.list(), values.GetAsColumn(row));
      if (child) {
        child->build();
      } else {
        null_with_warning("unsupported nested ClickHouse type `{}`",
                          values.GetAsColumn(row)->Type()->GetName());
      }
    }
  }

  auto append(::clickhouse::ColumnLowCardinality const&) -> void {
    auto code = column.logical_type.type->GetCode();
    switch (code) {
      case ::clickhouse::Type::Void:
        for_each_present_row([&](size_t) {
          field.null();
        });
        break;
      case ::clickhouse::Type::Bool:
        for_each_present_row([&](size_t row) {
          auto item = column.original->GetItem(row);
          field.data(item.get<::clickhouse::Bool>() == ::clickhouse::true_);
        });
        break;
      case ::clickhouse::Type::UInt8:
        for_each_present_row([&](size_t row) {
          auto item = column.original->GetItem(row);
          field.data(uint64_t{item.get<uint8_t>()});
        });
        break;
      case ::clickhouse::Type::UInt16:
        for_each_present_row([&](size_t row) {
          auto item = column.original->GetItem(row);
          field.data(uint64_t{item.get<uint16_t>()});
        });
        break;
      case ::clickhouse::Type::UInt32:
        for_each_present_row([&](size_t row) {
          auto item = column.original->GetItem(row);
          field.data(uint64_t{item.get<uint32_t>()});
        });
        break;
      case ::clickhouse::Type::UInt64:
        for_each_present_row([&](size_t row) {
          auto item = column.original->GetItem(row);
          field.data(item.get<uint64_t>());
        });
        break;
      case ::clickhouse::Type::Int8:
        for_each_present_row([&](size_t row) {
          auto item = column.original->GetItem(row);
          field.data(int64_t{item.get<int8_t>()});
        });
        break;
      case ::clickhouse::Type::Int16:
        for_each_present_row([&](size_t row) {
          auto item = column.original->GetItem(row);
          field.data(int64_t{item.get<int16_t>()});
        });
        break;
      case ::clickhouse::Type::Int32:
        for_each_present_row([&](size_t row) {
          auto item = column.original->GetItem(row);
          field.data(int64_t{item.get<int32_t>()});
        });
        break;
      case ::clickhouse::Type::Int64:
        for_each_present_row([&](size_t row) {
          auto item = column.original->GetItem(row);
          field.data(item.get<int64_t>());
        });
        break;
      case ::clickhouse::Type::Float32:
        for_each_present_row([&](size_t row) {
          auto item = column.original->GetItem(row);
          field.data(double{item.get<float>()});
        });
        break;
      case ::clickhouse::Type::Float64:
        for_each_present_row([&](size_t row) {
          auto item = column.original->GetItem(row);
          field.data(item.get<double>());
        });
        break;
      case ::clickhouse::Type::String:
      case ::clickhouse::Type::FixedString:
        for_each_present_row([&](size_t row) {
          auto item = column.original->GetItem(row);
          field.data(std::string{item.get<std::string_view>()});
        });
        break;
      case ::clickhouse::Type::UUID:
        for_each_present_row([&](size_t row) {
          auto item = column.original->GetItem(row);
          field.data(format_uuid(item.AsBinaryData()));
        });
        break;
      case ::clickhouse::Type::Enum8: {
        auto enum_type = column.logical_type.type->As<::clickhouse::EnumType>();
        for_each_present_row([&](size_t row) {
          auto item = column.original->GetItem(row);
          field.data(std::string{enum_type->GetEnumName(item.get<int8_t>())});
        });
        break;
      }
      case ::clickhouse::Type::Enum16: {
        auto enum_type = column.logical_type.type->As<::clickhouse::EnumType>();
        for_each_present_row([&](size_t row) {
          auto item = column.original->GetItem(row);
          field.data(std::string{enum_type->GetEnumName(item.get<int16_t>())});
        });
        break;
      }
      case ::clickhouse::Type::Int128:
        for_each_present_row([&](size_t row) {
          auto item = column.original->GetItem(row);
          field.data(format_int128(item.get<::clickhouse::Int128>()));
        });
        break;
      case ::clickhouse::Type::UInt128:
        for_each_present_row([&](size_t row) {
          auto item = column.original->GetItem(row);
          field.data(format_uint128(item.get<::clickhouse::UInt128>()));
        });
        break;
      case ::clickhouse::Type::Date:
        for_each_present_row([&](size_t row) {
          auto item = column.original->GetItem(row);
          append_or_null(time_from_unix_days(item.get<uint16_t>()),
                         "Date value is out of range after rescaling to "
                         "nanoseconds");
        });
        break;
      case ::clickhouse::Type::Date32:
        for_each_present_row([&](size_t row) {
          auto item = column.original->GetItem(row);
          append_or_null(time_from_unix_days(item.get<int32_t>()),
                         "Date32 value is out of range after rescaling to "
                         "nanoseconds");
        });
        break;
      case ::clickhouse::Type::DateTime:
        for_each_present_row([&](size_t row) {
          auto item = column.original->GetItem(row);
          append_or_null(time_from_unix_seconds(item.get<uint32_t>()),
                         "DateTime value is out of range after rescaling to "
                         "nanoseconds");
        });
        break;
      case ::clickhouse::Type::DateTime64: {
        auto precision
          = column.logical_type.type->As<::clickhouse::DateTime64Type>()
              ->GetPrecision();
        for_each_present_row([&](size_t row) {
          auto item = column.original->GetItem(row);
          append_or_null(
            time_from_nanos(
              rescale_decimal_to_nanos(item.get<int64_t>(), precision)),
            "DateTime64 value is out of range after rescaling to nanoseconds");
        });
        break;
      }
      case ::clickhouse::Type::Time:
        for_each_present_row([&](size_t row) {
          auto item = column.original->GetItem(row);
          append_or_null(duration_from_clickhouse_time(item.get<int32_t>()),
                         "Time value is out of range after rescaling to "
                         "nanoseconds");
        });
        break;
      case ::clickhouse::Type::Time64: {
        auto precision
          = column.logical_type.type->As<::clickhouse::Time64Type>()
              ->GetPrecision();
        for_each_present_row([&](size_t row) {
          auto item = column.original->GetItem(row);
          append_or_null(
            duration_from_nanos(
              rescale_decimal_to_nanos(item.get<int64_t>(), precision)),
            "Time64 value is out of range after rescaling to nanoseconds");
        });
        break;
      }
      case ::clickhouse::Type::IPv4:
        for_each_present_row([&](size_t row) {
          auto item = column.original->GetItem(row);
          append_or_null(parse_ip_bytes<4UZ>(item.AsBinaryData()),
                         "expected 4-byte IPv4 data");
        });
        break;
      case ::clickhouse::Type::IPv6:
        for_each_present_row([&](size_t row) {
          auto item = column.original->GetItem(row);
          append_or_null(parse_ip_bytes<16UZ>(item.AsBinaryData()),
                         "expected 16-byte IPv6 data");
        });
        break;
      case ::clickhouse::Type::Decimal:
      case ::clickhouse::Type::Decimal32:
      case ::clickhouse::Type::Decimal64:
      case ::clickhouse::Type::Decimal128: {
        auto scale = column.logical_type.type->As<::clickhouse::DecimalType>()
                       ->GetScale();
        for_each_present_row([&](size_t row) {
          auto item = column.original->GetItem(row);
          append_or_null(decimal_from_bytes(item.AsBinaryData(), scale),
                         "expected decimal payload width of 4, 8, or 16 bytes");
        });
        break;
      }
      case ::clickhouse::Type::Nullable:
      case ::clickhouse::Type::Array:
      case ::clickhouse::Type::Tuple:
      case ::clickhouse::Type::LowCardinality:
      case ::clickhouse::Type::Map:
      case ::clickhouse::Type::Point:
      case ::clickhouse::Type::Ring:
      case ::clickhouse::Type::Polygon:
      case ::clickhouse::Type::MultiPolygon:
        null_entire_column_with_warning("unsupported LowCardinality ClickHouse "
                                        "type `{}`",
                                        column.logical_type.type->GetName());
        break;
    }
  }

  template <class Column>
  auto append(Column const& values) -> void {
    if constexpr (std::same_as<Column, ::clickhouse::ColumnNullable>) {
      TENZIR_UNREACHABLE();
    } else {
      null_entire_column_with_warning("unsupported ClickHouse runtime type "
                                      "`{}`",
                                      values.GetType().GetName());
    }
  }
};

auto build_series(::clickhouse::ColumnRef const& column, value_path path,
                  diagnostic_handler& dh) -> Option<series> {
  auto output_type = infer_type(column->Type(), path, dh);
  if (not output_type) {
    emit_unsupported_column_warning(path, column->Type()->GetName(), dh);
    return None{};
  }
  auto builder = series_builder{*output_type};
  auto matcher
    = series_matcher::make(std::move(path), dh, builder_ref{builder}, column);
  if (not matcher) {
    emit_unsupported_column_warning(path, column->Type()->GetName(), dh);
    return None{};
  }
  matcher->build();
  return builder.finish_assert_one_array();
}

} // namespace

auto block_to_table_slice(::clickhouse::Block const& block,
                          std::string_view schema_name, diagnostic_handler& dh)
  -> Option<table_slice> {
  if (block.GetColumnCount() == 0) {
    emit_empty_block_warning(schema_name, dh);
    return None{};
  }
  auto arrays = std::vector<std::shared_ptr<arrow::Array>>{};
  auto fields = std::vector<struct record_type::field>{};
  arrays.reserve(block.GetColumnCount());
  fields.reserve(block.GetColumnCount());
  for (auto column = size_t{0}; column < block.GetColumnCount(); ++column) {
    auto result = build_series(
      block[column], value_path{}.field(block.GetColumnName(column)), dh);
    if (not result) {
      continue;
    }
    arrays.push_back(std::move(result->array));
    fields.emplace_back(block.GetColumnName(column), std::move(result->type));
  }
  if (fields.empty()) {
    emit_empty_block_warning(schema_name, dh);
    return None{};
  }
  auto runtime_schema = type{schema_name, record_type{fields}};
  auto batch = arrow::RecordBatch::Make(
    runtime_schema.to_arrow_schema(),
    detail::narrow<std::int64_t>(block.GetRowCount()), std::move(arrays));
  return table_slice{std::move(batch), std::move(runtime_schema)};
}

} // namespace tenzir::plugins::clickhouse

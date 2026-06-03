//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "clickhouse/block_to_table_slice.hpp"

#include "clickhouse/column_variant_traits.hpp"
#include "tenzir/arrow_table_slice.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/series.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/value_path.hpp"

#include <arpa/inet.h>
#include <arrow/util/bit_util.h>
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

using ConstColumnRef = std::shared_ptr<::clickhouse::Column const>;

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
    case ::clickhouse::Type::Point:
    case ::clickhouse::Type::Ring:
    case ::clickhouse::Type::Polygon:
    case ::clickhouse::Type::MultiPolygon:
    case ::clickhouse::Type::Nullable:
    case ::clickhouse::Type::LowCardinality:
    case ::clickhouse::Type::JSON:
      return None{};
  }
  return None{};
}

struct normalized_column {
  ConstColumnRef original;
  ConstColumnRef effective;
  std::shared_ptr<::clickhouse::ColumnNullable const> nullable = nullptr;
  unwrapped_type logical_type = {};
  type output_type = {};
};

struct warning_state {
  bool malformed = false;
};

auto normalize_column(ConstColumnRef const& column, value_path path,
                      diagnostic_handler& dh) -> Option<normalized_column> {
  auto output_type = infer_type(column->Type(), path, dh);
  if (not output_type) {
    return None{};
  }
  auto result = normalized_column{
    .original = column,
    .effective = column,
    .logical_type = unwrap_type(column->Type()),
    .output_type = *output_type,
  };
  if (auto nullable = column->As<::clickhouse::ColumnNullable>()) {
    result.nullable = nullable;
    result.effective = nullable->Nested();
  }
  return result;
}

auto row_is_null(normalized_column const& column, size_t row) -> bool {
  if (column.nullable) {
    return column.nullable->IsNull(row);
  }
  auto code = column.logical_type.type->GetCode();
  return column.logical_type.nullable and code != ::clickhouse::Type::Tuple
         and code != ::clickhouse::Type::Array
         and column.original->GetItem(row).type == ::clickhouse::Type::Void;
}

template <class... Ts>
auto warn_malformed(warning_state& warnings, value_path const& path,
                    diagnostic_handler& dh, fmt::format_string<Ts...> str,
                    Ts&&... xs) -> void {
  if (warnings.malformed) {
    return;
  }
  diagnostic::warning("malformed ClickHouse values in `{}`: {}", path,
                      fmt::format(str, std::forward<Ts>(xs)...))
    .emit(dh);
  warnings.malformed = true;
}

template <class... Ts>
auto null_with_warning(warning_state& warnings, builder_ref field,
                       value_path const& path, diagnostic_handler& dh,
                       fmt::format_string<Ts...> str, Ts&&... xs) -> void {
  warn_malformed(warnings, path, dh, str, std::forward<Ts>(xs)...);
  field.null();
}

template <class... Ts>
auto null_entire_column_with_warning(normalized_column const& column,
                                     warning_state& warnings, builder_ref field,
                                     value_path const& path,
                                     diagnostic_handler& dh,
                                     fmt::format_string<Ts...> str, Ts&&... xs)
  -> void {
  warn_malformed(warnings, path, dh, str, std::forward<Ts>(xs)...);
  for (auto row = size_t{0}; row < column.original->Size(); ++row) {
    field.null();
  }
}

template <class T, class... Ts>
auto append_or_null(Option<T> value, warning_state& warnings, builder_ref field,
                    value_path const& path, diagnostic_handler& dh,
                    fmt::format_string<Ts...> str, Ts&&... xs) -> void {
  if (value) {
    field.data(*value);
  } else {
    null_with_warning(warnings, field, path, dh, str, std::forward<Ts>(xs)...);
  }
}

template <class F>
auto for_each_present_row(normalized_column const& column, builder_ref field,
                          F&& f) -> void {
  for (auto row = size_t{0}; row < column.original->Size(); ++row) {
    if (row_is_null(column, row)) {
      field.null();
      continue;
    }
    f(row);
  }
}

auto build_series(ConstColumnRef const& column, value_path path,
                  diagnostic_handler& dh) -> Option<series>;

auto make_null_bitmap(normalized_column const& column)
  -> std::shared_ptr<arrow::Buffer> {
  if (not column.nullable) {
    return nullptr;
  }
  auto length = detail::narrow<int64_t>(column.original->Size());
  auto bitmap = check(arrow::AllocateBitmap(length, arrow_memory_pool()));
  arrow::bit_util::SetBitsTo(bitmap->mutable_data(), 0, length, true);
  for (auto row = int64_t{0}; row < length; ++row) {
    if (column.nullable->IsNull(detail::narrow<size_t>(row))) {
      arrow::bit_util::SetBitTo(bitmap->mutable_data(), row, false);
    }
  }
  return bitmap;
}

auto build_tuple_series(normalized_column const& column,
                        ::clickhouse::ColumnTuple const& values,
                        value_path const& path, diagnostic_handler& dh)
  -> Option<series> {
  const auto* const tuple
    = values.GetType().template As<::clickhouse::TupleType>();
  auto names = tuple_field_names(*tuple);
  if (values.TupleSize() != names.size()) {
    diagnostic::warning("malformed ClickHouse values in `{}`: unexpected "
                        "tuple size",
                        path)
      .emit(dh);
    return None{};
  }
  auto fields
    = std::vector<std::pair<std::string, std::shared_ptr<arrow::Array>>>{};
  fields.reserve(names.size());
  for (auto i = size_t{0}; i < names.size(); ++i) {
    auto child = build_series(values.At(i), path.field(names[i]), dh);
    if (not child) {
      return None{};
    }
    if (child->length() != detail::narrow<int64_t>(column.original->Size())) {
      diagnostic::warning("malformed ClickHouse values in `{}`: unexpected "
                          "tuple field length",
                          path.field(names[i]))
        .emit(dh);
      return None{};
    }
    fields.emplace_back(std::move(names[i]), std::move(child->array));
  }
  auto const& record = as<record_type>(column.output_type);
  auto array
    = make_struct_array(detail::narrow<int64_t>(column.original->Size()),
                        make_null_bitmap(column), std::move(fields), record);
  return series{record, std::move(array)};
}

template <class T>
  requires(std::integral<T> or std::floating_point<T>)
auto build_column(normalized_column const& column,
                  ::clickhouse::ColumnVector<T> const& values,
                  value_path const&, diagnostic_handler&) -> series {
  auto builder = series_builder{column.output_type};
  auto field = builder_ref{builder};
  for_each_present_row(column, field, [&](size_t row) {
    if constexpr (std::signed_integral<T>) {
      field.data(int64_t{values.At(row)});
    } else if constexpr (std::floating_point<T>) {
      field.data(double{values.At(row)});
    } else {
      field.data(uint64_t{values.At(row)});
    }
  });
  return builder.finish_assert_one_array();
}

auto build_column(normalized_column const& column,
                  ::clickhouse::ColumnBool const& values, value_path const&,
                  diagnostic_handler&) -> series {
  auto builder = series_builder{column.output_type};
  auto field = builder_ref{builder};
  for_each_present_row(column, field, [&](size_t row) {
    field.data(values.At(row));
  });
  return builder.finish_assert_one_array();
}

template <class Column>
  requires concepts::one_of<Column, ::clickhouse::ColumnString,
                            ::clickhouse::ColumnFixedString>
auto build_column(normalized_column const& column, Column const& values,
                  value_path const&, diagnostic_handler&) -> series {
  auto builder = series_builder{column.output_type};
  auto field = builder_ref{builder};
  for_each_present_row(column, field, [&](size_t row) {
    field.data(values.At(row));
  });
  return builder.finish_assert_one_array();
}

template <class Column>
  requires concepts::one_of<Column, ::clickhouse::ColumnEnum8,
                            ::clickhouse::ColumnEnum16>
auto build_column(normalized_column const& column, Column const& values,
                  value_path const&, diagnostic_handler&) -> series {
  auto builder = series_builder{column.output_type};
  auto field = builder_ref{builder};
  for_each_present_row(column, field, [&](size_t row) {
    field.data(values.NameAt(row));
  });
  return builder.finish_assert_one_array();
}

auto build_column(normalized_column const& column,
                  ::clickhouse::ColumnInt128 const& values, value_path const&,
                  diagnostic_handler&) -> series {
  auto builder = series_builder{column.output_type};
  auto field = builder_ref{builder};
  for_each_present_row(column, field, [&](size_t row) {
    field.data(format_int128(values.At(row)));
  });
  return builder.finish_assert_one_array();
}

auto build_column(normalized_column const& column,
                  ::clickhouse::ColumnUInt128 const& values, value_path const&,
                  diagnostic_handler&) -> series {
  auto builder = series_builder{column.output_type};
  auto field = builder_ref{builder};
  for_each_present_row(column, field, [&](size_t row) {
    field.data(format_uint128(values.At(row)));
  });
  return builder.finish_assert_one_array();
}

auto build_column(normalized_column const& column,
                  ::clickhouse::ColumnUUID const& values, value_path const&,
                  diagnostic_handler&) -> series {
  auto builder = series_builder{column.output_type};
  auto field = builder_ref{builder};
  for_each_present_row(column, field, [&](size_t row) {
    field.data(format_uuid(values.At(row)));
  });
  return builder.finish_assert_one_array();
}

auto build_column(normalized_column const& column,
                  ::clickhouse::ColumnDate const& values,
                  value_path const& path, diagnostic_handler& dh) -> series {
  auto warnings = warning_state{};
  auto builder = series_builder{column.output_type};
  auto field = builder_ref{builder};
  for_each_present_row(column, field, [&](size_t row) {
    append_or_null(time_from_unix_days(int64_t{values.At(row)}), warnings,
                   field, path, dh,
                   "Date value is out of range after rescaling to nanoseconds");
  });
  return builder.finish_assert_one_array();
}

auto build_column(normalized_column const& column,
                  ::clickhouse::ColumnDate32 const& values,
                  value_path const& path, diagnostic_handler& dh) -> series {
  auto warnings = warning_state{};
  auto builder = series_builder{column.output_type};
  auto field = builder_ref{builder};
  for_each_present_row(column, field, [&](size_t row) {
    append_or_null(
      time_from_unix_days(int64_t{values.At(row)}), warnings, field, path, dh,
      "Date32 value is out of range after rescaling to nanoseconds");
  });
  return builder.finish_assert_one_array();
}

auto build_column(normalized_column const& column,
                  ::clickhouse::ColumnDateTime const& values,
                  value_path const& path, diagnostic_handler& dh) -> series {
  auto warnings = warning_state{};
  auto builder = series_builder{column.output_type};
  auto field = builder_ref{builder};
  for_each_present_row(column, field, [&](size_t row) {
    append_or_null(time_from_unix_seconds(int64_t{values.At(row)}), warnings,
                   field, path, dh,
                   "DateTime value is out of range after rescaling to "
                   "nanoseconds");
  });
  return builder.finish_assert_one_array();
}

auto build_column(normalized_column const& column,
                  ::clickhouse::ColumnDateTime64 const& values,
                  value_path const& path, diagnostic_handler& dh) -> series {
  auto warnings = warning_state{};
  auto builder = series_builder{column.output_type};
  auto field = builder_ref{builder};
  for_each_present_row(column, field, [&](size_t row) {
    append_or_null(
      time_from_nanos(
        rescale_decimal_to_nanos(values.At(row), values.GetPrecision())),
      warnings, field, path, dh,
      "DateTime64 value is out of range after rescaling to nanoseconds");
  });
  return builder.finish_assert_one_array();
}

auto build_column(normalized_column const& column,
                  ::clickhouse::ColumnTime const& values,
                  value_path const& path, diagnostic_handler& dh) -> series {
  auto warnings = warning_state{};
  auto builder = series_builder{column.output_type};
  auto field = builder_ref{builder};
  for_each_present_row(column, field, [&](size_t row) {
    append_or_null(duration_from_clickhouse_time(int64_t{values.At(row)}),
                   warnings, field, path, dh,
                   "Time value is out of range after rescaling to "
                   "nanoseconds");
  });
  return builder.finish_assert_one_array();
}

auto build_column(normalized_column const& column,
                  ::clickhouse::ColumnTime64 const& values,
                  value_path const& path, diagnostic_handler& dh) -> series {
  auto warnings = warning_state{};
  auto builder = series_builder{column.output_type};
  auto field = builder_ref{builder};
  for_each_present_row(column, field, [&](size_t row) {
    append_or_null(
      duration_from_nanos(
        rescale_decimal_to_nanos(values.At(row), values.GetPrecision())),
      warnings, field, path, dh,
      "Time64 value is out of range after rescaling to nanoseconds");
  });
  return builder.finish_assert_one_array();
}

auto build_column(normalized_column const& column,
                  ::clickhouse::ColumnIPv4 const& values,
                  value_path const& path, diagnostic_handler& dh) -> series {
  auto warnings = warning_state{};
  auto builder = series_builder{column.output_type};
  auto field = builder_ref{builder};
  for_each_present_row(column, field, [&](size_t row) {
    append_or_null(parse_ip<in_addr>(values.AsString(row), AF_INET), warnings,
                   field, path, dh,
                   "expected valid IP data for ClickHouse type `{}`",
                   values.GetType().GetName());
  });
  return builder.finish_assert_one_array();
}

auto build_column(normalized_column const& column,
                  ::clickhouse::ColumnIPv6 const& values,
                  value_path const& path, diagnostic_handler& dh) -> series {
  auto warnings = warning_state{};
  auto builder = series_builder{column.output_type};
  auto field = builder_ref{builder};
  for_each_present_row(column, field, [&](size_t row) {
    append_or_null(parse_ip<in6_addr>(values.AsString(row), AF_INET6), warnings,
                   field, path, dh,
                   "expected valid IP data for ClickHouse type `{}`",
                   values.GetType().GetName());
  });
  return builder.finish_assert_one_array();
}

auto build_column(normalized_column const& column,
                  ::clickhouse::ColumnDecimal const& values, value_path const&,
                  diagnostic_handler&) -> series {
  auto builder = series_builder{column.output_type};
  auto field = builder_ref{builder};
  for_each_present_row(column, field, [&](size_t row) {
    field.data(
      format_scaled_integer(format_int128(values.At(row)), values.GetScale()));
  });
  return builder.finish_assert_one_array();
}

template <class Column>
auto build_column(normalized_column const& column, Column const& values,
                  value_path const& path, diagnostic_handler& dh) -> series {
  auto warnings = warning_state{};
  auto builder = series_builder{column.output_type};
  auto field = builder_ref{builder};
  null_entire_column_with_warning(column, warnings, field, path, dh,
                                  "unsupported ClickHouse runtime type `{}`",
                                  values.GetType().GetName());
  return builder.finish_assert_one_array();
}

auto build_blob_array_series(normalized_column const& column,
                             ::clickhouse::ColumnArray const& values,
                             value_path const& path, diagnostic_handler& dh)
  -> series {
  auto warnings = warning_state{};
  auto builder = series_builder{column.output_type};
  auto field = builder_ref{builder};
  auto item_type
    = values.GetType().template As<::clickhouse::ArrayType>()->GetItemType();
  auto child_type = unwrap_type(item_type);
  if (child_type.nullable or child_type.low_cardinality
      or child_type.type->GetCode() != ::clickhouse::Type::UInt8) {
    null_entire_column_with_warning(column, warnings, field, path, dh,
                                    "unexpected non-blob array in builder "
                                    "path");
    return builder.finish_assert_one_array();
  }
  for (auto row = size_t{0}; row < column.original->Size(); ++row) {
    if (row_is_null(column, row)) {
      field.null();
      continue;
    }
    auto nested = values.GetAsColumn(row);
    auto bytes = nested->template As<::clickhouse::ColumnUInt8>();
    if (not bytes) {
      null_with_warning(warnings, field, path, dh,
                        "expected Array(UInt8) data");
      continue;
    }
    auto value = blob{};
    value.reserve(bytes->Size());
    for (auto i = size_t{0}; i < bytes->Size(); ++i) {
      value.push_back(static_cast<std::byte>(bytes->At(i)));
    }
    field.data(std::move(value));
  }
  return builder.finish_assert_one_array();
}

auto build_array_series(normalized_column const& column,
                        ::clickhouse::ColumnArray const& values,
                        value_path const& path, diagnostic_handler& dh)
  -> Option<series> {
  auto const& list = as<list_type>(column.output_type);
  auto flat = build_series(values.GetData(), path.list(), dh);
  if (not flat) {
    return None{};
  }
  if (not flat->array) {
    // series_builder returns a default series{} when given a 0-row column
    // (i.e., all outer arrays are empty). Create a typed empty array instead.
    auto empty = check(arrow::MakeArrayOfNull(list.value_type().to_arrow_type(),
                                              0, arrow_memory_pool()));
    flat = series{list.value_type(), std::move(empty)};
  }
  TENZIR_ASSERT_EQ(flat->type, list.value_type());
  auto offsets = arrow::Int32Builder{arrow_memory_pool()};
  auto value_offset = int32_t{0};
  for (auto row = size_t{0}; row < values.Size(); ++row) {
    if (row_is_null(column, row)) {
      auto validity = uint8_t{0};
      check(offsets.AppendValues(&value_offset, 1, &validity));
      continue;
    }
    check(offsets.Append(value_offset));
    value_offset += detail::narrow<int32_t>(values.GetSize(row));
  }
  check(offsets.Append(value_offset));
  TENZIR_ASSERT(value_offset == flat->length());
  auto offset_array = std::shared_ptr<arrow::Int32Array>{};
  check(offsets.Finish(&offset_array));
  auto list_array = check(arrow::ListArray::FromArrays(
    *offset_array, *flat->array, arrow_memory_pool()));
  return series{column.output_type, std::move(list_array)};
}

auto build_low_cardinality_series(normalized_column const& column,
                                  value_path const& path,
                                  diagnostic_handler& dh) -> series {
  auto warnings = warning_state{};
  auto builder = series_builder{column.output_type};
  auto field = builder_ref{builder};
  auto code = column.logical_type.type->GetCode();
  switch (code) {
    case ::clickhouse::Type::Void:
      for_each_present_row(column, field, [&](size_t) {
        field.null();
      });
      break;
    case ::clickhouse::Type::Bool:
      for_each_present_row(column, field, [&](size_t row) {
        field.data(column.original->GetItem(row).get<bool>());
      });
      break;
    case ::clickhouse::Type::UInt8:
      for_each_present_row(column, field, [&](size_t row) {
        field.data(uint64_t{column.original->GetItem(row).get<uint8_t>()});
      });
      break;
    case ::clickhouse::Type::UInt16:
      for_each_present_row(column, field, [&](size_t row) {
        field.data(uint64_t{column.original->GetItem(row).get<uint16_t>()});
      });
      break;
    case ::clickhouse::Type::UInt32:
      for_each_present_row(column, field, [&](size_t row) {
        field.data(uint64_t{column.original->GetItem(row).get<uint32_t>()});
      });
      break;
    case ::clickhouse::Type::UInt64:
      for_each_present_row(column, field, [&](size_t row) {
        field.data(column.original->GetItem(row).get<uint64_t>());
      });
      break;
    case ::clickhouse::Type::Int8:
      for_each_present_row(column, field, [&](size_t row) {
        field.data(int64_t{column.original->GetItem(row).get<int8_t>()});
      });
      break;
    case ::clickhouse::Type::Int16:
      for_each_present_row(column, field, [&](size_t row) {
        field.data(int64_t{column.original->GetItem(row).get<int16_t>()});
      });
      break;
    case ::clickhouse::Type::Int32:
      for_each_present_row(column, field, [&](size_t row) {
        field.data(int64_t{column.original->GetItem(row).get<int32_t>()});
      });
      break;
    case ::clickhouse::Type::Int64:
      for_each_present_row(column, field, [&](size_t row) {
        field.data(column.original->GetItem(row).get<int64_t>());
      });
      break;
    case ::clickhouse::Type::Float32:
      for_each_present_row(column, field, [&](size_t row) {
        field.data(double{column.original->GetItem(row).get<float>()});
      });
      break;
    case ::clickhouse::Type::Float64:
      for_each_present_row(column, field, [&](size_t row) {
        field.data(column.original->GetItem(row).get<double>());
      });
      break;
    case ::clickhouse::Type::String:
    case ::clickhouse::Type::FixedString:
      for_each_present_row(column, field, [&](size_t row) {
        field.data(
          std::string{column.original->GetItem(row).get<std::string_view>()});
      });
      break;
    case ::clickhouse::Type::UUID:
      for_each_present_row(column, field, [&](size_t row) {
        field.data(format_uuid(column.original->GetItem(row).AsBinaryData()));
      });
      break;
    case ::clickhouse::Type::Enum8: {
      const auto* const enum_type
        = column.logical_type.type->As<::clickhouse::EnumType>();
      for_each_present_row(column, field, [&](size_t row) {
        field.data(std::string{
          enum_type->GetEnumName(column.original->GetItem(row).get<int8_t>())});
      });
      break;
    }
    case ::clickhouse::Type::Enum16: {
      const auto* const enum_type
        = column.logical_type.type->As<::clickhouse::EnumType>();
      for_each_present_row(column, field, [&](size_t row) {
        field.data(std::string{enum_type->GetEnumName(
          column.original->GetItem(row).get<int16_t>())});
      });
      break;
    }
    case ::clickhouse::Type::Int128:
      for_each_present_row(column, field, [&](size_t row) {
        field.data(format_int128(
          column.original->GetItem(row).get<::clickhouse::Int128>()));
      });
      break;
    case ::clickhouse::Type::UInt128:
      for_each_present_row(column, field, [&](size_t row) {
        field.data(format_uint128(
          column.original->GetItem(row).get<::clickhouse::UInt128>()));
      });
      break;
    case ::clickhouse::Type::Date:
      for_each_present_row(column, field, [&](size_t row) {
        append_or_null(
          time_from_unix_days(column.original->GetItem(row).get<uint16_t>()),
          warnings, field, path, dh,
          "Date value is out of range after rescaling to nanoseconds");
      });
      break;
    case ::clickhouse::Type::Date32:
      for_each_present_row(column, field, [&](size_t row) {
        append_or_null(
          time_from_unix_days(column.original->GetItem(row).get<int32_t>()),
          warnings, field, path, dh,
          "Date32 value is out of range after rescaling to nanoseconds");
      });
      break;
    case ::clickhouse::Type::DateTime:
      for_each_present_row(column, field, [&](size_t row) {
        append_or_null(
          time_from_unix_seconds(column.original->GetItem(row).get<uint32_t>()),
          warnings, field, path, dh,
          "DateTime value is out of range after rescaling to nanoseconds");
      });
      break;
    case ::clickhouse::Type::DateTime64: {
      auto precision
        = column.logical_type.type->As<::clickhouse::DateTime64Type>()
            ->GetPrecision();
      for_each_present_row(column, field, [&](size_t row) {
        append_or_null(
          time_from_nanos(rescale_decimal_to_nanos(
            column.original->GetItem(row).get<int64_t>(), precision)),
          warnings, field, path, dh,
          "DateTime64 value is out of range after rescaling to nanoseconds");
      });
      break;
    }
    case ::clickhouse::Type::Time:
      for_each_present_row(column, field, [&](size_t row) {
        append_or_null(duration_from_clickhouse_time(
                         column.original->GetItem(row).get<int32_t>()),
                       warnings, field, path, dh,
                       "Time value is out of range after rescaling to "
                       "nanoseconds");
      });
      break;
    case ::clickhouse::Type::Time64: {
      auto precision = column.logical_type.type->As<::clickhouse::Time64Type>()
                         ->GetPrecision();
      for_each_present_row(column, field, [&](size_t row) {
        append_or_null(
          duration_from_nanos(rescale_decimal_to_nanos(
            column.original->GetItem(row).get<int64_t>(), precision)),
          warnings, field, path, dh,
          "Time64 value is out of range after rescaling to nanoseconds");
      });
      break;
    }
    case ::clickhouse::Type::IPv4:
      for_each_present_row(column, field, [&](size_t row) {
        append_or_null(
          parse_ip_bytes<4UZ>(column.original->GetItem(row).AsBinaryData()),
          warnings, field, path, dh, "expected 4-byte IPv4 data");
      });
      break;
    case ::clickhouse::Type::IPv6:
      for_each_present_row(column, field, [&](size_t row) {
        append_or_null(
          parse_ip_bytes<16UZ>(column.original->GetItem(row).AsBinaryData()),
          warnings, field, path, dh, "expected 16-byte IPv6 data");
      });
      break;
    case ::clickhouse::Type::Decimal:
    case ::clickhouse::Type::Decimal32:
    case ::clickhouse::Type::Decimal64:
    case ::clickhouse::Type::Decimal128: {
      auto scale
        = column.logical_type.type->As<::clickhouse::DecimalType>()->GetScale();
      for_each_present_row(column, field, [&](size_t row) {
        append_or_null(decimal_from_bytes(
                         column.original->GetItem(row).AsBinaryData(), scale),
                       warnings, field, path, dh,
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
    case ::clickhouse::Type::JSON:
      null_entire_column_with_warning(column, warnings, field, path, dh,
                                      "unsupported LowCardinality ClickHouse "
                                      "type `{}`",
                                      column.logical_type.type->GetName());
      break;
  }
  return builder.finish_assert_one_array();
}

auto build_series(ConstColumnRef const& column, value_path path,
                  diagnostic_handler& dh) -> Option<series> {
  auto normalized = normalize_column(column, path, dh);
  if (not normalized) {
    emit_unsupported_column_warning(path, column->Type()->GetName(), dh);
    return None{};
  }
  return match(
    *normalized->effective,
    [&]<class Column>(Column const& values) -> Option<series> {
      if constexpr (std::same_as<Column, ::clickhouse::ColumnTuple>) {
        return build_tuple_series(*normalized, values, path, dh);
      } else if constexpr (std::same_as<Column, ::clickhouse::ColumnArray>) {
        if (is<list_type>(normalized->output_type)) {
          return build_array_series(*normalized, values, path, dh);
        }
        return build_blob_array_series(*normalized, values, path, dh);
      } else if constexpr (std::same_as<Column,
                                        ::clickhouse::ColumnLowCardinality>) {
        return build_low_cardinality_series(*normalized, path, dh);
      } else if constexpr (std::same_as<Column, ::clickhouse::ColumnNothing>) {
        auto builder = series_builder{normalized->output_type};
        auto field = builder_ref{builder};
        for (auto row = size_t{0}; row < normalized->original->Size(); ++row) {
          field.null();
        }
        return builder.finish_assert_one_array();
      } else if constexpr (std::same_as<Column, ::clickhouse::ColumnNullable>) {
        TENZIR_UNREACHABLE();
      } else {
        return build_column(*normalized, values, path, dh);
      }
    });
}

} // namespace

auto block_to_table_slice(::clickhouse::Block const& block,
                          std::string_view schema_name, diagnostic_handler& dh)
  -> Option<table_slice> {
  if (block.GetColumnCount() == 0 or block.GetRowCount() == 0) {
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

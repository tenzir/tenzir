//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/variant_traits.hpp"

#include <clickhouse/columns/array.h>
#include <clickhouse/columns/date.h>
#include <clickhouse/columns/decimal.h>
#include <clickhouse/columns/enum.h>
#include <clickhouse/columns/geo.h>
#include <clickhouse/columns/ip4.h>
#include <clickhouse/columns/ip6.h>
#include <clickhouse/columns/lowcardinality.h>
#include <clickhouse/columns/map.h>
#include <clickhouse/columns/nothing.h>
#include <clickhouse/columns/nullable.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/time.h>
#include <clickhouse/columns/tuple.h>
#include <clickhouse/columns/uuid.h>

namespace tenzir::plugins::clickhouse {

template <::clickhouse::Type::Code Code>
struct column_type_for_code;

template <>
struct column_type_for_code<::clickhouse::Type::Void> {
  using type = ::clickhouse::ColumnNothing;
};

template <>
struct column_type_for_code<::clickhouse::Type::Int8> {
  using type = ::clickhouse::ColumnInt8;
};

template <>
struct column_type_for_code<::clickhouse::Type::Int16> {
  using type = ::clickhouse::ColumnInt16;
};

template <>
struct column_type_for_code<::clickhouse::Type::Int32> {
  using type = ::clickhouse::ColumnInt32;
};

template <>
struct column_type_for_code<::clickhouse::Type::Int64> {
  using type = ::clickhouse::ColumnInt64;
};

template <>
struct column_type_for_code<::clickhouse::Type::UInt8> {
  using type = ::clickhouse::ColumnUInt8;
};

template <>
struct column_type_for_code<::clickhouse::Type::UInt16> {
  using type = ::clickhouse::ColumnUInt16;
};

template <>
struct column_type_for_code<::clickhouse::Type::UInt32> {
  using type = ::clickhouse::ColumnUInt32;
};

template <>
struct column_type_for_code<::clickhouse::Type::UInt64> {
  using type = ::clickhouse::ColumnUInt64;
};

template <>
struct column_type_for_code<::clickhouse::Type::Float32> {
  using type = ::clickhouse::ColumnFloat32;
};

template <>
struct column_type_for_code<::clickhouse::Type::Float64> {
  using type = ::clickhouse::ColumnFloat64;
};

template <>
struct column_type_for_code<::clickhouse::Type::String> {
  using type = ::clickhouse::ColumnString;
};

template <>
struct column_type_for_code<::clickhouse::Type::FixedString> {
  using type = ::clickhouse::ColumnFixedString;
};

template <>
struct column_type_for_code<::clickhouse::Type::DateTime> {
  using type = ::clickhouse::ColumnDateTime;
};

template <>
struct column_type_for_code<::clickhouse::Type::Date> {
  using type = ::clickhouse::ColumnDate;
};

template <>
struct column_type_for_code<::clickhouse::Type::Array> {
  using type = ::clickhouse::ColumnArray;
};

template <>
struct column_type_for_code<::clickhouse::Type::Nullable> {
  using type = ::clickhouse::ColumnNullable;
};

template <>
struct column_type_for_code<::clickhouse::Type::Tuple> {
  using type = ::clickhouse::ColumnTuple;
};

template <>
struct column_type_for_code<::clickhouse::Type::Enum8> {
  using type = ::clickhouse::ColumnEnum8;
};

template <>
struct column_type_for_code<::clickhouse::Type::Enum16> {
  using type = ::clickhouse::ColumnEnum16;
};

template <>
struct column_type_for_code<::clickhouse::Type::UUID> {
  using type = ::clickhouse::ColumnUUID;
};

template <>
struct column_type_for_code<::clickhouse::Type::IPv4> {
  using type = ::clickhouse::ColumnIPv4;
};

template <>
struct column_type_for_code<::clickhouse::Type::IPv6> {
  using type = ::clickhouse::ColumnIPv6;
};

template <>
struct column_type_for_code<::clickhouse::Type::Int128> {
  using type = ::clickhouse::ColumnInt128;
};

template <>
struct column_type_for_code<::clickhouse::Type::UInt128> {
  using type = ::clickhouse::ColumnUInt128;
};

template <>
struct column_type_for_code<::clickhouse::Type::Decimal> {
  using type = ::clickhouse::ColumnDecimal;
};

template <>
struct column_type_for_code<::clickhouse::Type::Decimal32> {
  using type = ::clickhouse::ColumnDecimal;
};

template <>
struct column_type_for_code<::clickhouse::Type::Decimal64> {
  using type = ::clickhouse::ColumnDecimal;
};

template <>
struct column_type_for_code<::clickhouse::Type::Decimal128> {
  using type = ::clickhouse::ColumnDecimal;
};

template <>
struct column_type_for_code<::clickhouse::Type::LowCardinality> {
  using type = ::clickhouse::ColumnLowCardinality;
};

template <>
struct column_type_for_code<::clickhouse::Type::DateTime64> {
  using type = ::clickhouse::ColumnDateTime64;
};

template <>
struct column_type_for_code<::clickhouse::Type::Date32> {
  using type = ::clickhouse::ColumnDate32;
};

template <>
struct column_type_for_code<::clickhouse::Type::Map> {
  using type = ::clickhouse::ColumnMap;
};

template <>
struct column_type_for_code<::clickhouse::Type::Point> {
  using type = ::clickhouse::ColumnPoint;
};

template <>
struct column_type_for_code<::clickhouse::Type::Ring> {
  using type = ::clickhouse::ColumnRing;
};

template <>
struct column_type_for_code<::clickhouse::Type::Polygon> {
  using type = ::clickhouse::ColumnPolygon;
};

template <>
struct column_type_for_code<::clickhouse::Type::MultiPolygon> {
  using type = ::clickhouse::ColumnMultiPolygon;
};

template <>
struct column_type_for_code<::clickhouse::Type::Time> {
  using type = ::clickhouse::ColumnTime;
};

template <>
struct column_type_for_code<::clickhouse::Type::Time64> {
  using type = ::clickhouse::ColumnTime64;
};

template <>
struct column_type_for_code<::clickhouse::Type::Bool> {
  using type = ::clickhouse::ColumnBool;
};

template <::clickhouse::Type::Code Code>
using column_type_for_code_t = typename column_type_for_code<Code>::type;

} // namespace tenzir::plugins::clickhouse

namespace tenzir {

template <>
class variant_traits<::clickhouse::Column> {
public:
  static constexpr auto count
    = static_cast<size_t>(::clickhouse::Type::Bool) + 1;

  static auto index(::clickhouse::Column const& x) -> size_t {
    auto code = x.GetType().GetCode();
    TENZIR_ASSERT(static_cast<size_t>(code) < count);
    return static_cast<size_t>(code);
  }

  template <size_t I>
  static auto get(::clickhouse::Column const& x) -> decltype(auto) {
    static_assert(I < count);
    constexpr auto code = static_cast<::clickhouse::Type::Code>(I);
    using type = plugins::clickhouse::column_type_for_code_t<code>;
    return static_cast<type const&>(x);
  }
};

static_assert(has_variant_traits<::clickhouse::Column>);

} // namespace tenzir

//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/flat_map.hpp"
#include "tenzir/detail/flat_set.hpp"
#include "tenzir/option.hpp"
#include "tenzir/variant.hpp"

#include <cstdint>
#include <string>
#include <vector>

/// The columns of a source, as filter pushdown sees them.
///
/// A source describes each column that a predicate may reference by the TQL
/// values that the source's reader decodes the stored values into. That is
/// what translation reasons about, which is why the same description serves
/// every source. Where a renderer cannot spell a literal of the column's type
/// from that alone, the description also carries a compatibility token that
/// the source defines, such as `TimeType::native_type`. Translation copies
/// such a token into the literals it emits and compares tokens for equality,
/// but never interprets them. Each source maps its own catalog metadata, such
/// as the output of `DESCRIBE`, onto this model.
namespace tenzir::pushdown {

/// A boolean column. TQL sees `bool`.
struct BoolType {};

/// A signed or unsigned integer of up to 64 bits. TQL sees `int64` or
/// `uint64`.
struct IntType {
  /// The width in bits, which bounds the result of arithmetic.
  uint8_t bits;
  bool is_signed;
};

/// A floating-point number. TQL sees `double`.
struct FloatType {};

/// A string. TQL sees `string`.
struct StringType {};

/// A string of `length` bytes. TQL sees all of them, including padding.
struct FixedStringType {
  size_t length;
};

/// An enumeration. TQL sees the element names.
struct EnumType {
  detail::flat_set<std::string> names;
};

/// A UUID. TQL sees the canonical lowercase text.
struct UuidType {};

/// A temporal column. TQL sees `time`.
///
/// The column stores a number of ticks of `unit` nanoseconds since the epoch,
/// and the reader decodes it to nanoseconds in an `int64`, which TQL compares.
/// Ticks outside `[lo, hi]` either do not exist in the column's type or
/// overflow the conversion, in which case TQL sees `null`.
struct TimeType {
  /// The source's name for the column's type, such as `DateTime64(3)` in
  /// ClickHouse. An opaque token that belongs to the plugin that builds the
  /// model: translation only compares it, to decide whether two columns share
  /// a type. The plugin's renderer spells a literal of exactly this type with
  /// it, so that the target never converts the column. The tick layout alone
  /// cannot serve, since it does not tell `Date` from `Date32`, for example.
  /// A renderer must veto a `TimeValue` whose token it does not know.
  std::string native_type;
  /// The number of nanoseconds per tick.
  int64_t unit;
  /// The range of ticks that TQL decodes to a value.
  int64_t lo;
  int64_t hi;
  /// Whether the type holds ticks below `lo` or above `hi`, which TQL decodes
  /// to `null`. Such rows need a guard where a comparison would otherwise see
  /// the stored value.
  bool guard_lo;
  bool guard_hi;

  friend auto operator==(TimeType const&, TimeType const&) -> bool = default;
};

/// An IP address. TQL sees `ip`.
struct IpType {
  /// Whether the column holds IPv4 addresses only. TQL sees them mapped into
  /// IPv6.
  bool v4;
};

/// The column types that filter pushdown compares.
using ColumnType
  = variant<BoolType, IntType, FloatType, StringType, FixedStringType, EnumType,
            UuidType, TimeType, IpType>;

/// A column together with the properties that decide how to compare it.
struct ColumnInfo {
  ColumnType type;
  /// Whether the target may yield `NULL` for the column. Equality on a
  /// nullable column needs a null guard, because TQL treats `null` as a value
  /// (`null == 1` is `false`, `null != 1` is `true`) while the target
  /// propagates it.
  bool nullable;
};

/// The columns of a source that predicates may reference.
class ColumnModel {
public:
  /// Makes the column at `path` addressable, unless it already is. A path with
  /// more than one segment names a field nested in a record column.
  auto add(std::vector<std::string> path, ColumnInfo column) -> void;

  /// Returns the column at `path`, or `None` if predicates on it must stay
  /// local.
  auto find(std::vector<std::string> const& path) const
    -> Option<ColumnInfo const&>;

private:
  detail::flat_map<std::vector<std::string>, ColumnInfo> columns_;
};

} // namespace tenzir::pushdown

//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/diagnostics.hpp"
#include "tenzir/option.hpp"
#include "tenzir/value_path.hpp"

#include <clickhouse/base/uuid.h>
#include <clickhouse/types/types.h>

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

/// The parts of decoding ClickHouse blocks that do not depend on the data
/// model being built: which types decode, into what, and the conversion of
/// individual values.
namespace tenzir::plugins::clickhouse {

/// The TQL type that a ClickHouse type decodes into.
enum class DecodedKind {
  null,
  bool_,
  int64,
  uint64,
  double_,
  string,
  time,
  duration,
  ip,
  blob,
  list,
  record,
};

struct DecodedType {
  DecodedKind kind;
  /// The element type of a list, or the field types of a record.
  std::vector<DecodedType> children;
  /// The field names of a record.
  std::vector<std::string> names;
};

/// Returns what a column of `type` decodes into, or `None` if it does not
/// decode. A list or record does not decode if any of its elements does not.
auto classify(::clickhouse::TypeRef const& type) -> Option<DecodedType>;

/// Returns whether a column of the ClickHouse type `type`, spelled as
/// `DESCRIBE TABLE` renders it, decodes. A column of a type that does not
/// decode is absent from the operator's output, as is a tuple with any element
/// of such a type.
auto is_decodable_type(std::string_view type) -> bool;

struct UnwrappedType {
  ::clickhouse::TypeRef type;
  bool nullable = false;
  bool low_cardinality = false;
};

/// Strips `Nullable` and `LowCardinality` from `type`.
auto unwrap_type(::clickhouse::TypeRef type) -> UnwrappedType;

/// Returns the field names of `type`, synthesizing `field0`, `field1`, ... for
/// unnamed elements.
auto tuple_field_names(::clickhouse::TupleType const& type)
  -> std::vector<std::string>;

auto format_uint128(::clickhouse::UInt128 value) -> std::string;
auto format_int128(::clickhouse::Int128 value) -> std::string;

/// Inserts a decimal point `scale` digits from the right of `digits`.
auto format_scaled_integer(std::string digits, size_t scale) -> std::string;

auto format_uuid(::clickhouse::UUID value) -> std::string;
auto format_uuid(std::string_view data) -> std::string;

/// Formats a decimal from its 4, 8, or 16 raw bytes.
auto decimal_from_bytes(std::string_view bytes, size_t scale)
  -> Option<std::string>;

/// Converts a value with `precision` fractional decimal digits into
/// nanoseconds. Returns `None` if the result does not fit.
auto rescale_decimal_to_nanos(int64_t value, size_t precision)
  -> Option<int64_t>;

auto seconds_to_nanos(int64_t seconds) -> Option<int64_t>;
auto days_to_nanos(int64_t days) -> Option<int64_t>;

auto emit_unsupported_column_warning(value_path path, std::string_view text,
                                     diagnostic_handler& dh) -> void;

auto emit_empty_block_warning(std::string_view schema_name,
                              diagnostic_handler& dh) -> void;

} // namespace tenzir::plugins::clickhouse

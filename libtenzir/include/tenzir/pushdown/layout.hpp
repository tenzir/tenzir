//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/diagnostics.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/option.hpp"

#include <algorithm>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace tenzir::pushdown {

/// The TQL type that a result column becomes, which each reader reaches from
/// what its target returns. `native` keeps what the reader infers, `integer`
/// is a count, `number` a number, `time` the target's own time, and `text` a
/// string without type inference. `exact` is an integer that the target may
/// widen beyond 64 bits, such as a sum of integers, which becomes `null` with
/// a warning if it does not fit.
enum class Decode { native, integer, number, time, exact, text };

/// A result column with how it decodes.
using Decoded = std::pair<std::string, Decode>;

/// Output fields, each with the result column that holds it.
using LayoutFields
  = std::vector<std::pair<std::vector<std::string>, std::string>>;

/// How the result columns of a query that a source sends become events.
struct Layout {
  /// The output fields with the columns that hold them, in order, or `None` if
  /// the result columns are the fields, as they are.
  Option<LayoutFields> fields;
  /// Result columns that the query needs internally, which are not fields.
  std::vector<std::string> hidden;
  /// The schema name of the output events, if the query changes it.
  Option<std::string> schema_name;
  /// The result columns that do not decode natively, and how they decode.
  std::vector<Decoded> decode = {};

  /// Returns how the result column `column` decodes.
  auto decode_of(std::string_view column) const -> Decode {
    auto it = std::ranges::find(decode, column, &Decoded::first);
    return it == decode.end() ? Decode::native : it->second;
  }
};

/// Arranges `events`, whose fields are the result columns of a query, as
/// `layout` describes. Output fields nest along their paths. A column that is
/// missing, as when a reader drops a column that it cannot decode, leaves its
/// field missing. Text in an `exact` column decodes as an integer, and text in
/// a `time` column as a time, where it reads as one.
auto apply_layout(nova::Events events, Layout const& layout,
                  diagnostic_handler& dh) -> nova::Events;

} // namespace tenzir::pushdown

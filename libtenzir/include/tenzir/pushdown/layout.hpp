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

#include <string>
#include <utility>
#include <vector>

namespace tenzir::pushdown {

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
};

/// Arranges `events`, whose fields are the result columns of a query, as
/// `layout` describes. Output fields nest along their paths. A column that is
/// missing, as when a reader drops a column that it cannot decode, leaves its
/// field missing.
auto apply_layout(nova::Events events, Layout const& layout,
                  diagnostic_handler& dh) -> nova::Events;

} // namespace tenzir::pushdown

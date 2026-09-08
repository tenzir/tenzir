//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/ir.hpp"
#include "tenzir/option.hpp"
#include "tenzir/table_slice.hpp"

#include <string>
#include <vector>

namespace tenzir {

/// Resolve conservative top-level column requirements for a reader, including
/// the columns referenced by pushed-down filter predicates.
/// Returns `None` when the reader must retain all columns, for example when
/// the projection references `this` or a filter contains a function call.
auto read_projection(Option<ir::OptimizeProjection> projection,
                     ir::OptimizeFilter const& filter)
  -> Option<std::vector<std::string>>;

/// Apply pushed-down filter predicates and then the remaining row limit.
/// The limit counts only events that survive every predicate, and `remaining`
/// is decremented by the number of rows returned.
auto apply_read_pushdown(table_slice slice, ir::OptimizeFilter const& filter,
                         Option<uint64_t>& remaining, diagnostic_handler& dh)
  -> table_slice;

} // namespace tenzir

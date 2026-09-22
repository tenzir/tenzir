//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/ir.hpp"
#include "tenzir/nova/eval.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/option.hpp"
#include "tenzir/table_slice.hpp"

#include <span>
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

/// Apply prepared predicates and the remaining limit by narrowing the active
/// mask. Columns and metadata stay intact; only surviving rows count.
auto apply_read_pushdown(nova::Events events,
                         std::span<nova::Evaluator> filters,
                         Option<uint64_t>& remaining, diagnostic_handler& dh)
  -> nova::Events;

} // namespace tenzir

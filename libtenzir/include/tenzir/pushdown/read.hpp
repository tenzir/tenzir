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

/// Application of optimizer hints inside a reader.
///
/// A reader that decodes events itself, such as `read_parquet`, has no target
/// to hand the optimizer's hints to. It applies them while reading instead: it
/// decodes only the columns that the projection and the filters reference,
/// and applies the filters and the limit to each batch before emitting it.
namespace tenzir::pushdown {

/// Resolve conservative top-level column requirements for a reader, including
/// the columns referenced by pushed-down filter predicates.
/// Returns `None` when the reader must retain all columns, for example when
/// the projection references `this` or a filter contains a function call.
auto read_projection(Option<ir::OptimizeProjection> projection,
                     ir::OptimizeFilter const& filter)
  -> Option<std::vector<std::string>>;

/// Like `read_projection`, but keeps complete field paths, for readers that
/// can decode the fields of records selectively.
auto read_projection_paths(Option<ir::OptimizeProjection> projection,
                           ir::OptimizeFilter const& filter)
  -> Option<ir::OptimizeProjection>;

/// Apply pushed-down filter predicates and then the remaining row limit.
/// The limit counts only events that survive every predicate, and `remaining`
/// is decremented by the number of rows returned.
auto apply_read(table_slice slice, ir::OptimizeFilter const& filter,
                Option<uint64_t>& remaining, diagnostic_handler& dh)
  -> table_slice;

/// Apply prepared predicates and the remaining limit by narrowing the active
/// mask. Columns and metadata stay intact; only surviving rows count.
auto apply_read(nova::Events events, std::span<nova::Evaluator> filters,
                Option<uint64_t>& remaining, diagnostic_handler& dh)
  -> nova::Events;

/// Drop the top-level fields that `projection` does not reference. A nested
/// path keeps its whole top-level field, and a path that refers to `this`
/// keeps every field. Rows, the active mask, and metadata stay intact.
/// Apply this after `apply_read`, since filters may reference fields that the
/// projection drops.
auto project(nova::Events events, ir::OptimizeProjection const& projection)
  -> nova::Events;

} // namespace tenzir::pushdown

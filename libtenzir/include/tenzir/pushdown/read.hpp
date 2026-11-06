//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/nova/eval.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/option.hpp"
#include "tenzir/table_slice.hpp"

#include <concepts>
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

/// A field that an expression reads.
struct RequiredField {
  /// The path of the field. An empty path stands for the whole event.
  ast::field_path path;
  /// Whether the expression may read any field below `path`, as through an
  /// index that is not a constant, such as `x[y]`. A source that can only
  /// request leaf fields cannot provide such a field.
  bool subtree = false;
};

/// Returns the fields that `expr` reads, in the order of first occurrence.
///
/// The left side of a named argument, as in `f(x, ignore_case=true)`, is a
/// label, not a field. The base of an index or field access that is not a
/// path itself, as in `xs[0]` or `f(x).y`, contributes the fields that it
/// reads. Function calls contribute the fields of their arguments only.
auto required_fields(ast::expression const& expr) -> std::vector<RequiredField>;

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
                Option<uint64_t>& remaining, base_ctx ctx) -> table_slice;

/// Apply prepared predicates and the remaining limit by narrowing the active
/// mask. Columns and metadata stay intact; only surviving rows count.
auto apply_read(nova::Events events, std::span<nova::Evaluator> filters,
                Option<uint64_t>& remaining, diagnostic_handler& dh)
  -> nova::Events;

/// Leaves out the first `count` active rows of `events`, and counts down
/// `count` by the rows that it left out. A restored read that runs its query
/// again uses this to skip the rows that it emitted before.
auto skip_rows(nova::Events events, uint64_t& count) -> nova::Events;

/// Drop the top-level fields that `projection` does not reference. A nested
/// path keeps its whole top-level field, and a path that refers to `this`
/// keeps every field. Rows, the active mask, and metadata stay intact.
/// Apply this after `apply_read`, since filters may reference fields that the
/// projection drops.
auto project(nova::Events events, ir::OptimizeProjection const& projection)
  -> nova::Events;

} // namespace tenzir::pushdown

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

/// Drop the top-level fields that `projection` does not reference. A nested
/// path keeps its whole top-level field, and a path that refers to `this`
/// keeps every field. Rows, the active mask, and metadata stay intact.
/// Apply this after `apply_read`, since filters may reference fields that the
/// projection drops.
auto project(nova::Events events, ir::OptimizeProjection const& projection)
  -> nova::Events;

/// The optimizer hints that a source applies to the events it reads itself.
///
/// A source that accepts a filter, a limit, or a projection from the optimizer
/// must enforce them, since the optimizer removed the operators downstream
/// that did. A source that pushes only part of them into its query evaluates
/// the rest of the filter, counts the limit, and projects with this class.
///
/// `Output` is the batch type of the source: `table_slice` or `nova::Events`.
template <class Output>
class LocalHints {
public:
  /// The prepared predicates: expressions for table slices, evaluators for
  /// events.
  using Filters
    = std::conditional_t<std::same_as<Output, table_slice>, ir::OptimizeFilter,
                         std::vector<nova::Evaluator>>;

  /// Applies `limit`, which counts events after the filter.
  explicit LocalHints(Option<uint64_t> limit = None{});

  /// Replaces the predicates to evaluate, in order. Returns `false` if one of
  /// them does not compile, after emitting the diagnostics. The caller must
  /// not emit events then, since the pipeline would drop them otherwise.
  auto prepare(ir::OptimizeFilter filter, OpCtx& ctx) -> Task<bool>;

  /// Replaces the predicates to evaluate with prepared ones.
  auto set_filters(Filters filters) -> void;

  /// Drops the top-level fields that `projection` does not reference from
  /// every batch after filtering.
  auto set_projection(Option<ir::OptimizeProjection> projection) -> void
    requires std::same_as<Output, nova::Events>;

  /// Applies the predicates, then the limit, then the projection.
  auto apply(Output output, base_ctx ctx) -> Output;

  /// The number of events that the limit still admits, or `None` without a
  /// limit.
  auto remaining() const -> Option<uint64_t>;

  /// Whether the limit admits no more events, so that the source can stop.
  auto exhausted() const -> bool;

  /// Saves and restores the remaining limit under the name `remaining`.
  auto snapshot(Serde& serde) -> void;

private:
  Filters filters_;
  Option<uint64_t> remaining_;
  Option<ir::OptimizeProjection> projection_;
};

extern template class LocalHints<table_slice>;
extern template class LocalHints<nova::Events>;

} // namespace tenzir::pushdown

//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/ir.hpp"
#include "tenzir/option.hpp"
#include "tenzir/pushdown/renderer.hpp"
#include "tenzir/pushdown/translate.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

/// Pushdown into a user's query in a piped query language, such as KQL or SPL.
///
/// A source that runs a user's query cannot rewrite it without parsing it. In a
/// piped language, it can append stages instead, which apply to the rows that
/// the query returns: a filter, a projection, and a limit. The source knows
/// neither the columns nor the types of the query's output, so the filter
/// holds prefilters only, and the source evaluates every predicate locally.
namespace tenzir::pushdown {

/// The spelling of trailing stages in a piped query language.
///
/// Every stage is spelled with its leading separator, such as `\n| where `, so
/// that the planner can append it to the query as is.
class PipeDialect {
public:
  virtual ~PipeDialect() = default;

  /// Returns whether a stage appended to `query` applies to the rows that
  /// `query` returns, without changing whether the query compiles.
  virtual auto accepts_trailing_stages(std::string_view query) const -> bool
    = 0;

  /// The renderer of the prefilters in the filter stage.
  virtual auto renderer() const -> Renderer const& = 0;

  /// Spells the stage that keeps the rows where every one of the rendered
  /// `predicates` holds. `predicates` is not empty.
  virtual auto filter_stage(std::span<std::string const> predicates) const
    -> std::string
    = 0;

  /// Spells the stage that keeps the top-level fields `names`, which are
  /// distinct and not empty, or returns `None` if one of them has no spelling.
  /// A field that the query does not return must not fail the query.
  virtual auto project_stage(std::span<std::string const> names) const
    -> Option<std::string>
    = 0;

  /// Spells the stage that keeps the first `n` rows, or returns `None` if the
  /// dialect cannot spell `n`. `n` is positive.
  virtual auto limit_stage(uint64_t n) const -> Option<std::string> = 0;

  /// The size budget of the stages that grow with the hints: the filter stage
  /// alone must fit into it, and so must all stages up to and including the
  /// projection stage. Stages that do not fit stay local, which only costs
  /// reduction, never correctness. The limit stages of a result cap and of a
  /// limit have a small bounded size and do not count against the budget.
  virtual auto max_appendix() const -> size_t = 0;
};

/// The query that a source with a piped query language sends.
struct PipePlan {
  /// The user's query, unchanged, followed by the appended stages.
  std::string query;
  /// Whether a filter stage with prefilters, preceded by a limit stage with
  /// any result cap, was appended.
  bool filtered = false;
  /// Whether a projection stage was appended.
  bool projected = false;
  /// Whether a limit stage for the limit was appended.
  bool limited = false;
};

/// Appends optimizer hints to a user's `query` as trailing stages of
/// `dialect`, in the order filter, projection, and limit, so that they apply
/// to the rows that `query` returns.
///
/// The filter stage holds the prefilters of `filter` that render, as many as
/// fit into the appendix. The source must still evaluate all of `filter`
/// locally.
///
/// `result_cap` is the number of rows of `query` after which the service
/// silently truncates the result. If set, a limit stage of that many rows
/// precedes the filter stage, so that the filter never reveals rows beyond the
/// cap. If the dialect cannot spell the cap, no filter stage is appended.
/// Pass `None` for a service that fails a truncated result instead.
///
/// The projection stage keeps the top-level fields that `projection` and
/// `filter` reference. The limit stage is appended only for an empty `filter`,
/// since every predicate stays local and would otherwise run after the limit,
/// and only for a positive limit that the dialect can spell. The source must
/// enforce `limit` locally in any case.
///
/// Returns `query` unchanged if the dialect does not accept trailing stages.
auto plan_trailing_stages(PipeDialect const& dialect, std::string_view query,
                          ir::OptimizeFilter const& filter,
                          Option<ir::OptimizeProjection> const& projection,
                          Option<uint64_t> limit, Option<uint64_t> result_cap,
                          registry const& reg) -> PipePlan;

/// Selects the `predicates` to push within a size budget, in order.
///
/// A predicate is selected if the spelling of the selection with it, as
/// measured by `size`, fits into `budget`. A predicate that does not fit is
/// skipped, so that a smaller later one may still fit, and stays local only.
auto select_within_budget(
  std::span<std::string const> predicates, size_t budget,
  std::function<auto(std::span<std::string const>)->size_t> const& size)
  -> std::vector<std::string>;

} // namespace tenzir::pushdown

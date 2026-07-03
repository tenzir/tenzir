//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/parallel_plan.hpp"

#include <cstddef>
#include <span>
#include <vector>

namespace tenzir {

/// The exchange required on the edge between two adjacent parallelism widths.
///
/// An edge connects an upstream width of `n` lanes to a downstream width of
/// `m` lanes. External sources and sinks count as width 1.
enum class edge_kind {
  /// `1 -> 1`: a plain channel, no exchange.
  direct,
  /// `1 -> n` (n > 1): a scatter fans one lane out to many.
  scatter,
  /// `n -> 1` (n > 1): a gather merges many lanes into one.
  gather,
  /// `n -> n` (n > 1): per-lane channels, no cross-lane exchange.
  parallel,
  /// `n -> m` (n, m > 1, n != m): a gather followed by a scatter (a "pinch").
  pinch,
};

/// Classifies the exchange required between an upstream and downstream width.
auto classify_edge(size_t upstream_degree, size_t downstream_degree)
  -> edge_kind;

/// The physical execution layout of a linear operator chain.
///
/// A `PhysicalPlan` partitions the chain's operators into consecutive stages,
/// each running at a fixed parallelism degree. The plan also classifies the
/// exchange on each inter-stage edge and on the edges to the external source
/// and sink (both of which are width 1).
///
/// The degenerate `flat` plan places every operator in a single serial stage;
/// it reproduces the pre-parallelization topology exactly.
class PhysicalPlan {
public:
  /// Builds the degenerate plan: one serial stage covering all operators.
  static auto flat(size_t num_operators) -> PhysicalPlan;

  /// Builds a plan from a planner result.
  ///
  /// The stages must be contiguous and cover `[0, num_operators)`.
  static auto from_plan(plan_result plan) -> PhysicalPlan;

  /// Returns the stages in chain order.
  auto stages() const -> std::span<const planned_stage> {
    return stages_;
  }

  /// Returns the number of operators covered by the plan.
  auto num_operators() const -> size_t {
    return num_operators_;
  }

  /// Returns the number of stages.
  auto num_stages() const -> size_t {
    return stages_.size();
  }

  /// Returns whether any stage runs at a degree greater than one.
  auto parallelized() const -> bool;

  /// Returns the largest degree across all stages.
  auto max_degree() const -> size_t;

  /// Returns the index of the stage containing operator `op`.
  auto stage_of(size_t op) const -> size_t;

  /// Returns the parallelism degree of `stage`.
  auto degree_of(size_t stage) const -> size_t {
    return stages_[stage].degree;
  }

  /// Returns the exchange on the input edge of `stage`.
  ///
  /// The upstream width is the preceding stage's degree, or 1 when `stage` is
  /// the first stage (fed by the external source).
  auto edge_into(size_t stage) const -> edge_kind;

  /// Returns the exchange on the output edge of `stage`.
  ///
  /// The downstream width is the following stage's degree, or 1 when `stage`
  /// is the last stage (feeding the external sink).
  auto edge_out_of(size_t stage) const -> edge_kind;

private:
  explicit PhysicalPlan(std::vector<planned_stage> stages, size_t num_operators)
    : stages_{std::move(stages)}, num_operators_{num_operators} {
  }

  std::vector<planned_stage> stages_;
  size_t num_operators_ = 0;
};

} // namespace tenzir

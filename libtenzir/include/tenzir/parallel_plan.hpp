//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/distribution.hpp"

#include <cstddef>
#include <span>
#include <vector>

namespace tenzir {

namespace ir {
struct pipeline;
} // namespace ir

/// Options controlling the implicit parallelization planner (exchange
/// insertion).
struct plan_options {
  /// Target parallelism degree for parallelizable stages, e.g.
  /// `hardware_concurrency`. A degree of 1 disables all widening.
  size_t degree = 1;

  /// Minimum number of operators a candidate region must contain to be worth
  /// parallelizing (cost heuristic). Regions smaller than this stay serial.
  size_t min_region_size = 1;

  /// Whether reordering events across the region is permitted (derived from
  /// `event_order`). When false, widening is disabled because a round-robin
  /// scatter would observably reorder the stream.
  bool allow_reordering = true;

  /// Whether checkpointing is enabled for the pipeline. When true the planner
  /// is a no-op guard: every stage stays at degree 1. Checkpointed pipelines
  /// lose implicit parallelism until the checkpointing epic lands.
  bool checkpointing = false;
};

/// A maximal run of operators `[begin, end)` sharing one parallelism degree.
///
/// Indices refer to the (non-transparent) operator sequence passed to the
/// planner. `degree == 1` denotes a serial stage.
struct planned_stage {
  size_t begin = 0;
  size_t end = 0;
  size_t degree = 1;
  /// The requirement satisfied by every operator within this stage.
  Distribution distribution = SingleDistribution{};
};

/// The planned stage layout for a pipeline.
struct plan_result {
  std::vector<planned_stage> stages;

  /// Returns whether the plan widens any stage beyond degree 1.
  auto parallelized() const -> bool;
};

/// Groups a sequence of per-operator distribution requirements into stages.
///
/// Consecutive operators requiring `AnyDistribution` are grouped into a single
/// parallelizable stage (subject to the gates in `opts`); consecutive
/// operators sharing the same `HashDistribution` key form a hash stage; every
/// other operator becomes a serial (degree-1) stage. Adjacent stages with the
/// same grouping class are merged.
///
/// The gates (`checkpointing`, `allow_reordering`, `min_region_size`,
/// `degree`) only ever *reduce* a stage's degree to 1; they never change the
/// grouping, so the layout is stable regardless of whether parallelization is
/// ultimately enabled.
auto plan_stages(std::span<const Distribution> requirements,
                 const plan_options& opts) -> plan_result;

/// Convenience overload extracting requirements from an IR pipeline.
///
/// Transparent operators (elided at spawn) are skipped: they contribute no
/// stage and never break a region. Stage indices therefore refer to the
/// pipeline's non-transparent operators in order.
auto plan_stages(const ir::pipeline& pipe, const plan_options& opts)
  -> plan_result;

} // namespace tenzir

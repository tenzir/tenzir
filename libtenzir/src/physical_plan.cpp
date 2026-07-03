//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/physical_plan.hpp"

#include "tenzir/detail/assert.hpp"

#include <algorithm>

namespace tenzir {

auto classify_edge(size_t upstream_degree, size_t downstream_degree)
  -> edge_kind {
  TENZIR_ASSERT(upstream_degree > 0);
  TENZIR_ASSERT(downstream_degree > 0);
  if (upstream_degree == 1 and downstream_degree == 1) {
    return edge_kind::direct;
  }
  if (upstream_degree == 1) {
    return edge_kind::scatter;
  }
  if (downstream_degree == 1) {
    return edge_kind::gather;
  }
  if (upstream_degree == downstream_degree) {
    return edge_kind::parallel;
  }
  return edge_kind::pinch;
}

auto PhysicalPlan::flat(size_t num_operators) -> PhysicalPlan {
  auto stages = std::vector<planned_stage>{};
  if (num_operators > 0) {
    stages.push_back({
      .begin = 0,
      .end = num_operators,
      .degree = 1,
      .distribution = SingleDistribution{},
    });
  }
  return PhysicalPlan{std::move(stages), num_operators};
}

auto PhysicalPlan::from_plan(plan_result plan) -> PhysicalPlan {
  auto num_operators = size_t{0};
  auto expected = size_t{0};
  for (const auto& stage : plan.stages) {
    // Stages must be contiguous and non-empty.
    TENZIR_ASSERT(stage.begin == expected);
    TENZIR_ASSERT(stage.end > stage.begin);
    TENZIR_ASSERT(stage.degree > 0);
    expected = stage.end;
    num_operators = stage.end;
  }
  return PhysicalPlan{std::move(plan.stages), num_operators};
}

auto PhysicalPlan::parallelized() const -> bool {
  return std::ranges::any_of(stages_, [](const planned_stage& stage) {
    return stage.degree > 1;
  });
}

auto PhysicalPlan::max_degree() const -> size_t {
  auto result = size_t{1};
  for (const auto& stage : stages_) {
    result = std::max(result, stage.degree);
  }
  return result;
}

auto PhysicalPlan::stage_of(size_t op) const -> size_t {
  TENZIR_ASSERT(op < num_operators_);
  for (auto i = size_t{0}; i < stages_.size(); ++i) {
    if (op >= stages_[i].begin and op < stages_[i].end) {
      return i;
    }
  }
  TENZIR_UNREACHABLE();
}

auto PhysicalPlan::edge_into(size_t stage) const -> edge_kind {
  TENZIR_ASSERT(stage < stages_.size());
  auto upstream = stage == 0 ? size_t{1} : stages_[stage - 1].degree;
  return classify_edge(upstream, stages_[stage].degree);
}

auto PhysicalPlan::edge_out_of(size_t stage) const -> edge_kind {
  TENZIR_ASSERT(stage < stages_.size());
  auto downstream
    = stage + 1 == stages_.size() ? size_t{1} : stages_[stage + 1].degree;
  return classify_edge(stages_[stage].degree, downstream);
}

} // namespace tenzir

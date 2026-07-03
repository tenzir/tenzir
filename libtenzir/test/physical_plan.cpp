//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/physical_plan.hpp"

#include "tenzir/test/test.hpp"

namespace tenzir {

namespace {

auto stage(size_t begin, size_t end, size_t degree,
           Distribution dist = SingleDistribution{}) -> planned_stage {
  return planned_stage{
    .begin = begin,
    .end = end,
    .degree = degree,
    .distribution = std::move(dist),
  };
}

auto plan_of(std::vector<planned_stage> stages) -> PhysicalPlan {
  return PhysicalPlan::from_plan(plan_result{.stages = std::move(stages)});
}

} // namespace

TEST("classify edge covers every width combination") {
  check(classify_edge(1, 1) == edge_kind::direct);
  check(classify_edge(1, 4) == edge_kind::scatter);
  check(classify_edge(4, 1) == edge_kind::gather);
  check(classify_edge(4, 4) == edge_kind::parallel);
  check(classify_edge(4, 8) == edge_kind::pinch);
  check(classify_edge(8, 4) == edge_kind::pinch);
}

TEST("flat plan is a single serial stage") {
  auto plan = PhysicalPlan::flat(3);
  check_eq(plan.num_operators(), size_t{3});
  check_eq(plan.num_stages(), size_t{1});
  check(not plan.parallelized());
  check_eq(plan.max_degree(), size_t{1});
  check_eq(plan.degree_of(0), size_t{1});
  // Every operator maps to the single stage.
  check_eq(plan.stage_of(0), size_t{0});
  check_eq(plan.stage_of(1), size_t{0});
  check_eq(plan.stage_of(2), size_t{0});
  // The external source and sink edges are plain channels.
  check(plan.edge_into(0) == edge_kind::direct);
  check(plan.edge_out_of(0) == edge_kind::direct);
}

TEST("flat plan with no operators has no stages") {
  auto plan = PhysicalPlan::flat(0);
  check_eq(plan.num_operators(), size_t{0});
  check_eq(plan.num_stages(), size_t{0});
  check(not plan.parallelized());
}

TEST("from_plan preserves stage layout") {
  auto plan = plan_of({
    stage(0, 1, 1),
    stage(1, 3, 4, AnyDistribution{}),
    stage(3, 4, 1),
  });
  check_eq(plan.num_operators(), size_t{4});
  check_eq(plan.num_stages(), size_t{3});
  check(plan.parallelized());
  check_eq(plan.max_degree(), size_t{4});
  check_eq(plan.degree_of(1), size_t{4});
  // Operator-to-stage mapping.
  check_eq(plan.stage_of(0), size_t{0});
  check_eq(plan.stage_of(1), size_t{1});
  check_eq(plan.stage_of(2), size_t{1});
  check_eq(plan.stage_of(3), size_t{2});
}

TEST("edges around a widened stage scatter and gather") {
  auto plan = plan_of({
    stage(0, 1, 1),
    stage(1, 2, 4, AnyDistribution{}),
    stage(2, 3, 1),
  });
  // Source -> serial -> wide -> serial -> sink.
  check(plan.edge_into(0) == edge_kind::direct);
  check(plan.edge_out_of(0) == edge_kind::scatter);
  check(plan.edge_into(1) == edge_kind::scatter);
  check(plan.edge_out_of(1) == edge_kind::gather);
  check(plan.edge_into(2) == edge_kind::gather);
  check(plan.edge_out_of(2) == edge_kind::direct);
}

TEST("adjacent stages of equal degree wire per lane") {
  auto plan = plan_of({
    stage(0, 1, 4, AnyDistribution{}),
    stage(1, 2, 4, HashDistribution{}),
  });
  // Source scatters into the first wide stage; the two wide stages of equal
  // degree wire per lane; the last stage gathers into the sink.
  check(plan.edge_into(0) == edge_kind::scatter);
  check(plan.edge_out_of(0) == edge_kind::parallel);
  check(plan.edge_into(1) == edge_kind::parallel);
  check(plan.edge_out_of(1) == edge_kind::gather);
}

TEST("adjacent stages of unequal degree pinch") {
  auto plan = plan_of({
    stage(0, 1, 4, AnyDistribution{}),
    stage(1, 2, 8, AnyDistribution{}),
  });
  check(plan.edge_out_of(0) == edge_kind::pinch);
  check(plan.edge_into(1) == edge_kind::pinch);
}

} // namespace tenzir

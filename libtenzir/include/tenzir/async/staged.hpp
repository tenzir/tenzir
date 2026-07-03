//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/exchange.hpp"
#include "tenzir/async/executor.hpp"
#include "tenzir/async/push_pull.hpp"
#include "tenzir/box.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/physical_plan.hpp"

#include <folly/coro/Collect.h>

#include <utility>
#include <vector>

namespace tenzir {

/// Data-plane orchestration for a staged pipeline.
///
/// `run_stages` composes a `PhysicalPlan` with the scatter/gather exchange
/// primitives to run a pipeline whose stages may execute at different
/// parallelism degrees. It wires every edge according to the plan and spawns
/// one lane task per lane per stage.
///
/// The caller supplies `spawn_lane`, a factory that instantiates a single lane
/// of a stage:
///
/// ```cpp
/// auto spawn_lane(size_t stage, size_t lane,
///                 Box<Pull<OperatorMsg<T>>> input,
///                 Box<Push<OperatorMsg<T>>> output) -> Task<void>;
/// ```
///
/// A lane task consumes `input` until it drains and drops `output` when done,
/// which propagates closure downstream. `make_channel` produces the internal
/// per-edge channels, e.g. `ExecCtx::make_channel<T>`.
///
/// This orchestrator handles the data plane only: it does not yet propagate
/// control-plane messages (`no_more_input`, `HardStop`, checkpoints) across
/// stages, and it assumes a single element type `T` across every edge. Both
/// limitations are lifted when this is wired into the executor's spawn path.
template <class T, class SpawnLane, class Factory>
auto run_stages(const PhysicalPlan& plan, Box<Pull<OperatorMsg<T>>> input,
                Box<Push<OperatorMsg<T>>> output, SpawnLane spawn_lane,
                Factory make_channel, PipeId id) -> Task<void> {
  using Push = Box<Push<OperatorMsg<T>>>;
  using Pull = Box<Pull<OperatorMsg<T>>>;
  const auto n = plan.num_stages();
  TENZIR_ASSERT(n > 0);
  auto tasks = std::vector<Task<void>>{};
  // The per-lane input pulls and output pushes for each stage. Edges populate
  // these; lanes consume them.
  auto stage_inputs = std::vector<std::vector<Pull>>(n);
  auto stage_outputs = std::vector<std::vector<Push>>(n);
  // Forwards a pull into a push until the pull drains, then drops the push.
  auto forward = [](Pull from, Push to) -> Task<void> {
    while (auto msg = co_await (*from)()) {
      co_await (*to)(std::move(*msg));
    }
  };
  // Builds an internal edge connecting `up` upstream lanes to `down`
  // downstream lanes, returning the upstream pushes and downstream pulls and
  // appending any merger/forwarder tasks.
  auto build_edge
    = [&](edge_kind kind, size_t up, size_t down, const ChannelId& edge_id)
    -> std::pair<std::vector<Push>, std::vector<Pull>> {
    auto pushes = std::vector<Push>{};
    auto pulls = std::vector<Pull>{};
    switch (kind) {
      case edge_kind::direct: {
        auto pair = make_channel(edge_id);
        pushes.push_back(std::move(pair.push));
        pulls.push_back(std::move(pair.pull));
        break;
      }
      case edge_kind::parallel: {
        TENZIR_ASSERT(up == down);
        for (auto lane = size_t{0}; lane < up; ++lane) {
          auto pair = make_channel(
            ChannelId{fmt::format("{}#lane/{}", edge_id.value, lane)});
          pushes.push_back(std::move(pair.push));
          pulls.push_back(std::move(pair.pull));
        }
        break;
      }
      case edge_kind::scatter: {
        TENZIR_ASSERT(up == 1);
        auto [scatter, lane_pulls]
          = make_scatter<T>(down, RoundRobinAdaptive{}, make_channel, edge_id);
        pushes.push_back(std::move(scatter));
        pulls = std::move(lane_pulls);
        break;
      }
      case edge_kind::gather: {
        TENZIR_ASSERT(down == 1);
        auto parts = make_gather<T>(up, make_channel, edge_id);
        pushes = std::move(parts.lanes);
        pulls.push_back(std::move(parts.pull));
        tasks.push_back(std::move(parts.merger));
        break;
      }
      case edge_kind::pinch: {
        // Merge the upstream lanes into one stream, then fan back out.
        auto parts = make_gather<T>(up, make_channel, edge_id);
        auto [scatter, lane_pulls]
          = make_scatter<T>(down, RoundRobinAdaptive{}, make_channel, edge_id);
        pushes = std::move(parts.lanes);
        pulls = std::move(lane_pulls);
        tasks.push_back(std::move(parts.merger));
        tasks.push_back(forward(std::move(parts.pull), std::move(scatter)));
        break;
      }
    }
    return {std::move(pushes), std::move(pulls)};
  };
  // Source edge: the external input (width 1) feeds stage 0.
  {
    auto d0 = plan.degree_of(0);
    if (plan.edge_into(0) == edge_kind::direct) {
      stage_inputs[0].push_back(std::move(input));
    } else {
      // Widening the source: scatter the external input across stage 0.
      auto [scatter, lane_pulls] = make_scatter<T>(
        d0, RoundRobinAdaptive{}, make_channel, ChannelId::first(id.op(0)));
      tasks.push_back(forward(std::move(input), std::move(scatter)));
      stage_inputs[0] = std::move(lane_pulls);
    }
  }
  // Internal edges connect consecutive stages.
  for (auto s = size_t{0}; s + 1 < n; ++s) {
    auto edge_id
      = id.op(plan.stages()[s].end - 1).to(id.op(plan.stages()[s + 1].begin));
    auto [pushes, pulls] = build_edge(plan.edge_out_of(s), plan.degree_of(s),
                                      plan.degree_of(s + 1), edge_id);
    stage_outputs[s] = std::move(pushes);
    stage_inputs[s + 1] = std::move(pulls);
  }
  // Sink edge: the last stage feeds the external output (width 1).
  {
    auto last = n - 1;
    auto dl = plan.degree_of(last);
    if (plan.edge_out_of(last) == edge_kind::direct) {
      stage_outputs[last].push_back(std::move(output));
    } else {
      // Narrowing to the sink: gather the last stage into the external output.
      auto parts = make_gather<T>(
        dl, make_channel, ChannelId::last(id.op(plan.num_operators() - 1)));
      stage_outputs[last] = std::move(parts.lanes);
      tasks.push_back(std::move(parts.merger));
      tasks.push_back(forward(std::move(parts.pull), std::move(output)));
    }
  }
  // Spawn one task per lane per stage.
  for (auto s = size_t{0}; s < n; ++s) {
    auto degree = plan.degree_of(s);
    TENZIR_ASSERT(stage_inputs[s].size() == degree);
    TENZIR_ASSERT(stage_outputs[s].size() == degree);
    for (auto lane = size_t{0}; lane < degree; ++lane) {
      tasks.push_back(spawn_lane(s, lane, std::move(stage_inputs[s][lane]),
                                 std::move(stage_outputs[s][lane])));
    }
  }
  co_await folly::coro::collectAllRange(std::move(tasks));
}

} // namespace tenzir

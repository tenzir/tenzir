//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/parallel_plan.hpp"

#include "tenzir/ir.hpp"

#include <algorithm>

namespace tenzir {

namespace {

/// Returns whether two requirements belong to the same grouping class, i.e.
/// whether adjacent operators with these requirements can share one stage.
auto same_group(const Distribution& lhs, const Distribution& rhs) -> bool {
  return match(
    lhs,
    [&](const AnyDistribution&) {
      return is<AnyDistribution>(rhs);
    },
    [&](const SingleDistribution&) {
      return is<SingleDistribution>(rhs);
    },
    [&](const HashDistribution& l) {
      const auto* r = try_as<HashDistribution>(rhs);
      return r != nullptr and same_hash_keys(l, *r);
    });
}

} // namespace

auto plan_result::parallelized() const -> bool {
  return std::ranges::any_of(stages, [](const planned_stage& stage) {
    return stage.degree > 1;
  });
}

auto plan_stages(std::span<const Distribution> requirements,
                 const plan_options& opts) -> plan_result {
  auto result = plan_result{};
  if (requirements.empty()) {
    return result;
  }
  // Group consecutive operators that share a grouping class into runs.
  auto run_begin = size_t{0};
  auto flush = [&](size_t run_end) {
    const auto& dist = requirements[run_begin];
    auto size = run_end - run_begin;
    // Only `Any` regions can widen in phase 1. Hash exchanges (phase 3) and
    // single-instance operators stay serial. Every gate can only pull the
    // degree back down to 1; it never changes the grouping.
    auto degree = size_t{1};
    if (is<AnyDistribution>(dist) and opts.degree > 1 and not opts.checkpointing
        and opts.allow_reordering and size >= opts.min_region_size) {
      degree = opts.degree;
    }
    result.stages.push_back({
      .begin = run_begin,
      .end = run_end,
      .degree = degree,
      .distribution = dist,
    });
    run_begin = run_end;
  };
  for (auto i = size_t{1}; i < requirements.size(); ++i) {
    if (not same_group(requirements[i - 1], requirements[i])) {
      flush(i);
    }
  }
  flush(requirements.size());
  return result;
}

auto plan_stages(const ir::pipeline& pipe, const plan_options& opts)
  -> plan_result {
  auto requirements = std::vector<Distribution>{};
  requirements.reserve(pipe.operators.size());
  for (const auto& op : pipe.operators) {
    // Transparent operators are elided at spawn: they contribute no stage and
    // must not break a region.
    if (op->is_transparent()) {
      continue;
    }
    requirements.push_back(op->required_distribution());
  }
  return plan_stages(requirements, opts);
}

} // namespace tenzir

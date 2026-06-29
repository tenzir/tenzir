//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>

namespace tenzir::plugins::optimize_barrier {

namespace {

struct OptimizeBarrierArgs {};

template <class T>
class OptimizeBarrier final : public Operator<T, T> {
public:
  explicit OptimizeBarrier(OptimizeBarrierArgs /*args*/) {
  }

  auto process(T input, Push<T>& push, OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(ctx);
    co_await push(std::move(input));
  }
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "optimize_barrier";
  }

  auto describe() const -> Description override {
    auto d = Describer<OptimizeBarrierArgs, OptimizeBarrier<table_slice>,
                       OptimizeBarrier<chunk_ptr>>{};
    // Act as an optimization barrier: no downstream optimization passes
    // upstream. The strictest ordering requirement is requested from upstream
    // and any downstream filter remains on the downstream side of the barrier.
    //
    // The operator must survive both optimization passes (exec.cpp pass and the
    // pass inside ir::pipeline::spawn()) so that filter_self where_ir nodes
    // produced in the first pass cannot leak upstream in the second pass. It is
    // therefore a physical pass-through at runtime, which has zero cost.
    return d.optimize([](DescribeCtx&, event_order,
                         ir::optimize_filter filter) -> Optimization {
      return {
        .order = event_order::ordered,
        .filter_self = std::move(filter),
      };
    });
  }
};

} // namespace

} // namespace tenzir::plugins::optimize_barrier

TENZIR_REGISTER_PLUGIN(tenzir::plugins::optimize_barrier::plugin)

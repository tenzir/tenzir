//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/operator_plugin.hpp>
#include <tenzir/panic.hpp>
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

  // This operator only exists in the IR and removes itself during optimization,
  // so it is never spawned.
  auto start(OpCtx&) -> Task<void> override {
    panic("optimize_barrier must be removed during optimization");
  }

  auto process(T, Push<T>&, OpCtx&) -> Task<void> override {
    panic("optimize_barrier must be removed during optimization");
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
    // and any downstream filter remains on the downstream side of the operator.
    // The operator only exists in the IR and removes itself before spawning.
    return d.optimize([](DescribeCtx&, event_order,
                         ir::optimize_filter filter) -> Optimization {
      return {
        .order = event_order::ordered,
        .filter_self = std::move(filter),
        .drop = true,
      };
    });
  }
};

} // namespace

} // namespace tenzir::plugins::optimize_barrier

TENZIR_REGISTER_PLUGIN(tenzir::plugins::optimize_barrier::plugin)

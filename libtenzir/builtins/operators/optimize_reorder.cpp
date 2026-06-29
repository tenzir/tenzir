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

namespace tenzir::plugins::optimize_reorder {

namespace {

struct OptimizeReorderArgs {};

template <class T>
class OptimizeReorder final : public Operator<T, T> {
public:
  explicit OptimizeReorder(OptimizeReorderArgs /*args*/) {
  }

  // This operator only exists in the IR and removes itself during optimization,
  // so it is never spawned.
  auto start(OpCtx&) -> Task<void> override {
    panic("optimize_reorder must be removed during optimization");
  }

  auto process(T, Push<T>&, OpCtx&) -> Task<void> override {
    panic("optimize_reorder must be removed during optimization");
  }
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "optimize_reorder";
  }

  auto describe() const -> Description override {
    auto d = Describer<OptimizeReorderArgs, OptimizeReorder<table_slice>,
                       OptimizeReorder<chunk_ptr>>{};
    // Tell upstream that it may produce events in any order, while remaining
    // a transparent pass-through for everything else: filters continue to flow
    // upstream and the operator removes itself.
    return d.optimize([](DescribeCtx&, event_order,
                         ir::optimize_filter filter) -> Optimization {
      return {
        .order = event_order::unordered,
        .filter_upstream = std::move(filter),
        .drop = true,
      };
    });
  }
};

} // namespace

} // namespace tenzir::plugins::optimize_reorder

TENZIR_REGISTER_PLUGIN(tenzir::plugins::optimize_reorder::plugin)

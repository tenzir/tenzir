//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/compile_ctx.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/panic.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tql2/ast.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <utility>

namespace tenzir::plugins::optimize_barrier {

namespace {

/// An optimization barrier that prevents downstream filters and ordering
/// relaxations from being pushed into upstream operators.
///
/// This is an IR-only operator: it has no runtime representation and removes
/// itself before the pipeline is spawned, so it adds no operator (and thus no
/// channel) to the running pipeline.
///
/// The barrier cannot simply drop itself on the first optimization pass like,
/// for example, `optimize_reorder` does. Order relaxation is sticky because
/// `weaker_event_order` keeps the most relaxed requirement seen on any pass, so
/// a one-shot hint survives. Tightening the order back to `ordered` is not
/// sticky: a later pass would relax it again. The execution path optimizes the
/// IR twice (once in `exec.cpp` and once inside `ir::pipeline::spawn()`), so
/// the barrier must stay present while it can still be re-optimized and only
/// remove itself on the pass that immediately precedes spawning.
class OptimizeBarrierIr final : public ir::Operator {
public:
  OptimizeBarrierIr() = default;

  explicit OptimizeBarrierIr(location loc) : loc_{loc} {
  }

  auto name() const -> std::string override {
    return "OptimizeBarrierIr";
  }

  auto main_location() const -> location override {
    return loc_;
  }

  auto substitute(substitute_ctx, bool) -> failure_or<void> override {
    return {};
  }

  auto infer_type(element_type_tag input, diagnostic_handler&) const
    -> failure_or<std::optional<element_type_tag>> override {
    return input;
  }

  auto optimize(ir::optimize_filter filter,
                event_order /*order*/) && -> ir::optimize_result override {
    // Pin the downstream filter right after the barrier and request the
    // strictest ordering from upstream. Keep the barrier itself until the pass
    // that precedes spawning, then drop it.
    auto replacement = std::vector<Box<ir::Operator>>{};
    const auto drop = std::exchange(sealed_, true);
    if (not drop) {
      replacement.push_back(std::move(*this).move());
    }
    for (auto& expr : filter) {
      replacement.push_back(make_where_ir(std::move(expr)));
    }
    return {
      ir::optimize_filter{},
      event_order::ordered,
      ir::pipeline{{}, std::move(replacement)},
    };
  }

  auto spawn(element_type_tag) && -> AnyOperator override {
    panic("optimize_barrier must be optimized away before spawning");
  }

  friend auto inspect(auto& f, OptimizeBarrierIr& x) -> bool {
    return f.object(x).fields(f.field("sealed", x.sealed_),
                              f.field("loc", x.loc_));
  }

private:
  bool sealed_ = false;
  location loc_;
};

class plugin final : public virtual operator_compiler_plugin {
public:
  auto name() const -> std::string override {
    return "optimize_barrier";
  }

  auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<ir::CompileResult> override {
    auto loc = inv.op.get_location();
    if (not inv.args.empty()) {
      diagnostic::error("`optimize_barrier` does not accept arguments")
        .primary(loc)
        .emit(ctx);
      return failure::promise();
    }
    return OptimizeBarrierIr{loc};
  }
};

} // namespace

} // namespace tenzir::plugins::optimize_barrier

TENZIR_REGISTER_PLUGIN(tenzir::plugins::optimize_barrier::plugin)
TENZIR_REGISTER_PLUGIN(
  tenzir::inspection_plugin<
    tenzir::ir::Operator, tenzir::plugins::optimize_barrier::OptimizeBarrierIr>)

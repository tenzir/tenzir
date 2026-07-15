//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/panic.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tql2/ast.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::optimize_reorder {

namespace {

/// Relaxes the ordering requirement for upstream operators, allowing them to
/// produce events in any order.
class OptimizeReorderIr final : public ir::Operator {
public:
  OptimizeReorderIr() = default;

  explicit OptimizeReorderIr(location loc) : loc_{loc} {
  }

  auto name() const -> std::string override {
    return "OptimizeReorderIr";
  }

  auto main_location() const -> location override {
    return loc_;
  }

  auto substitute(substitute_ctx, bool) -> failure_or<void> override {
    return {};
  }

  auto infer_type(element_type_tag input, diagnostic_handler&) const
    -> failure_or<element_type_tag> override {
    return input;
  }

  auto optimize(ir::optimize_filter filter,
                event_order /*order*/) && -> ir::optimize_result override {
    // Relax the upstream ordering requirement and forward the filters
    // unchanged.
    return {
      std::move(filter),
      event_order::unordered,
      ir::pipeline{{}, {}},
    };
  }

  auto spawn(element_type_tag) const -> AnyOperator override {
    panic("cannot spawn optimize_reorder; it must be removed during "
          "optimization");
  }

  friend auto inspect(auto& f, OptimizeReorderIr& x) -> bool {
    return f.object(x).fields(f.field("loc", x.loc_));
  }

private:
  location loc_;
};

class plugin final : public virtual operator_compiler_plugin {
public:
  auto name() const -> std::string override {
    return "optimize_reorder";
  }

  auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<ir::CompileResult> override {
    auto loc = inv.op.get_location();
    if (not inv.args.empty()) {
      diagnostic::error("`optimize_reorder` does not accept arguments")
        .primary(loc)
        .emit(ctx);
      return failure::promise();
    }
    return OptimizeReorderIr{loc};
  }
};

} // namespace

} // namespace tenzir::plugins::optimize_reorder

TENZIR_REGISTER_PLUGIN(tenzir::plugins::optimize_reorder::plugin)
TENZIR_REGISTER_PLUGIN(
  tenzir::inspection_plugin<
    tenzir::ir::Operator, tenzir::plugins::optimize_reorder::OptimizeReorderIr>)

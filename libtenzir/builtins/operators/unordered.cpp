//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/compile_ctx.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/panic.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tql2/ast.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/type.h>

namespace tenzir::plugins::unordered {

namespace {

class UnorderedIr final : public ir::Operator {
public:
  UnorderedIr() = default;

  explicit UnorderedIr(ir::pipeline pipeline, location loc)
    : pipeline_{std::move(pipeline)}, loc_{loc} {
  }

  auto name() const -> std::string override {
    return "UnorderedIr";
  }

  auto main_location() const -> location override {
    return loc_;
  }

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override {
    return pipeline_.substitute(ctx, instantiate);
  }

  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<element_type_tag> override {
    return pipeline_.infer_type(input, dh);
  }

  auto optimize(ir::OptimizeRequest req,
                const ir::OptimizeCtx& octx) && -> ir::OptimizeResult override {
    // Optimize each sub-operator individually, always passing unordered. Limit
    // and projection are threaded through like in `ir::pipeline::optimize`.
    auto replacement = ir::pipeline{std::move(pipeline_.lets), {}};
    for (auto& op : std::ranges::reverse_view(pipeline_.operators)) {
      req.order = EventOrder::unordered;
      auto opt = std::move(*op).optimize(std::move(req), octx);
      req = ir::OptimizeRequest{
        .filter = std::move(opt.filter),
        .order = opt.order,
        .limit = opt.limit,
        .projection = std::move(opt.projection),
      };
      replacement.operators.insert(
        replacement.operators.begin(),
        std::move_iterator{opt.replacement.operators.begin()},
        std::move_iterator{opt.replacement.operators.end()});
    }
    return {
      .filter = std::move(req.filter),
      .order = EventOrder::unordered,
      .replacement = std::move(replacement),
      .limit = req.limit,
      .projection = std::move(req.projection),
    };
  }

  auto spawn(element_type_tag) const -> AnyOperator override {
    panic("Cannot spawn unordered. It should have been optimized away.");
  }

  friend auto inspect(auto& f, UnorderedIr& x) -> bool {
    return f.object(x).fields(f.field("pipeline", x.pipeline_),
                              f.field("loc", x.loc_));
  }

private:
  ir::pipeline pipeline_;
  location loc_;
};

class plugin final : public virtual operator_compiler_plugin {
public:
  auto name() const -> std::string override {
    return "unordered";
  }

  auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<ir::CompileResult> override {
    auto loc = inv.op.get_location();
    if (inv.args.size() != 1) {
      diagnostic::error("`unordered` expects a single pipeline argument")
        .primary(loc)
        .emit(ctx);
      return failure::promise();
    }
    auto* pipe_expr = try_as<ast::pipeline_expr>(inv.args[0]);
    if (not pipe_expr) {
      diagnostic::error("`unordered` expects a pipeline argument `{{ … }}`")
        .primary(inv.args[0])
        .emit(ctx);
      return failure::promise();
    }
    TRY(auto pipe_ir, std::move(pipe_expr->inner).compile(ctx));
    return UnorderedIr{std::move(pipe_ir), loc};
  }
};

} // namespace

} // namespace tenzir::plugins::unordered

TENZIR_REGISTER_PLUGIN(tenzir::plugins::unordered::plugin)
TENZIR_REGISTER_PLUGIN(
  tenzir::inspection_plugin<tenzir::ir::Operator,
                            tenzir::plugins::unordered::UnorderedIr>)

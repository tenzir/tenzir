//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/compile_ctx.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/panic.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/tql2/plugin.hpp"

#include <iterator>
#include <ranges>

namespace tenzir::plugins::merge {

namespace {

struct MergeArgs {
  location keyword;
  location pipe_location;
  ir::pipeline pipe;

  friend auto inspect(auto& f, MergeArgs& x) -> bool {
    return f.object(x).fields(f.field("keyword", x.keyword),
                              f.field("pipe_location", x.pipe_location),
                              f.field("pipe", x.pipe));
  }
};

class MergeIr final : public ir::Operator {
public:
  MergeIr() = default;

  explicit MergeIr(MergeArgs args) : args_{std::move(args)} {
  }

  auto name() const -> std::string override {
    return "merge_ir";
  }

  auto copy() const -> Box<ir::Operator> override {
    return MergeIr{args_};
  }

  auto move() && -> Box<ir::Operator> override {
    return MergeIr{std::move(args_)};
  }

  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<element_type_tag> override {
    if (input.is_not<table_slice>()) {
      diagnostic::error("`merge` expects events as input")
        .primary(args_.keyword)
        .emit(dh);
      return failure::promise();
    }
    // The subpipeline is a source: it must start on its own and produce events.
    TRY(auto branch_ty, args_.pipe.infer_type(tag_v<void>, dh));
    if (branch_ty.is_not<table_slice>()) {
      diagnostic::error("`merge` subpipeline must be a source producing events")
        .primary(args_.pipe_location)
        .emit(dh);
      return failure::promise();
    }
    return tag_v<table_slice>;
  }

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override {
    return args_.pipe.substitute(ctx, instantiate);
  }

  auto
  optimize(ir::optimize_filter filter, event_order order,
           const ir::OptimizeCtx& octx) && -> ir::optimize_result override {
    // Push optimizations into both legs of merge: upstream and subpipeline
    auto opt = std::move(args_.pipe).optimize(filter, order, octx);
    args_.pipe = std::move(opt.replacement);
    args_.pipe.operators.insert_range(args_.pipe.operators.begin(),
                                      opt.filter
                                        | std::views::transform(make_where_ir));
    auto replacement = std::vector<Box<ir::Operator>>{};
    replacement.push_back(std::move(*this).move());
    return {
      std::move(filter),
      order,
      ir::pipeline{{}, std::move(replacement)},
    };
  }

  auto spawn(element_type_tag) const -> AnyOperator override {
    panic("cannot spawn merge; it must be lowered into the plan");
  }

  auto plan(ir::PlanBuilder& builder, ir::PlanPorts input,
            diagnostic_handler& dh) && -> failure_or<ir::PlanPorts> override {
    // Lower the source subpipeline with an empty input frontier so its head
    // stays a void-input source with no incoming channel. The planner picks it
    // up as an orphan source and broadcasts the external input's signals to it,
    // keeping it in lockstep with the main lane.
    TRY(auto tail,
        builder.lower_pipeline(std::move(args_.pipe), ir::PlanPorts{}, dh));
    // Return the main input lane together with the subpipeline's output so the
    // downstream consumer gathers them into a single stream.
    auto tails = std::move(input);
    tails.insert(tails.end(), std::make_move_iterator(tail.begin()),
                 std::make_move_iterator(tail.end()));
    return tails;
  }

  friend auto inspect(auto& f, MergeIr& x) -> bool {
    return f.apply(x.args_);
  }

private:
  MergeArgs args_;
};

class plugin final : public virtual operator_compiler_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.merge";
  }

  auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<ir::CompileResult> override {
    auto args = MergeArgs{};
    args.keyword = inv.op.get_location();
    if (inv.args.size() != 1) {
      diagnostic::error("`merge` expects exactly one pipeline argument")
        .primary(args.keyword)
        .hint("use `merge { … }`")
        .emit(ctx);
      return failure::promise();
    }
    auto* pipe_expr = try_as<ast::pipeline_expr>(inv.args.front());
    if (not pipe_expr) {
      diagnostic::error("`merge` expects a pipeline argument `{{ … }}`")
        .primary(inv.args.front())
        .emit(ctx);
      return failure::promise();
    }
    args.pipe_location = pipe_expr->get_location();
    TRY(auto pipe_ir, std::move(pipe_expr->inner).compile(ctx));
    args.pipe = std::move(pipe_ir);
    return MergeIr{std::move(args)};
  }
};

using merge_ir_plugin = inspection_plugin<ir::Operator, MergeIr>;

} // namespace

} // namespace tenzir::plugins::merge

TENZIR_REGISTER_PLUGIN(tenzir::plugins::merge::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::merge::merge_ir_plugin)

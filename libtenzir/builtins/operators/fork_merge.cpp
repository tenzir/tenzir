//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async.hpp"
#include "tenzir/compile_ctx.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/tql2/plugin.hpp"

#include <iterator>
#include <ranges>

namespace tenzir::plugins::fork_merge {

namespace {

struct ForkMergeArgs {
  location keyword;
  std::vector<location> locations;
  std::vector<ir::pipeline> branches;

  friend auto inspect(auto& f, ForkMergeArgs& x) -> bool {
    return f.object(x).fields(f.field("keyword", x.keyword),
                              f.field("locations", x.locations),
                              f.field("branches", x.branches));
  }
};

class ForkMergeIr final : public ir::Operator {
public:
  ForkMergeIr() = default;

  explicit ForkMergeIr(ForkMergeArgs args) : args_{std::move(args)} {
  }

  auto name() const -> std::string override {
    return "fork_merge_ir";
  }

  auto copy() const -> Box<ir::Operator> override {
    return ForkMergeIr{args_};
  }

  auto move() && -> Box<ir::Operator> override {
    return ForkMergeIr{std::move(args_)};
  }

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override {
    for (auto& branch : args_.branches) {
      TRY(branch.substitute(ctx, instantiate));
    }
    return {};
  }

  auto optimize(ir::optimize_filter filter,
                event_order order) && -> ir::optimize_result override {
    // Each branch receives the same input, and their outputs are merged. A
    // downstream filter over the merged output equals the union of that filter
    // applied to each branch, so we can push it into every branch. The residual
    // filter that a branch would push to its upstream is reinserted at the
    // front of that branch, because all branches share a single upstream and
    // cannot push differing filters into it.
    auto optimize_branch
      = [&](ir::pipeline& branch, ir::optimize_filter f) -> event_order {
      auto opt = std::move(branch).optimize(std::move(f), order);
      branch = std::move(opt.replacement);
      branch.operators.insert_range(branch.operators.begin(),
                                    opt.filter
                                      | std::views::transform(make_where_ir));
      return opt.order;
    };
    auto result_order = order;
    for (auto i = size_t{0}; i < args_.branches.size(); ++i) {
      auto branch_order = optimize_branch(args_.branches[i], filter);
      result_order = i == 0 ? branch_order
                            : stronger_event_order(result_order, branch_order);
    }
    auto replacement = std::vector<Box<ir::Operator>>{};
    replacement.push_back(std::move(*this).move());
    return {
      {},
      result_order,
      ir::pipeline{{}, std::move(replacement)},
    };
  }

  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<element_type_tag> override {
    if (input.is_not<table_slice>()) {
      diagnostic::error("`fork_merge` expects events as input")
        .primary(args_.keyword)
        .emit(dh);
      return failure::promise();
    }
    for (auto i = size_t{0}; i < args_.branches.size(); ++i) {
      TRY(auto branch_ty, args_.branches[i].infer_type(input, dh));
      if (branch_ty.is_not<table_slice>()) {
        diagnostic::error("`fork_merge` subpipelines must produce events")
          .primary(args_.locations[i])
          .emit(dh);
        return failure::promise();
      }
    }
    return tag_v<table_slice>;
  }

  auto spawn(element_type_tag) const -> AnyOperator override {
    TENZIR_UNREACHABLE();
  }

  auto plan(ir::PlanBuilder& builder, ir::PlanPorts input,
            diagnostic_handler& dh) && -> failure_or<ir::PlanPorts> override {
    // Broadcast the input to every branch and gather their outputs: wire the
    // (single) upstream to each branch head via a `Broadcast` channel, then
    // return the branch tails so the consumer merges them.
    auto src = builder.into_single(input);
    auto heads = std::vector<size_t>{};
    heads.reserve(args_.branches.size());
    auto tails = ir::PlanPorts{};
    for (auto& branch : args_.branches) {
      // A detached identity head gives the branch a single entry node to
      // broadcast into (and the identity for an empty branch).
      auto ty = tag_v<table_slice>;
      auto head = builder.add_identity(ty);
      heads.push_back(head);
      TRY(auto tail,
          builder.lower_pipeline(std::move(branch),
                                 ir::PlanPorts{ir::PlanPort{head, ty}}, dh));
      tails.insert(tails.end(), std::make_move_iterator(tail.begin()),
                   std::make_move_iterator(tail.end()));
    }
    builder.broadcast(src, heads);
    return tails;
  }

  friend auto inspect(auto& f, ForkMergeIr& x) -> bool {
    return f.apply(x.args_);
  }

private:
  ForkMergeArgs args_;
};

class plugin final : public virtual operator_compiler_plugin {
public:
  auto name() const -> std::string override {
    return "fork_merge";
  }

  auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<ir::CompileResult> override {
    auto args = ForkMergeArgs{};
    args.keyword = inv.op.get_location();
    if (inv.args.empty()) {
      diagnostic::error("`fork_merge` expects at least one pipeline argument")
        .primary(args.keyword)
        .hint("provide branches as `fork_merge { … }, { … }`")
        .emit(ctx);
      return failure::promise();
    }
    for (auto& arg : inv.args) {
      auto* pipe_expr = try_as<ast::pipeline_expr>(arg);
      if (not pipe_expr) {
        diagnostic::error("`fork_merge` expects pipeline arguments `{{ … }}`")
          .primary(arg)
          .emit(ctx);
        return failure::promise();
      }
      args.locations.push_back(pipe_expr->get_location());
      TRY(auto pipe_ir, std::move(pipe_expr->inner).compile(ctx));
      args.branches.push_back(std::move(pipe_ir));
    }
    return ForkMergeIr{std::move(args)};
  }
};

using fork_merge_ir_plugin = inspection_plugin<ir::Operator, ForkMergeIr>;

} // namespace

} // namespace tenzir::plugins::fork_merge

TENZIR_REGISTER_PLUGIN(tenzir::plugins::fork_merge::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::fork_merge::fork_merge_ir_plugin)

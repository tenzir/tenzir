//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async.hpp"
#include "tenzir/compile_ctx.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/tql2/plugin.hpp"

#include <algorithm>
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

/// Broadcasts each input slice to all branches. The outputs of the branch
/// subpipelines are merged into the operator's output by the executor.
///
/// Every branch is an events-to-events transformation.
class ForkMerge final : public Operator<table_slice, table_slice> {
public:
  explicit ForkMerge(ForkMergeArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    for (auto i = size_t{0}; i < args_.branches.size(); ++i) {
      if (args_.branches[i].operators.empty()
          or ctx.get_sub(static_cast<int64_t>(i)).is_some()) {
        continue;
      }
      co_await ctx.spawn_sub<table_slice>(static_cast<int64_t>(i),
                                          args_.branches[i]);
    }
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    for (auto i = size_t{0}; i < args_.branches.size(); ++i) {
      if (closed_[i]) {
        continue;
      }
      auto sub = ctx.get_sub(static_cast<int64_t>(i));
      if (not sub) {
        // An empty branch acts as the identity, forwarding its input straight
        // to the merged output.
        if (args_.branches[i].operators.empty()) {
          co_await push(input);
        } else {
          closed_[i] = true;
        }
        continue;
      }
      auto& handle = as<SubHandle<table_slice>>(*sub);
      closed_[i] = (co_await handle.push(input)).is_err();
    }
  }

  auto finish_sub(SubKeyView key, Push<table_slice>&, OpCtx&)
    -> Task<void> override {
    auto key_data = materialize(key);
    auto* index = try_as<int64_t>(key_data);
    TENZIR_ASSERT(index);
    closed_[detail::narrow<size_t>(*index)] = true;
    co_return;
  }

  auto state() -> OperatorState override {
    return (not closed_.empty()
            and std::ranges::all_of(closed_, std::identity{}))
             ? OperatorState::done
             : OperatorState::normal;
  }

private:
  ForkMergeArgs args_;
  // A branch is considered closed once its subpipeline terminated. Empty
  // branches never close on their own; they drain input until end of stream.
  std::vector<bool> closed_ = std::vector<bool>(args_.branches.size(), false);
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
    -> failure_or<std::optional<element_type_tag>> override {
    if (input.is_not<table_slice>()) {
      diagnostic::error("`fork_merge` expects events as input")
        .primary(args_.keyword)
        .emit(dh);
      return failure::promise();
    }
    for (auto i = size_t{0}; i < args_.branches.size(); ++i) {
      TRY(auto branch_ty, args_.branches[i].infer_type(input, dh));
      if (not branch_ty) {
        continue;
      }
      if (branch_ty->is_not<table_slice>()) {
        diagnostic::error("`fork_merge` subpipelines must produce events")
          .primary(args_.locations[i])
          .emit(dh);
        return failure::promise();
      }
    }
    return tag_v<table_slice>;
  }

  auto spawn(element_type_tag input) && -> Option<AnyOperator> override {
    TENZIR_ASSERT(input.is<table_slice>());
    return ForkMerge{std::move(args_)}.with_name("fork_merge");
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

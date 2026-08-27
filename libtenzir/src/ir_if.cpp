//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/ir_if.hpp"

#include "tenzir/async.hpp"
#include "tenzir/compile_ctx.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/multi_series.hpp"
#include "tenzir/option.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/tql2/eval.hpp"

#include <ranges>
#include <utility>

namespace tenzir {

namespace {

struct IfArgs {
  ast::expression condition;
  ir::pipeline consequence;
  Option<ir::pipeline> alternative;

  friend auto inspect(auto& f, IfArgs& x) -> bool {
    return f.object(x).fields(f.field("condition", x.condition),
                              f.field("consequence", x.consequence),
                              f.field("alternative", x.alternative));
  }
};

/// The maximum number of contiguous runs for which `IfOp` keeps the zero-copy
/// sub-slice path instead of materializing one slice per branch. Conditions
/// that correlate with the input order, such as a predicate on a sorted or
/// per-schema field, stay well below this; interleaved ones exceed it after a
/// short prefix and fall back to copying.
constexpr auto max_partition_runs = size_t{4};

/// Runtime operator for `if`: evaluates the condition per slice and routes
/// `true` rows to port 0 (consequence) and `false`/`null` rows to port 1
/// (alternative). A non-boolean condition routes the whole subslice to the
/// alternative and emits a diagnostic.
class IfOp final : public Operator<table_slice, table_slice, true> {
public:
  explicit IfOp(ast::expression condition) : condition_{std::move(condition)} {
  }

  auto process(table_slice input, PushPorts<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    auto& dh = ctx.dh();
    auto end = int64_t{0};
    for (const auto& [predicate] :
         split_multi_series(eval(condition_, input, dh))) {
      const auto start = std::exchange(end, end + predicate.length());
      TENZIR_ASSERT(end > start);
      const auto sliced = subslice(input, start, end);
      const auto typed = predicate.as<bool_type>();
      if (not typed) {
        if (not is<null_type>(predicate.type)) {
          diagnostic::warning("expected `bool`, but got `{}`",
                              predicate.type.kind())
            .primary(condition_)
            .emit(dh);
        }
        co_await push(1, sliced);
        continue;
      }
      // A condition that is clustered rather than interleaved lets both
      // branches be served by zero-copy sub-slices, at the price of a few more
      // messages than the two that `partition` produces.
      if (auto runs
          = partition_runs(sliced, *typed->array, max_partition_runs)) {
        for (auto& [selected, part] : *runs) {
          co_await push(selected ? 0 : 1, std::move(part));
        }
        continue;
      }
      // `partition` sends `true` rows to the first slice and `false`/`null`
      // rows to the second.
      auto [then_slice, else_slice] = partition(sliced, *typed->array);
      TENZIR_ASSERT(then_slice.rows() + else_slice.rows() == sliced.rows());
      if (then_slice.rows() > 0) {
        co_await push(0, std::move(then_slice));
      }
      if (else_slice.rows() > 0) {
        co_await push(1, std::move(else_slice));
      }
    }
  }

private:
  ast::expression condition_;
};

class IfIr final : public ir::Operator {
public:
  IfIr() = default;

  explicit IfIr(IfArgs args) : args_{std::move(args)} {
  }

  auto name() const -> std::string override {
    return "If";
  }

  auto copy() const -> Box<ir::Operator> override {
    return IfIr{args_};
  }

  auto move() && -> Box<ir::Operator> override {
    return IfIr{std::move(args_)};
  }

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override {
    TRY(args_.condition.substitute(ctx));
    TRY(args_.consequence.substitute(ctx, instantiate));
    if (args_.alternative) {
      TRY(args_.alternative->substitute(ctx, instantiate));
    }
    return {};
  }

  auto
  optimize(ir::optimize_filter filter, event_order order,
           const ir::OptimizeCtx& octx) && -> ir::optimize_result override {
    // A branch that does not return events has no downstream event consumer,
    // so it inherits neither the downstream filter nor its order requirement.
    auto null_dh = null_diagnostic_handler{};
    auto optimize_branch
      = [&](ir::pipeline& branch, const ir::optimize_filter& f) -> event_order {
      auto ty = branch.infer_type(tag_v<table_slice>, null_dh);
      auto events = ty and ty->is<table_slice>();
      auto opt
        = std::move(branch).optimize(events ? f : ir::optimize_filter{},
                                     events ? order : event_order::ordered,
                                     octx);
      branch = std::move(opt.replacement);
      branch.prepend(std::move(opt.filter));
      return opt.order;
    };
    // Handle downstream filters when there is no explicit `else` branch.
    if (not args_.alternative and not filter.empty()) {
      args_.alternative.emplace(ir::pipeline{});
    }
    auto cons_order = optimize_branch(args_.consequence, filter);
    auto alt_order
      = args_.alternative ? optimize_branch(*args_.alternative, filter) : order;
    auto replacement = std::vector<Box<ir::Operator>>{};
    replacement.push_back(std::move(*this).move());
    return {
      {},
      stronger_event_order(cons_order, alt_order),
      ir::pipeline{{}, std::move(replacement)},
    };
  }

  auto spawn(element_type_tag) const -> AnyOperator override {
    return Box<tenzir::Operator<table_slice, table_slice, true>>{
      IfOp{args_.condition}.with_name("if")};
  }

  auto parallelizable() const -> bool override {
    return true;
  }

  auto plan(ir::PlanBuilder& builder, ir::PlanPorts input,
            diagnostic_handler& dh) && -> failure_or<ir::PlanPorts> override {
    // `if` is a two-output operator: it evaluates the condition per row and
    // routes each row to exactly one branch (port 0 = consequence for `true`
    // rows, port 1 = alternative for `false`/`null` rows). Without an explicit
    // `else`, the alternative branch is empty and forwards unmatched rows
    // unchanged. Both branch tails are returned so the consumer merges them.
    auto ty = tag_v<table_slice>;
    auto consequence = std::move(args_.consequence);
    auto alternative
      = args_.alternative ? std::move(*args_.alternative) : ir::pipeline{};
    auto node = builder.append_node(std::move(*this).move(), ty, ty);
    builder.add_channels(input, node);
    TRY(auto tails,
        builder.lower_subpipeline(node, std::move(consequence),
                                  ir::PlanPorts{ir::Port{node, 0, ty}}, dh));
    TRY(auto tail_alt,
        builder.lower_subpipeline(node, std::move(alternative),
                                  ir::PlanPorts{ir::Port{node, 1, ty}}, dh));
    tails.insert(tails.end(), tail_alt.begin(), tail_alt.end());
    return tails;
  }

  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<element_type_tag> override {
    // A branch may be empty (or contain only `let`s), in which case it has no
    // operator to point at. Fall back to the condition's location, which is
    // always present.
    auto branch_location = [&](const ir::pipeline& branch) -> location {
      if (not branch.operators.empty()) {
        return branch.operators.back()->main_location();
      }
      return args_.condition.get_location();
    };
    TRY(auto then_ty, args_.consequence.infer_type(input, dh));
    auto else_ty = input;
    if (args_.alternative) {
      TRY(else_ty, args_.alternative->infer_type(input, dh));
    }
    if (then_ty.is<chunk_ptr>()) {
      diagnostic::error("branches must not return bytes")
        .primary(branch_location(args_.consequence))
        .emit(dh);
      return failure::promise();
    }
    if (args_.alternative and else_ty.is<chunk_ptr>()) {
      diagnostic::error("branches must not return bytes")
        .primary(branch_location(*args_.alternative))
        .emit(dh);
      return failure::promise();
    }
    if (then_ty == else_ty) {
      return then_ty;
    }
    if (then_ty.is<void>()) {
      return else_ty;
    }
    if (else_ty.is<void>()) {
      return then_ty;
    }
    // TODO: Improve diagnostic.
    auto diag = diagnostic::error("incompatible branch output types: {} and {}",
                                  operator_type_name(then_ty),
                                  operator_type_name(else_ty))
                  .primary(branch_location(args_.consequence));
    if (args_.alternative) {
      diag = std::move(diag).secondary(branch_location(*args_.alternative));
    }
    std::move(diag).emit(dh);
    return failure::promise();
  }

  friend auto inspect(auto& f, IfIr& x) -> bool {
    return f.apply(x.args_);
  }

private:
  IfArgs args_;
};

TENZIR_REGISTER_PLUGIN(inspection_plugin<ir::Operator, IfIr>)

} // namespace

auto make_if_ir(ast::if_stmt x, compile_ctx& ctx)
  -> failure_or<Box<ir::Operator>> {
  TRY(x.condition.bind(ctx));
  TRY(auto then, std::move(x.then).compile(ctx));
  auto args = IfArgs{};
  args.condition = std::move(x.condition);
  args.consequence = std::move(then);
  if (x.else_) {
    TRY(auto pipe, std::move(x.else_->pipe).compile(ctx));
    args.alternative.emplace(std::move(pipe));
  }
  return Box<ir::Operator>{IfIr{std::move(args)}};
}

} // namespace tenzir

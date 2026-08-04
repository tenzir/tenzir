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
#include "tenzir/option.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/rebatch.hpp"
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
  std::optional<ir::pipeline> alternative;

  friend auto inspect(auto& f, IfArgs& x) -> bool {
    return f.object(x).fields(f.field("condition", x.condition),
                              f.field("consequence", x.consequence),
                              f.field("alternative", x.alternative));
  }
};

/// Shared implementation for both transform and sink variants of `if`.
class IfImpl {
public:
  explicit IfImpl(IfArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> {
    // Spawn subpipelines if they are not already spawned (due to restore).
    if (not ctx.get_sub(true).is_some()) {
      if (not co_await ctx.plan_and_spawn_sub<table_slice>(true,
                                                           args_.consequence)) {
        co_return;
      }
      if (args_.alternative) {
        if (not co_await ctx.plan_and_spawn_sub<table_slice>(
              false, *args_.alternative)) {
          co_return;
        }
      }
    }
  }

  auto process(table_slice input, OpCtx& ctx, Push<table_slice>* push = nullptr)
    -> Task<void> {
    // FIXME: If the inner subpipelines terminate and get erased, this can fail.
    auto& true_sub = ctx.get_sub(true).unwrap();
    auto& consequence = as<SubHandle<table_slice>>(true_sub);
    auto false_sub = ctx.get_sub(false);
    auto alternative
      = false_sub ? Option<SubHandle<table_slice>&>{as<SubHandle<table_slice>>(
                      *false_sub)}
                  : None{};
    TENZIR_ASSERT(alternative.is_some() == args_.alternative.has_value());
    auto true_events = std::vector<table_slice>{};
    auto false_events = std::vector<table_slice>{};
    auto end = int64_t{0};
    for (auto const& predicate : eval(args_.condition, input, ctx)) {
      auto const start = std::exchange(end, end + predicate.length());
      TENZIR_ASSERT(end > start);
      auto const sliced_input = subslice(input, start, end);
      auto const typed_predicate = predicate.as<bool_type>();
      if (not typed_predicate) {
        diagnostic::warning("expected `bool`, but got `{}`",
                            predicate.type.kind())
          .primary(args_.condition)
          .emit(ctx);
        TENZIR_ASSERT(sliced_input.rows() > 0);
        false_events.push_back(sliced_input);
        continue;
      }
      if (typed_predicate->array->null_count() > 0) {
        diagnostic::warning("expected `bool`, but got `null`")
          .primary(args_.condition)
          .emit(ctx);
      }
      auto [lhs, rhs] = partition(sliced_input, *typed_predicate->array);
      TENZIR_ASSERT(lhs.rows() + rhs.rows() == sliced_input.rows());
      if (lhs.rows() > 0) {
        true_events.push_back(std::move(lhs));
      }
      if (rhs.rows() > 0) {
        false_events.push_back(std::move(rhs));
      }
    }
    if (not consequence_closed_) {
      for (auto& slice : rebatch(std::move(true_events))) {
        consequence_closed_
          = (co_await consequence.push(std::move(slice))).is_err();
      }
    }
    if (not alternative_closed_) {
      for (auto& slice : rebatch(std::move(false_events))) {
        if (alternative) {
          alternative_closed_
            = (co_await alternative->push(std::move(slice))).is_err();
        } else if (push) {
          co_await (*push)(std::move(slice));
        }
      }
    }
  }

  auto state() -> OperatorState {
    if (consequence_closed_ and alternative_closed_) {
      return OperatorState::done;
    }
    return OperatorState::normal;
  }

private:
  IfArgs args_;
  bool consequence_closed_ = false;
  bool alternative_closed_ = false;
};

class If final : public Operator<table_slice, table_slice> {
public:
  explicit If(IfArgs args) : impl_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    return impl_.start(ctx);
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    return impl_.process(std::move(input), ctx, &push);
  }

  auto state() -> OperatorState override {
    return impl_.state();
  }

private:
  IfImpl impl_;
};

/// Sink variant of `if` for when both branches return void.
class IfSink final : public Operator<table_slice, void> {
public:
  explicit IfSink(IfArgs args) : impl_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    return impl_.start(ctx);
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    return impl_.process(std::move(input), ctx);
  }

  auto state() -> OperatorState override {
    return impl_.state();
  }

private:
  IfImpl impl_;
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

  auto optimize(ir::optimize_filter filter,
                event_order order) && -> ir::optimize_result override {
    // We need to skip `-> void` pipelines, which are invalid to optimize with
    // the downstream filter.
    auto null_dh = null_diagnostic_handler{};
    auto outputs_events = [&](ir::pipeline const& pipe) -> bool {
      auto t = pipe.infer_type(tag_v<table_slice>, null_dh);
      return t and (*t).is<table_slice>();
    };
    auto optimize_branch
      = [&](ir::pipeline& branch, ir::optimize_filter f) -> event_order {
      auto opt = std::move(branch).optimize(std::move(f), order);
      branch = std::move(opt.replacement);
      branch.operators.insert_range(branch.operators.begin(),
                                    opt.filter
                                      | std::views::transform(make_where_ir));
      return opt.order;
    };
    // Handle downstream filters when there is no explicit `else` branch.
    if (not args_.alternative and not filter.empty()) {
      args_.alternative.emplace(ir::pipeline{});
    }
    auto cons_filter
      = outputs_events(args_.consequence) ? filter : ir::optimize_filter{};
    auto cons_order
      = optimize_branch(args_.consequence, std::move(cons_filter));
    auto alt_order = order;
    if (args_.alternative) {
      auto alt_filter = outputs_events(*args_.alternative)
                          ? std::move(filter)
                          : ir::optimize_filter{};
      alt_order = optimize_branch(*args_.alternative, std::move(alt_filter));
    }
    auto replacement = std::vector<Box<ir::Operator>>{};
    replacement.push_back(std::move(*this).move());
    return {
      {},
      stronger_event_order(cons_order, alt_order),
      ir::pipeline{{}, std::move(replacement)},
    };
  }

  auto spawn(element_type_tag input) const -> AnyOperator override {
    TENZIR_ASSERT(input.is<table_slice>());
    auto dh = null_diagnostic_handler{};
    auto output = infer_type(input, dh);
    TENZIR_ASSERT(output);
    if ((*output).is<void>()) {
      return IfSink{args_}.with_name("if");
    }
    return If{args_}.with_name("if");
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

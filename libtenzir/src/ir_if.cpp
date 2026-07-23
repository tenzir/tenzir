//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/ir_if.hpp"

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

/// Routes rows across two lanes by evaluating a boolean condition: `true` rows
/// to lane 0 (the consequence), `false`/`null` rows to lane 1 (the
/// alternative). A non-boolean condition routes the whole subslice to the
/// alternative and emits a diagnostic, matching the `if` operator's semantics.
class BoolSplitter final : public Splitter {
public:
  explicit BoolSplitter(ast::expression condition)
    : condition_{std::move(condition)} {
  }

  auto copy() const -> Box<Splitter> override {
    return BoolSplitter{condition_};
  }

  auto lanes() const -> size_t override {
    return 2;
  }

  auto split(table_slice slice, diagnostic_handler& dh) const
    -> std::vector<SplitRun> override {
    auto runs = std::vector<SplitRun>{};
    auto end = int64_t{0};
    for (const auto& [predicate] :
         split_multi_series(eval(condition_, slice, dh))) {
      const auto start = std::exchange(end, end + predicate.length());
      TENZIR_ASSERT(end > start);
      const auto sliced = subslice(slice, start, end);
      const auto typed = predicate.as<bool_type>();
      if (not typed) {
        // A non-boolean condition routes the whole subslice to the
        // alternative, matching `(not <cond>) else true`.
        diagnostic::warning("expected `bool`, got `{}`", predicate.type.kind())
          .primary(condition_)
          .emit(dh);
        runs.push_back({1, sliced});
        continue;
      }
      if (typed->array->null_count() > 0) {
        diagnostic::warning("expected `bool`, got `null`")
          .primary(condition_)
          .emit(dh);
      }
      // `partition` sends `true` rows to the first slice and `false`/`null`
      // rows to the second.
      auto [then_slice, else_slice] = partition(sliced, *typed->array);
      TENZIR_ASSERT(then_slice.rows() + else_slice.rows() == sliced.rows());
      if (then_slice.rows() > 0) {
        runs.push_back({0, std::move(then_slice)});
      }
      if (else_slice.rows() > 0) {
        runs.push_back({1, std::move(else_slice)});
      }
    }
    return runs;
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

  auto spawn(element_type_tag) const -> AnyOperator override {
    // `if` expands into the plan via `plan()` and is never spawned as a single
    // node.
    TENZIR_UNREACHABLE();
  }

  auto plan(ir::PlanBuilder& builder, ir::PlanPorts input,
            diagnostic_handler& dh) && -> failure_or<ir::PlanPorts> override {
    // Flatten `if` into a `Split` channel that evaluates the condition per row
    // and routes each row to exactly one branch: `true` rows to the
    // consequence, `false`/`null` rows to the alternative. Without an explicit
    // `else`, the alternative forwards the unmatched rows unchanged. Unlike a
    // broadcast-plus-`where` expansion, the split materializes each row into
    // only one branch and evaluates the condition (and emits the sole
    // `expected bool` diagnostic) exactly once.
    auto src = builder.into_single(input);
    auto heads = std::vector<size_t>{};
    auto tails = ir::PlanPorts{};
    auto lower_branch = [&](ir::pipeline branch) -> failure_or<void> {
      auto ty = tag_v<table_slice>;
      auto head = builder.add_identity(ty);
      heads.push_back(head);
      TRY(auto tail,
          builder.lower_pipeline(std::move(branch),
                                 ir::PlanPorts{ir::PlanPort{head, ty}}, dh));
      tails.insert(tails.end(), tail.begin(), tail.end());
      return {};
    };
    // The first branch head (lane 0) receives the `true` rows, the second
    // (lane 1) the `false`/`null` rows.
    TRY(lower_branch(std::move(args_.consequence)));
    auto alternative
      = args_.alternative ? std::move(*args_.alternative) : ir::pipeline{};
    TRY(lower_branch(std::move(alternative)));
    builder.add_split(src, std::move(heads),
                      BoolSplitter{std::move(args_.condition)});
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

auto make_if_ir_inspection_plugin() -> plugin* {
  return new inspection_plugin<ir::Operator, IfIr>{};
}

} // namespace tenzir

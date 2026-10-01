//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/builtins/sort.hpp"

#include "tenzir/async.hpp"
#include "tenzir/nova/eval.hpp"
#include "tenzir/nova/sort.hpp"

#include <utility>

namespace tenzir::plugins::sort {

namespace {

class Sort final : public Operator<nova::Events, nova::Events> {
public:
  Sort(SortArgs args, Option<uint64_t> limit) {
    if (args.exprs.empty()) {
      args.exprs.emplace_back(ast::this_{location::unknown});
    }
    auto orders = std::vector<nova::Order>{};
    for (auto& expr : args.exprs) {
      auto* unary = try_as<ast::unary_expr>(expr);
      auto reverse = unary and unary->op == ast::unary_op::neg;
      orders.push_back(reverse ? nova::Order::descending
                               : nova::Order::ascending);
      expressions_.push_back(reverse ? std::move(unary->expr)
                                     : std::move(expr));
    }
    buffer_ = nova::SortBuffer{std::move(orders), limit};
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    for (auto& expr : expressions_) {
      auto evaluator = co_await nova::Evaluator::make(std::move(expr), ctx);
      if (not evaluator) {
        co_return;
      }
      evaluators_.push_back(std::move(*evaluator));
    }
  }

  auto process(nova::Events input, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(push);
    auto keys = std::vector<nova::Array<nova::Data>>{};
    for (auto& evaluator : evaluators_) {
      keys.push_back(evaluator.eval(input, nova::EvalCtx{ctx.dh()}));
    }
    buffer_.add(std::move(input), std::move(keys));
    co_return;
  }

  auto finalize(Push<nova::Events>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(ctx);
    for (auto batch : buffer_.finish()) {
      co_await push(std::move(batch));
    }
    co_return FinalizeBehavior::done;
  }

  auto snapshot(Serde& serde) -> void override {
    serde("buffer", buffer_);
  }

private:
  std::vector<ast::expression> expressions_;
  std::vector<nova::Evaluator> evaluators_;
  nova::SortBuffer buffer_;
};

} // namespace

auto make_sort(SortArgs args, Option<uint64_t> limit) -> AnyOperator {
  return Sort{std::move(args), limit}.with_name("sort");
}

} // namespace tenzir::plugins::sort

//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/assert.hpp"
#include "tenzir/nova/array_base.hpp"
#include "tenzir/nova/eval_internal.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/nova/function_plugin.hpp"
#include "tenzir/tql2/plugin_api.hpp"
#include "tenzir/try.hpp"

#include <functional>
#include <utility>
#include <vector>

namespace tenzir::nova {

namespace {

class CallSitePreparer final : public ast::visitor<CallSitePreparer> {
public:
  CallSitePreparer(
    InstantiateCtx ctx,
    std::unordered_map<ast::function_call const*, Box<_::CallSite>>& call_sites)
    : ctx_{ctx}, call_sites_{call_sites} {
  }

  auto run(ast::expression& expression) -> failure_or<void> {
    visit(expression);
    return failed_ ? failure_or<void>{failure::promise()} : failure_or<void>{};
  }

  template <class T>
  auto visit(T& x) -> void {
    if (not failed_) {
      enter(x);
    }
  }

  auto visit(ast::function_call& call) -> void {
    if (failed_) {
      return;
    }
    auto const& plugin = static_cast<registry const&>(ctx_).get(call);
    auto const* nova_plugin
      = dynamic_cast<FunctionPlugin const*>(std::addressof(plugin));
    if (not nova_plugin) {
      diagnostic::error("function `{}` is not implemented for the nova model",
                        plugin.function_name())
        .primary(call)
        .emit(ctx_);
      failed_ = true;
      return;
    }
    auto instantiation = nova_plugin->instantiate(call, ctx_);
    if (not instantiation) {
      failed_ = true;
      return;
    }
    // Everything the call evaluates later — argument expressions, lambda
    // bodies — is part of this expression, not of a nested evaluation: its
    // call sites go into the same call-site table, keyed by node address like
    // any other. Instantiation reports those nodes because it borrows them
    // instead of taking ownership.
    for (auto* expr : instantiation->deferred) {
      visit(*expr);
      if (failed_) {
        return;
      }
    }
    auto const inserted
      = call_sites_
          .emplace(std::addressof(call), std::move(instantiation->call_site))
          .second;
    TENZIR_ASSERT(inserted);
  }

  /// A lambda in expression position, i.e. one that is not the argument of a
  /// function that registered it as such.
  auto visit(ast::lambda_expr& lambda) -> void {
    diagnostic::error("expected an expression, got a lambda")
      .primary(lambda)
      .emit(ctx_);
    failed_ = true;
  }

private:
  InstantiateCtx ctx_;
  std::unordered_map<ast::function_call const*, Box<_::CallSite>>& call_sites_;
  bool failed_ = false;
};

} // namespace

Evaluator::Evaluator(ast::expression expression)
  : expression_{std::move(expression)} {
}

Evaluator::Evaluator(Evaluator&&) noexcept = default;

auto Evaluator::operator=(Evaluator&&) noexcept -> Evaluator& = default;

Evaluator::~Evaluator() = default;

auto Evaluator::make(ast::expression expression, InstantiateCtx ctx)
  -> failure_or<Evaluator> {
  auto result = Evaluator{std::move(expression)};
  TRY(result.prepare(*result.expression_, ctx));
  return result;
}

auto Evaluator::prepare(ast::expression& expression, InstantiateCtx ctx)
  -> failure_or<void> {
  auto preparer = CallSitePreparer{ctx, call_sites_};
  return preparer.run(expression);
}

ValueArgument::ValueArgument() : data{Array<Null>{storage::NullStorage{0}}} {
}

ValueArgument::ValueArgument(Array<Data> array, ::tenzir::location loc)
  : data{std::move(array)}, source{loc} {
}

auto _::CallSite::eval(EvalFrame frame) -> Array<Data> {
  // Every value is produced for exactly the rows the call was asked for, so
  // a function never has to think about which rows its arguments cover.
  // Slots are refilled on every call, which is what makes it safe to reuse
  // one bundle across batches, masks, and inputs.
  for (auto const& slot : slots_) {
    slot.store(args_, frame.eval(LazyArgument{*slot.expr}));
  }
  return kernel_(args_, std::move(frame));
}

auto Evaluator::call_site(ast::function_call const& call) -> _::CallSite& {
  auto it = call_sites_.find(std::addressof(call));
  TENZIR_ASSERT(it != call_sites_.end());
  return *it->second;
}

auto EvalFrame::dh() const -> diagnostic_handler& {
  return run_->ctx_.dh();
}

EvalFrame::operator diagnostic_handler&() const {
  return dh();
}

EvalFrame::operator EvalCtx() const {
  return run_->ctx_;
}

auto EvalFrame::ctx() const -> EvalCtx {
  return run_->ctx_;
}

auto EvalFrame::input() const -> Events const* {
  return run_->input_;
}

auto EvalFrame::eval(ast::expression const& expr) const -> Array<Data> {
  return run_->eval(expr, *this);
}

auto Evaluator::eval(Events const& events, EvalCtx ctx) -> Array<Data> {
  return eval(Option<Events const&>{events}, ctx);
}

auto Evaluator::eval(EvalCtx ctx) -> Array<Data> {
  return eval(None{}, ctx);
}

auto Evaluator::eval(Option<Events const&> events, EvalCtx ctx) -> Array<Data> {
  TENZIR_ASSERT(expression_, "evaluator has not been initialized");
  auto input = events ? std::addressof(*events) : nullptr;
  auto run = _::EvalRun{*this, input, ctx};
  // The root frame establishes the invariant that every frame's mask is a
  // subset of the input's mask; `EvalFrame::narrow` preserves it.
  auto frame = EvalFrame{run, active_mask(events)};
  return run.eval(*expression_, std::move(frame));
}

_::EvalRun::EvalRun(Evaluator& evaluator, Events const* input, EvalCtx ctx)
  : evaluator_{std::addressof(evaluator)}, input_{input}, ctx_{ctx} {
}

auto _::EvalRun::fail_not_constant(location loc, EvalFrame frame) const
  -> Array<Data> {
  diagnostic::error("expected a constant expression").primary(loc).emit(ctx_);
  return frame.null();
}

auto _::EvalRun::eval(ast::expression const& expr, EvalFrame frame)
  -> Array<Data> {
  return trace_panic(expr, [this, &expr, &frame] {
    return match(expr, [this, &frame](auto const& x) {
      return eval(x, std::move(frame));
    });
  });
}

} // namespace tenzir::nova

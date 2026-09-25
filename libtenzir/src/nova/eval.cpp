//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/assert.hpp"
#include "tenzir/nova/aggregation.hpp"
#include "tenzir/nova/array_base.hpp"
#include "tenzir/nova/const_eval.hpp"
#include "tenzir/nova/eval_internal.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/nova/function_plugin.hpp"
#include "tenzir/secret.hpp"
#include "tenzir/secret_resolution.hpp"
#include "tenzir/tql2/plugin_api.hpp"
#include "tenzir/try.hpp"

#include <algorithm>
#include <functional>
#include <iterator>
#include <utility>
#include <vector>

namespace tenzir::nova {

namespace {

auto is_secret_call(ast::function_call const& call) -> bool {
  auto const& ref = call.fn.ref;
  return ref.pkg() == entity_pkg_std and ref.segments().size() == 1
         and ref.segments()[0] == "secret";
}

class SecretResolver final : public ast::visitor<SecretResolver> {
public:
  SecretResolver(InstantiateCtx ctx, std::vector<secret_request>& requests)
    : ctx_{ctx}, requests_{requests} {
  }

  template <class T>
  auto visit(T& x) -> void {
    enter(x);
  }

  auto visit(ast::expression& expression) -> void {
    expression.match([this, &expression](auto& node) {
      visit_node(expression, node);
    });
  }

  auto visit_node(ast::expression&, auto& node) -> void {
    enter(node);
  }

  auto visit_node(ast::expression& expression, ast::function_call& call)
    -> void {
    if (not is_secret_call(call)) {
      enter(call);
      return;
    }
    if (call.args.size() != 1) {
      diagnostic::error("`secret` expects exactly one string argument")
        .primary(call)
        .emit(ctx_);
      failed = true;
      return;
    }
    auto name = const_eval(call.args.front(), ctx_);
    if (not name) {
      failed = true;
      return;
    }
    auto* name_string = try_as<std::string>(&*name);
    if (not name_string) {
      diagnostic::error("`secret` expects a string")
        .primary(call.args.front())
        .emit(ctx_);
      failed = true;
      return;
    }
    auto const target = std::addressof(expression);
    auto const source = call.get_location();
    requests_.emplace_back(
      secret::make_managed(*name_string), source,
      [target, source](resolved_secret_value value) -> failure_or<void> {
        auto secret = Secret{
          ecc::cleansing_blob{value.blob().begin(), value.blob().end()}};
        *target = ast::resolved_secret{std::move(secret), source};
        return {};
      });
  }

  bool failed = false;

private:
  InstantiateCtx ctx_;
  std::vector<secret_request>& requests_;
};

/// Collect only the calls at this level. Their descriptions decide which
/// arguments are constants and which expressions need subsequent preparation.
class CallSiteCollector final : public ast::visitor<CallSiteCollector> {
public:
  explicit CallSiteCollector(InstantiateCtx ctx) : ctx_{ctx} {
  }

  template <class T>
  auto visit(T& x) -> void {
    enter(x);
  }

  auto visit(ast::function_call& call) -> void {
    calls.push_back(std::addressof(call));
  }

  auto visit(ast::lambda_expr& lambda) -> void {
    diagnostic::error("expected an expression, got a lambda")
      .primary(lambda)
      .emit(ctx_);
    failed = true;
  }

  std::vector<ast::function_call*> calls;
  bool failed = false;

private:
  InstantiateCtx ctx_;
};

} // namespace

auto resolve_secrets(ast::expression& expression, OpCtx& ctx,
                     diagnostic_handler& dh) -> Task<failure_or<void>> {
  auto secrets = std::vector<secret_request>{};
  auto resolver = SecretResolver{InstantiateCtx{dh, ctx.reg()}, secrets};
  resolver.visit(expression);
  if (resolver.failed) {
    co_return failure::promise();
  }
  if (not secrets.empty()) {
    CO_TRY(co_await ctx.resolve_secrets(std::move(secrets)));
  }
  co_return {};
}

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

auto Evaluator::make(ast::expression expression, OpCtx& ctx)
  -> Task<failure_or<Evaluator>> {
  return make(std::move(expression), ctx, ctx.dh());
}

auto Evaluator::make(ast::expression expression, OpCtx& ctx,
                     diagnostic_handler& dh) -> Task<failure_or<Evaluator>> {
  CO_TRY(co_await resolve_secrets(expression, ctx, dh));
  co_return make(std::move(expression), InstantiateCtx{dh, ctx.reg()});
}

auto Evaluator::prepare(ast::expression& expression, InstantiateCtx ctx)
  -> failure_or<void> {
  auto collector = CallSiteCollector{ctx};
  collector.visit(expression);
  if (collector.failed) {
    return failure::promise();
  }
  for (auto* call : collector.calls) {
    auto const& plugin = static_cast<registry const&>(ctx).get(*call);
    // Functions come first; an aggregation invoked in expression position is
    // instantiated through its own description, whose kernel is the
    // regular-function fallback over list rows.
    auto instantiate = [&]() -> failure_or<FunctionDescription::Instantiation> {
      if (auto const* function
          = dynamic_cast<FunctionPlugin const*>(std::addressof(plugin))) {
        return function->instantiate(*call, ctx);
      }
      if (auto const* aggregation
          = dynamic_cast<AggregationPlugin const*>(std::addressof(plugin))) {
        return aggregation->instantiate(*call, ctx);
      }
      diagnostic::error("function `{}` is not implemented for the nova model",
                        plugin.function_name())
        .primary(*call)
        .emit(ctx);
      return failure::promise();
    };
    TRY(auto instantiation, instantiate());
    // Everything the call evaluates later — argument expressions, lambda
    // bodies — is part of this expression, not of a nested evaluation: its
    // call sites go into the same call-site table, keyed by node address like
    // any other. Instantiation reports those nodes because it borrows them
    // instead of taking ownership. Their call sites join this evaluator's
    // single preparation pass.
    for (auto* expr : instantiation.deferred) {
      TRY(prepare(*expr, ctx));
    }
    auto const inserted
      = call_sites_.emplace(call, std::move(instantiation.call_site)).second;
    TENZIR_ASSERT(inserted);
  }
  return {};
}

ValueArgument::ValueArgument() : data{Array<Null>{storage::NullStorage{0}}} {
}

ValueArgument::ValueArgument(Array<Data> array, ::tenzir::location loc)
  : data{std::move(array)}, source{loc} {
}

auto _::CallSite::fill(EvalFrame const& frame) -> Any const& {
  // Every value is produced for exactly the rows the call was asked for, so
  // a function never has to think about which rows its arguments cover.
  // Slots are refilled on every call, which is what makes it safe to reuse
  // one bundle across batches, masks, and inputs.
  for (auto const& slot : slots_) {
    slot.store(args_, frame.eval(LazyArgument{*slot.expr}));
  }
  return args_;
}

auto _::CallSite::eval(EvalFrame frame) -> Array<Data> {
  auto const& args = fill(frame);
  return kernel_(args, std::move(frame));
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

auto EvalFrame::detached_impl(storage::BitMap mask, void* ctx,
                              void (*f)(void*, EvalFrame)) const -> void {
  // A field-less input of the mask's length, like the input-less lambda
  // application: both `make_empty`s are constants, so this is O(1). The root
  // frame's mask is the input's mask, which establishes the subset invariant.
  auto const length = mask.length();
  auto events = Events{Array<Record>::make_empty(length), std::move(mask),
                       Events::Meta::make_empty(length)};
  auto run = _::EvalRun{*run_->evaluator_, std::addressof(events), run_->ctx_};
  f(ctx, EvalFrame{run, events.mask});
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

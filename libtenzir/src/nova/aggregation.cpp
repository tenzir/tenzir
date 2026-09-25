//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/aggregation.hpp"

#include "tenzir/async.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/tql2/registry.hpp"
#include "tenzir/try.hpp"

#include <memory>
#include <utility>

namespace tenzir::nova {

auto Aggregation::make(ast::expression expr, InstantiateCtx ctx)
  -> failure_or<Box<Aggregation>> {
  auto const* call = try_as<ast::function_call>(&expr);
  if (not call) {
    diagnostic::error("expected an aggregation function")
      .primary(expr)
      .emit(ctx);
    return failure::promise();
  }
  auto const& plugin = static_cast<registry const&>(ctx).get(*call);
  if (not dynamic_cast<AggregationPlugin const*>(std::addressof(plugin))) {
    diagnostic::error("`{}` is not an aggregation function",
                      plugin.function_name())
      .primary(*call)
      .emit(ctx);
    return failure::promise();
  }
  // Preparing the expression instantiates the root through the aggregation
  // plugin, which leaves the factory on its call site. The node itself never
  // moves, so `call` stays valid past the move of `expr`.
  TRY(auto evaluator, Evaluator::make(std::move(expr), ctx));
  auto factory = evaluator.call_site(*call).aggregation();
  TENZIR_ASSERT(factory, "aggregation plugin did not use AggregationDescriber");
  return factory(std::move(evaluator), *call);
}

auto Aggregation::make(ast::expression expr, OpCtx& ctx)
  -> Task<failure_or<Box<Aggregation>>> {
  CO_TRY(co_await resolve_secrets(expr, ctx, ctx.dh()));
  co_return make(std::move(expr), InstantiateCtx{ctx.dh(), ctx.reg()});
}

auto AggregationInstance::make(ast::expression expr, OpCtx& ctx)
  -> Task<failure_or<Box<AggregationInstance>>> {
  CO_TRY(auto aggregation, co_await Aggregation::make(std::move(expr), ctx));
  co_return AggregationInstance{std::move(aggregation)};
}

auto AggregationDescription::instantiate(std::string_view name,
                                         ast::function_call& call,
                                         InstantiateCtx ctx) const
  -> failure_or<Instantiation> {
  TRY(auto instantiation, FunctionDescription::instantiate(name, call, ctx));
  instantiation.call_site.set_aggregation(factory_);
  return instantiation;
}

auto AggregationPlugin::instantiate(ast::function_call& call,
                                    InstantiateCtx ctx) const
  -> failure_or<FunctionDescription::Instantiation> {
  return describe().instantiate(function_name(), call, ctx);
}

} // namespace tenzir::nova

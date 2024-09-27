//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/plugin.hpp"

#include "tenzir/tql2/eval_impl.hpp"

namespace tenzir {

auto function_use::evaluator::length() const -> int64_t {
  return static_cast<tenzir::evaluator*>(self_)->length();
}

auto function_use::evaluator::operator()(const ast::expression& expr) const
  -> series {
  return static_cast<tenzir::evaluator*>(self_)->eval(expr);
}

auto aggregation_plugin::make_function(invocation inv, session ctx) const
  -> failure_or<function_ptr> {
  // TODO: Consider making this pure-virtual or provide a default implementation.
  diagnostic::error("this function can only be used as an aggregation function")
    .primary(inv.call.fn)
    .emit(ctx);
  return failure::promise();
}

auto function_use::make(
  std::function<auto(evaluator eval, session ctx)->series> f)
  -> std::unique_ptr<function_use> {
  class result final : public function_use {
  public:
    explicit result(std::function<auto(evaluator eval, session ctx)->series> f)
      : f_{std::move(f)} {
    }

    auto run(evaluator eval, session ctx) const -> series override {
      return f_(eval, ctx);
    }

  private:
    std::function<auto(evaluator eval, session ctx)->series> f_;
  };
  return std::make_unique<result>(std::move(f));
}

auto function_plugin::function_name() const -> std::string {
  // TODO: Remove this once we removed TQL1 plugins.
  auto result = name();
  if (result.starts_with("tql2.")) {
    result.erase(0, 5);
  }
  return result;
}

} // namespace tenzir

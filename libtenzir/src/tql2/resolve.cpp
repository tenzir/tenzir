//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/resolve.hpp"

#include "tenzir/detail/assert.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/tql2/registry.hpp"

#include <tsl/robin_map.h>

namespace tenzir {

namespace {

class entity_resolver : public ast::visitor<entity_resolver> {
public:
  explicit entity_resolver(session ctx) : reg_{ctx.reg()}, diag_{ctx.dh()} {
  }

  void visit(ast::entity& x) {
    TENZIR_ASSERT(not x.path.empty());
    TENZIR_ASSERT(context_ != context_t::none);
    if (x.path.size() > 1) {
      diagnostic::error("module `{}` not found", x.path[0].name)
        .primary(x.path[0])
        .emit(diag_);
      result_ = failure::promise();
      return;
    }
    auto& name = x.path[0].name;
    auto ns = context_ == context_t::op_name ? entity_ns::op : entity_ns::fn;
    // TODO: We pretend here that every name directly maps to its path.
    auto path = entity_path{{name}, ns};
    auto entity = reg_.try_get(path);
    auto expected = std::invoke([&] {
      switch (context_) {
        case context_t::op_name:
          return "operator";
        case context_t::fn_name:
          return "function";
        case context_t::none:
          TENZIR_UNREACHABLE();
      }
      TENZIR_UNREACHABLE();
    });
    if (not entity) {
      auto available = std::invoke([&] {
        switch (ns) {
          case entity_ns::op:
            return reg_.operator_names();
          case entity_ns::fn:
            return reg_.function_names();
        }
        TENZIR_UNREACHABLE();
      });
      // TODO: Consider reporting whether an entity with this name of another
      // type would be found.
      diagnostic::error("{} `{}` not found", expected, name)
        .primary(x)
        .hint("must be one of: {}", fmt::join(available, ", "))
        .emit(diag_);
      result_ = failure::promise();
      return;
    }
    x.ref = std::move(path);
  }

  void visit(ast::invocation& x) {
    auto prev = std::exchange(context_, context_t::op_name);
    visit(x.op);
    context_ = prev;
    for (auto& y : x.args) {
      visit(y);
    }
  }

  void visit(ast::function_call& x) {
    auto prev = std::exchange(context_, context_t::fn_name);
    visit(x.fn);
    context_ = prev;
    for (auto& y : x.args) {
      visit(y);
    }
  }

  template <class T>
  void visit(T& x) {
    enter(x);
  }

  auto get_failure() -> failure_or<void> {
    return result_;
  }

private:
  enum class context_t { none, op_name, fn_name };
  const registry& reg_;
  diagnostic_handler& diag_;
  context_t context_ = context_t::none;
  failure_or<void> result_;
};

} // namespace

auto resolve_entities(ast::pipeline& pipe, session ctx) -> failure_or<void> {
  auto resolver = entity_resolver{ctx};
  resolver.visit(pipe);
  return resolver.get_failure();
}

auto resolve_entities(ast::expression& expr, session ctx) -> failure_or<void> {
  auto resolver = entity_resolver{ctx};
  resolver.visit(expr);
  return resolver.get_failure();
}

} // namespace tenzir

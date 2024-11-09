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
    auto ns = context_ == context_t::op_name ? entity_ns::op : entity_ns::fn;
    // TODO: We pretend here that every name directly maps to its path. This is
    // only the case if there are no user-given names.
    auto segments = std::vector<std::string>{};
    segments.reserve(x.path.size());
    for (auto& segment : x.path) {
      segments.push_back(segment.name);
    }
    auto path = entity_path{std::move(segments), ns};
    auto result = reg_.try_get(path);
    auto err = std::get_if<registry::error>(&result);
    if (not err) {
      x.ref = std::move(path);
      return;
    }
    TENZIR_ASSERT(err->segment < x.path.size());
    auto last = err->segment == path.segments().size() - 1;
    if (not last) {
      // TODO: We could print if there was something else with that name.
      diagnostic::error("module `{}` not found", x.path[err->segment].name)
        .primary(x.path[err->segment])
        .emit(diag_);
      result_ = failure::promise();
      return;
    }
    auto type = std::invoke([&] {
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
    if (err->other_exists) {
      diagnostic::error("`{}` is not a `{}`", x.path[err->segment].name, type)
        .primary(x.path[err->segment])
        .emit(diag_);
      result_ = failure::promise();
    } else {
      // TODO: This list is too long. Suggest only close matches instead. Also,
      // we should maybe only suggest things in the same module.
      auto available = std::invoke([&] {
        switch (ns) {
          case entity_ns::op:
            return reg_.operator_names();
          case entity_ns::fn:
            return reg_.function_names();
        }
        TENZIR_UNREACHABLE();
      });
      diagnostic::error("{} `{}` not found", type, x.path[err->segment].name)
        .primary(x.path[err->segment])
        .hint("must be one of: {}", fmt::join(available, ", "))
        .emit(diag_);
      result_ = failure::promise();
    }
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

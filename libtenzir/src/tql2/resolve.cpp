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
    if (x.ref.resolved()) {
      return;
    }
    TENZIR_ASSERT(not x.path.empty());
    TENZIR_ASSERT(context_ != context_t::none);
    // We use the following logic:
    // - Look at the first segment.
    // - Use its name + namespace (mod/fn/op) for lookup as user-defined.
    // - If something is found: Use rest of the segments. If not found: Error.
    // - If nothing is found: Repeat with lookup for built-in entities.
    auto report_not_found = [&](const ast::identifier& ident, entity_ns ns) {
      auto type = std::invoke([&] {
        switch (ns) {
          case entity_ns::op:
            return "operator";
          case entity_ns::fn:
            return "function";
          case entity_ns::mod:
            return "module";
        }
        TENZIR_UNREACHABLE();
      });
      // TODO: This list is too long. Suggest only close matches instead. Also,
      // we should maybe only suggest things in the same module.
      auto available = std::invoke([&] {
        switch (ns) {
          case entity_ns::op:
            return reg_.operator_names();
          case entity_ns::fn:
            return reg_.function_names();
          case entity_ns::mod:
            return reg_.module_names();
        }
        TENZIR_UNREACHABLE();
      });
      auto diag = diagnostic::error("{} `{}` not found", type, ident.name)
                    .primary(ident);
      if (not available.empty()) {
        diag = std::move(diag).hint("must be one of: {}",
                                    fmt::join(available, ", "));
      }
      std::move(diag).emit(diag_);
      result_ = failure::promise();
    };
    auto target_ns
      = context_ == context_t::op_name ? entity_ns::op : entity_ns::fn;
    auto first_ns = x.path.size() == 1 ? target_ns : entity_ns::mod;
    // Because there currently is no way to bring additional entities into the
    // scope, we can directly dispatch to the registry.
    auto pkg = std::invoke([&]() -> std::optional<entity_pkg> {
      for (auto pkg : {entity_pkg::cfg, entity_pkg::std}) {
        auto path = entity_path{pkg, {x.path[0].name}, first_ns};
        if (is<entity_ref>(reg_.try_get(path))) {
          return pkg;
        }
      }
      return std::nullopt;
    });
    if (not pkg) {
      report_not_found(x.path[0], first_ns);
      return;
    }
    // After figuring out which package it belongs to, we try the full path.
    auto segments = std::vector<std::string>{};
    segments.reserve(x.path.size());
    for (auto& segment : x.path) {
      segments.push_back(segment.name);
    };
    auto path = entity_path{*pkg, std::move(segments), target_ns};
    auto result = reg_.try_get(path);
    auto err = try_as<registry::error>(result);
    if (err) {
      TENZIR_ASSERT(err->segment < x.path.size());
      auto is_last = err->segment == path.segments().size() - 1;
      auto error_ns = is_last ? target_ns : entity_ns::mod;
      report_not_found(x.path[err->segment], error_ns);
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

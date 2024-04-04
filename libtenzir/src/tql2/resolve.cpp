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

namespace tenzir::tql2 {

namespace {

using namespace tenzir::tql2::ast;

class entity_resolver : public visitor<entity_resolver> {
public:
  entity_resolver(const registry& reg, diagnostic_handler& diag)
    : reg_{reg}, diag_{diag} {
    // Add every top-level package to `scope_` with its name.
    // TODO: Add everything from `std::prelude` to `scope_`.
  }

  void visit(entity& x) {
    TENZIR_ASSERT(not x.path.empty());
    TENZIR_ASSERT(context_ != context_t::none);
    if (x.path.size() > 1) {
      diagnostic::error("module `{}` not found", x.path[0].name)
        .primary(x.path[0].location)
        .emit(diag_);
      return;
    }
    auto& name = x.path[0].name;
    // TODO: We pretend here that every name directly maps to its path.
    auto xyz = reg_.try_get(entity_path{{name}});
    auto expected = std::invoke([&] {
      switch (context_) {
        case context_t::op_name:
          return "operator";
        case context_t::fn_name:
          return "function";
        case context_t::method_name:
          return "method";
        case context_t::none:
          TENZIR_UNREACHABLE();
      }
    });
    if (not xyz) {
      diagnostic::error("{} `{}` not found", expected, name)
        .primary(x.get_location())
        .emit(diag_);
      return;
    }
    // TODO: Check if this entity has right type?
    xyz->match(
      [&](const std::unique_ptr<function_def>&) {
        // TODO: Methods?
        x.ref = entity_path{{name}};
      },
      [&](const operator_factory_plugin*) {
        if (context_ != context_t::op_name) {
          diagnostic::error("expected {}, got operator", expected)
            .primary(x.get_location())
            .emit(diag_);
          return;
        }
        x.ref = entity_path{{name}};
      });
  }

  void visit(invocation& x) {
    auto prev = std::exchange(context_, context_t::op_name);
    visit(x.op);
    context_ = prev;
    for (auto& y : x.args) {
      visit(y);
    }
  }

  void visit(function_call& x) {
    if (x.subject) {
      visit(*x.subject);
    }
    auto prev = std::exchange(context_, x.subject ? context_t::method_name
                                                  : context_t::fn_name);
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

private:
  enum class context_t { none, op_name, fn_name, method_name };
  const registry& reg_;
  diagnostic_handler& diag_;
  context_t context_ = context_t::none;
};

} // namespace

void resolve_entities(ast::pipeline& pipe, registry& reg,
                      diagnostic_handler& diag) {
  entity_resolver{reg, diag}.visit(pipe);
}

} // namespace tenzir::tql2

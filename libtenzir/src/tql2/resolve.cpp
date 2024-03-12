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

class entity_resolver {
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
    auto xyz = reg_.try_get(name);
    if (not xyz) {
      auto category = std::invoke([&] {
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
      diagnostic::error("{} `{}` not found", category, name)
        .primary(x.get_location())
        .emit(diag_);
    }
    // TODO: Check if this entity has right type?
    xyz->match(
      [](const function_def&) {
        // TODO: Methods?
      },
      [](const std::unique_ptr<operator_def>&) {

      });
    x.ref = entity_path{{name}};
  }

  void visit(pipeline& x) {
    for (auto& y : x.body) {
      visit(y);
    }
  }

  void visit(statement& x) {
    x.match([&](auto& y) {
      visit(y);
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

  void visit(assignment& x) {
    visit(x.right);
  }

  void visit(expression& x) {
    x.match([&](auto& y) {
      visit(y);
    });
  }

  void visit(list& x) {
    for (auto& y : x.items) {
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

  void visit(index_expr& x) {
    visit(x.expr);
    visit(x.index);
  }

  void visit(selector& x) {
    (void)x;
  }

  void visit(binary_expr& x) {
    visit(x.left);
    visit(x.right);
  }

  void visit(let_stmt& x) {
    visit(x.expr);
  }

  template <class T>
  void visit(T&) {
    // detail::panic("todo: {}", typeid(T).name());
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

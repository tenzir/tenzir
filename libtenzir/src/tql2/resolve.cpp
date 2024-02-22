//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/resolve.hpp"

#include "tenzir/diagnostics.hpp"
#include "tenzir/tql2/ast.hpp"

namespace tenzir::tql2 {

namespace {

class entity_resolver {
public:
  explicit entity_resolver(diagnostic_handler& diag) : diag{diag} {
  }

  void visit(ast::entity& x) {
    if (context == context_t::op_name && x.path.size() == 1
        && x.path[0].name == "foo") {
      x.id = ast::entity_id{123};
      return;
    }
    if (context == context_t::fn_name && x.path.size() == 1
        && x.path[0].name == "bar") {
      x.id = ast::entity_id{456};
      return;
    }
    auto name = std::invoke([&] {
      switch (context) {
        case context_t::none:
          return "entity";
        case context_t::op_name:
          return "operator";
        case context_t::fn_name:
          return "function";
        case context_t::method_name:
          return "method";
      }
    });
    diagnostic::error("unknown {} name", name).primary(x.location()).emit(diag);
  }

  void visit(ast::pipeline& x) {
    for (auto& y : x.body) {
      visit(y);
    }
  }

  void visit(ast::statement& x) {
    x.match([&](auto& y) {
      visit(y);
    });
  }

  void visit(ast::invocation& x) {
    auto prev = std::exchange(context, context_t::op_name);
    visit(x.op);
    context = prev;
    for (auto& y : x.args) {
      visit(y);
    }
  }

  void visit(ast::assignment& x) {
    visit(x.right);
  }

  void visit(ast::expression& x) {
    x.match([&](auto& y) {
      visit(y);
    });
  }

  void visit(ast::function_call& x) {
    if (x.receiver) {
      visit(*x.receiver);
    }
    auto prev = std::exchange(context, x.receiver ? context_t::method_name
                                                  : context_t::fn_name);
    visit(x.fn);
    context = prev;
    for (auto& y : x.args) {
      visit(y);
    }
  }

  void visit(ast::selector& x) {
    (void)x;
  }

  void visit(ast::binary_expr& x) {
    visit(x.left);
    visit(x.right);
  }

  void visit(ast::integer& x) {
    (void)x;
  }

  template <class T>
  void visit(T&) {
    // detail::panic("todo: {}", typeid(T).name());
  }

private:
  enum class context_t { none, op_name, fn_name, method_name };
  context_t context = context_t::none;
  diagnostic_handler& diag;
};

} // namespace

void resolve_entities(ast::pipeline& pipe, diagnostic_handler& diag) {
  entity_resolver{diag}.visit(pipe);
}

} // namespace tenzir::tql2

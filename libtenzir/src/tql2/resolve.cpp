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

#include <tsl/robin_map.h>

namespace tenzir::tql2 {

namespace {

using namespace ast;

// functions, operators, methods

struct function_def {
  std::string test;
};

struct operator_def {
  std::string test;
};

using entity_def = variant<function_def, operator_def>;

class registry {
public:
  auto add(function_def fn) -> entity_id {
    defs_.emplace_back(std::move(fn));
    return last_id();
  }
  auto add(operator_def op) -> entity_id {
    defs_.emplace_back(std::move(op));
    return last_id();
  }

  auto get(entity_id id) const -> const entity_def& {
    TENZIR_ASSERT(id.id < defs_.size());
    return defs_[id.id];
  }

  auto try_fn(entity_id id) const -> const function_def* {
    return std::get_if<function_def>(&get(id));
  }

private:
  auto last_id() const -> entity_id {
    TENZIR_ASSERT(not defs_.empty());
    return entity_id{defs_.size() - 1};
  }

  // TODO: Lifetimes.
  std::vector<entity_def> defs_;
};

struct name_info {
  entity_id function;
  entity_id operator_;
  entity_id method;
};

class scope {
public:
  void add_fn(std::string name, entity_id id) {
    auto& info = map_[std::move(name)];
    if (info.function.resolved()) {
      detail::panic("function `{}` already declared", name);
    }
    info.function = id;
  }

  void add_op(std::string name, entity_id id) {
    auto& info = map_[std::move(name)];
    if (info.operator_.resolved()) {
      detail::panic("function `{}` already declared", name);
    }
    info.operator_ = id;
  }

  auto get(std::string_view name) const -> name_info {
    // TODO
    auto it = map_.find(std::string{name});
    if (it == map_.end()) {
      return {};
    }
    return it->second;
  }

private:
  tsl::robin_map<std::string, name_info> map_;
};

class entity_resolver {
public:
  explicit entity_resolver(scope& scope, diagnostic_handler& diag)
    : scope_{scope}, diag_{diag} {
  }

  void visit(entity& x) {
    TENZIR_ASSERT(not x.path.empty());
    if (x.path.size() > 1) {
      diagnostic::error("module `{}` not found", x.path[0].name)
        .primary(x.path[0].location)
        .emit(diag_);
      return;
    }
    auto& name = x.path[0].name;
    auto info = scope_.get(name);
    if (context_ == context_t::op_name && info.operator_.resolved()) {
      x.id = info.operator_;
      return;
    }
    if (context_ == context_t::fn_name && info.function.resolved()) {
      x.id = info.function;
      return;
    }
    if (context_ == context_t::method_name && info.method.resolved()) {
      x.id = info.method;
      return;
    }
    auto category = std::invoke([&] {
      switch (context_) {
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
    auto d = diagnostic::error("{} `{}` not found", category, name)
               .primary(x.location());
    if (info.function.resolved()) {
      d = std::move(d).hint("but there exists a function with this name");
    }
    if (info.operator_.resolved()) {
      d = std::move(d).hint("but there exists an operator with this name");
    }
    std::move(d).emit(diag_);
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

  void visit(function_call& x) {
    if (x.receiver) {
      visit(*x.receiver);
    }
    auto prev = std::exchange(context_, x.receiver ? context_t::method_name
                                                   : context_t::fn_name);
    visit(x.fn);
    context_ = prev;
    for (auto& y : x.args) {
      visit(y);
    }
  }

  void visit(selector& x) {
    (void)x;
  }

  void visit(binary_expr& x) {
    visit(x.left);
    visit(x.right);
  }

  void visit(number& x) {
    (void)x;
  }

  template <class T>
  void visit(T&) {
    // detail::panic("todo: {}", typeid(T).name());
  }

private:
  enum class context_t { none, op_name, fn_name, method_name };
  scope scope_;
  diagnostic_handler& diag_;
  context_t context_ = context_t::none;
};

} // namespace

void resolve_entities(ast::pipeline& pipe, diagnostic_handler& diag) {
  auto provider = registry{};
#if 0
  for (auto p : plugins::get<tql_plugin>()) {
    p->entities();
  }
#endif
  auto scope_ = scope{};
  auto sqrt_id = provider.add(function_def{"..."});
  scope_.add_fn("sqrt", sqrt_id);
  auto sort_id = provider.add(operator_def{"..."});
  scope_.add_op("sort", sort_id);
  auto from_id = provider.add(operator_def{"..."});
  scope_.add_op("from", from_id);
  entity_resolver{scope_, diag}.visit(pipe);
}

} // namespace tenzir::tql2

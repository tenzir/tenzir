//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/location.hpp"
#include "tenzir/session.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/plugin.hpp"

#include <functional>

namespace tenzir {

class argument_parser2 {
public:
  argument_parser2() = default;

  explicit argument_parser2(std::string usage) : usage_{std::move(usage)} {
  }

  void add(std::string name, std::optional<located<std::string>>& x) {
    named_.emplace_back(std::move(name), [&x](located<std::string> y) {
      x = std::move(y);
    });
  }

  void add(std::string name, std::optional<ast::expression>& x) {
    named_.emplace_back(std::move(name), [&x](ast::expression y) {
      x = std::move(y);
    });
  }

  void add(std::string name, bool& x) {
    named_.emplace_back(std::move(name), [&x](located<bool> y) {
      x = y.inner;
    });
  }

  void parse(const operator_factory_plugin::invocation& inv, session ctx) {
    auto emit = [&](diagnostic_builder d) {
      // TODO
      TENZIR_ASSERT(inv.self.path.size() == 1);
      auto name = inv.self.path[0].name;
      std::move(d).usage(fmt::format("{} {}", name, usage())).emit(ctx);
    };
    for (auto& arg : inv.args) {
      auto assignment = std::get_if<ast::assignment>(&*arg.kind);
      if (assignment) {
        if (assignment->left.path.size() != 1) {
          emit(diagnostic::error("invalid name")
                 .primary(assignment->left.get_location()));
          return;
        }
        auto& name = assignment->left.path[0].name;
        auto it = std::ranges::find(named_, name, &named::name);
        if (it == named_.end()) {
          emit(diagnostic::error("no such argument `{}`", name)
                 .primary(assignment->left.get_location()));
          return;
        }
        auto& expr = assignment->right;
        it->set.match(
          [&](setter<located<std::string>>& set) {
            auto value = tql2::const_eval(expr, ctx);
            if (not value) {
              return;
            }
            auto string = caf::get_if<std::string>(&*value);
            if (not string) {
              emit(diagnostic::error("expected a string")
                     .primary(expr.get_location()));
              return;
            }
            set(located{std::move(*string), expr.get_location()});
          },
          [&](setter<located<bool>>& set) {
            auto value = tql2::const_eval(expr, ctx);
            if (not value) {
              return;
            }
            auto boolean = caf::get_if<bool>(&*value);
            if (not boolean) {
              emit(diagnostic::error("expected a bool")
                     .primary(expr.get_location()));
              return;
            }
            set(located{boolean, expr.get_location()});
          },
          [&](setter<ast::expression>& set) {
            set(expr.copy());
          });
      } else {
        emit(diagnostic::error("did not expect any positional arguments")
               .primary(arg.get_location()));
      }
    }
  }

  auto usage() const -> std::string {
    if (usage_.empty()) {
      for (auto& [name, set] : named_) {
        if (not usage_.empty()) {
          usage_ += ", ";
        }
        auto meta = set.match(
          [](const setter<located<bool>>&) {
            return "bool";
          },
          [](const setter<ast::expression>&) {
            return "expr";
          },
          [](const setter<located<std::string>>&) {
            return "string";
          });
        usage_ += fmt::format("{}=<{}>", name, meta);
      }
    }
    return usage_;
  }

private:
  template <class T>
  using setter = std::function<void(T)>;

  template <class... Ts>
  using setter_variant = variant<setter<Ts>...>;

  struct named {
    std::string name;
    setter_variant<located<std::string>, ast::expression, located<bool>> set;
  };

  mutable std::string usage_;
  std::vector<named> named_;
};

} // namespace tenzir

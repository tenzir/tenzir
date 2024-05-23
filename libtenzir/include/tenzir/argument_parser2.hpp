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

#include <caf/detail/is_one_of.hpp>

#include <functional>

namespace tenzir {

class argument_parser2 {
public:
  argument_parser2() = default;

  explicit argument_parser2(std::string docs) : docs_{std::move(docs)} {
  }

  auto add(located<std::string>& x, std::string meta) -> argument_parser2& {
    positional_.emplace_back(
      [&x](located<std::string> y) {
        x = std::move(y);
      },
      std::move(meta));
    return *this;
  }

  auto add(std::string name, std::optional<located<std::string>>& x)
    -> argument_parser2& {
    named_.emplace_back(std::move(name), [&x](located<std::string> y) {
      x = std::move(y);
    });
    return *this;
  }

  auto add(std::string name, std::optional<ast::expression>& x)
    -> argument_parser2& {
    named_.emplace_back(std::move(name), [&x](ast::expression y) {
      x = std::move(y);
    });
    return *this;
  }

  auto add(std::string name, std::optional<location>& x) -> argument_parser2& {
    named_.emplace_back(std::move(name), [&x](located<bool> y) {
      if (y.inner) {
        x = y.source;
      } else {
        x = std::nullopt;
      }
    });
    return *this;
  }

  auto add(std::string name, bool& x) -> argument_parser2& {
    named_.emplace_back(std::move(name), [&x](located<bool> y) {
      x = y.inner;
    });
    return *this;
  }

  void parse(const operator_factory_plugin::invocation& inv, session ctx) {
    auto emit = [&](diagnostic_builder d) {
      // TODO
      TENZIR_ASSERT(inv.self.path.size() == 1);
      auto name = inv.self.path[0].name;
      d = std::move(d).usage(fmt::format("{} {}", name, usage()));
      if (not docs_.empty()) {
        d = std::move(d).docs(docs_);
      }
      std::move(d).emit(ctx);
    };
    auto kind = [](const data& x) -> std::string_view {
      // TODO: Refactor this.
      return caf::visit(
        []<class Data>(const Data&) -> std::string_view {
          if constexpr (caf::detail::is_one_of<Data, pattern>::value) {
            TENZIR_UNREACHABLE();
          } else {
            return to_string(type_kind::of<data_to_type_t<Data>>);
          }
        },
        x);
    };
    auto arg = inv.args.begin();
    for (auto& positional : positional_) {
      if (arg == inv.args.end()) {
        emit(diagnostic::error("expected additional positional argument `{}`",
                               positional.meta)
               .primary(inv.self.get_location()));
        break;
      }
      auto& expr = *arg;
      if (std::holds_alternative<ast::assignment>(*expr.kind)) {
        emit(diagnostic::error("expected positional argument `{}` first",
                               positional.meta)
               .primary(expr.get_location()));
        break;
      }
      positional.set.match([&](setter<located<std::string>>& set) {
        auto value = tql2::const_eval(expr, ctx);
        if (not value) {
          return;
        }
        auto string = caf::get_if<std::string>(&*value);
        if (not string) {
          emit(diagnostic::error("expected argument of type `string`, but got "
                                 "`{}`",
                                 kind(value))
                 .primary(expr.get_location()));
          return;
        }
        set(located{std::move(*string), expr.get_location()});
      });
      ++arg;
    }
    for (; arg != inv.args.end(); ++arg) {
      auto assignment = std::get_if<ast::assignment>(&*arg->kind);
      if (not assignment) {
        emit(diagnostic::error("did not expect more positional arguments")
               .primary(arg->get_location()));
        continue;
      }
      auto sel = std::get_if<ast::simple_selector>(&assignment->left);
      if (not sel || sel->has_this() || sel->path().size() != 1) {
        emit(diagnostic::error("invalid name")
               .primary(assignment->left.get_location()));
        continue;
      }
      auto& name = sel->path()[0].name;
      auto it = std::ranges::find(named_, name, &named::name);
      if (it == named_.end()) {
        emit(diagnostic::error("named argument `{}` does not exist", name)
               .primary(assignment->left.get_location()));
        continue;
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
            emit(diagnostic::error("expected argument of type `string`, but "
                                   "got `{}`",
                                   kind(value))
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
            emit(diagnostic::error("expected argument of type `bool`, but "
                                   "got `{}`",
                                   kind(value))
                   .primary(expr.get_location()));
            return;
          }
          set(located{boolean, expr.get_location()});
        },
        [&](setter<ast::expression>& set) {
          set(expr);
        });
    }
  }

  auto usage() const -> std::string {
    if (usage_cache_.empty()) {
      for (auto& positional : positional_) {
        if (not usage_cache_.empty()) {
          usage_cache_ += ", ";
        }
        usage_cache_ += positional.meta;
      }
      for (auto& [name, set] : named_) {
        if (not usage_cache_.empty()) {
          usage_cache_ += ", ";
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
        usage_cache_ += fmt::format("{}=<{}>", name, meta);
      }
    }
    return usage_cache_;
  }

private:
  template <class T>
  using setter = std::function<void(T)>;

  template <class... Ts>
  using setter_variant = variant<setter<Ts>...>;

  struct positional {
    setter_variant<located<std::string>> set;
    std::string meta;
  };

  struct named {
    std::string name;
    setter_variant<located<std::string>, ast::expression, located<bool>> set;
  };

  mutable std::string usage_cache_;
  std::vector<positional> positional_;
  std::vector<named> named_;
  std::string docs_;
};

} // namespace tenzir

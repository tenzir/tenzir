//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/argument_parser2.hpp"

namespace tenzir {

namespace {

template <class T>
concept data_type = detail::tl_contains_v<data::types, T>;

} // namespace

void argument_parser2::parse(const operator_factory_plugin::invocation& inv,
                             session ctx) {
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
    positional.set.match(
      [&]<data_type T>(setter<located<T>>& set) {
        auto value = tql2::const_eval(expr, ctx);
        if (not value) {
          return;
        }
        auto cast = caf::get_if<T>(&*value);
        if (not cast) {
          // TODO
          emit(diagnostic::error("expected argument of type `string`, but got "
                                 "`{}`",
                                 kind(value))
                 .primary(expr.get_location()));
          return;
        }
        set(located{std::move(*cast), expr.get_location()});
      },
      [&](setter<ast::expression>& set) {
        set(expr);
      },
      // [&](setter<located<duration>>& set) {
      //   auto value = tql2::const_eval(expr, ctx);
      //   if (not value) {
      //     return;
      //   }
      //   auto cast = caf::get_if<duration>(&*value);
      //   if (not cast) {
      //     emit(
      //       diagnostic::error("expected argument of type `duration`, but got "
      //                         "`{}`",
      //                         kind(value))
      //         .primary(expr.get_location()));
      //     return;
      //   }
      //   set(located{*cast, expr.get_location()});
      // },
      [&](setter<located<pipeline>>& set) {
        auto pipe_expr = std::get_if<ast::pipeline_expr>(&*expr.kind);
        if (not pipe_expr) {
          emit(diagnostic::error("expected a pipeline expression")
                 .primary(expr.get_location()));
          return;
        }
        auto pipe = tql2::prepare_pipeline(std::move(pipe_expr->inner), ctx);
        set(located{std::move(pipe), expr.get_location()});
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

auto argument_parser2::usage() const -> std::string {
  if (usage_cache_.empty()) {
    for (auto& positional : positional_) {
      if (std::holds_alternative<setter<located<pipeline>>>(positional.set)) {
        usage_cache_ += " { ... }";
        continue;
      }
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

template <argument_parser_any_type T>
auto argument_parser2::add(T& x, std::string meta) -> argument_parser2& {
  if constexpr (argument_parser_bare_type<T>) {
    positional_.emplace_back(
      [&](located<T> y) {
        x = std::move(y.inner);
      },
      std::move(meta));
  } else {
    positional_.emplace_back(
      [&](T y) {
        x = std::move(y);
      },
      std::move(meta));
  }
  return *this;
}

template <argument_parser_any_type T>
auto argument_parser2::add(std::optional<T>& x, std::string meta)
  -> argument_parser2& {
  return *this;
}

template <std::monostate>
struct instantiate_argument_parser_add {
  template <class T>
  using func_type
    = auto (argument_parser2::*)(T&, std::string) -> argument_parser2&;

  template <class... T>
  struct inner {
    static constexpr auto value
      = std::tuple{static_cast<func_type<T>>(&argument_parser2::add)...};
  };

  static constexpr auto value = detail::tl_apply_t<
    detail::tl_concat_t<argument_parser_types, argument_parser_bare_types>,
    inner>::value;
};

template struct instantiate_argument_parser_add<std::monostate{}>;

} // namespace tenzir

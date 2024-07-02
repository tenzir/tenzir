//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/ast.hpp"

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>
#include <caf/detail/type_list.hpp>

#include <type_traits>

namespace tenzir::ast {

auto simple_selector::try_from(ast::expression expr)
  -> std::optional<simple_selector> {
  // Path is collect in reversed order (outside-in).
  auto has_this = false;
  auto path = std::vector<identifier>{};
  auto current = static_cast<ast::expression*>(&expr);
  while (true) {
    auto sub_result = current->match(
      [&](ast::this_&) -> variant<ast::expression*, bool> {
        has_this = true;
        return true;
      },
      [&](ast::root_field& x) -> variant<ast::expression*, bool> {
        path.push_back(x.ident);
        return true;
      },
      [&](ast::field_access& e) -> variant<ast::expression*, bool> {
        path.push_back(e.name);
        return &e.left;
      },
      [&](ast::index_expr& e) -> variant<ast::expression*, bool> {
        auto constant = std::get_if<ast::constant>(&*e.index.kind);
        if (not constant) {
          return false;
        }
        if (auto name = std::get_if<std::string>(&constant->value)) {
          path.emplace_back(*name, constant->source);
          return &e.expr;
        }
        return false;
      },
      [](auto&) -> variant<ast::expression*, bool> {
        return false;
      });
    if (auto success = std::get_if<bool>(&sub_result)) {
      if (not *success) {
        return {};
      }
      std::ranges::reverse(path);
      return simple_selector{std::move(expr), has_this, std::move(path)};
    }
    current = std::get<ast::expression*>(sub_result);
  }
}

auto selector::try_from(ast::expression expr) -> std::optional<selector> {
  return expr.match(
    [](ast::meta& x) -> std::optional<selector> {
      return selector{x};
    },
    [&](auto&) -> std::optional<selector> {
      return simple_selector::try_from(std::move(expr));
    });
}

auto expression::get_location() const -> location {
  return match([](const auto& x) {
    return x.get_location();
  });
}

expression::expression(expression const& other) {
  static_assert(caf::detail::tl_empty<caf::detail::tl_filter_not_t<
                  expression_kinds, std::is_copy_constructible>>::value);
  if (other.kind) {
    kind = std::make_unique<expression_kind>(*other.kind);
  } else {
    kind = nullptr;
  }
}

auto expression::operator=(expression const& other) -> expression& {
  if (this != &other) {
    *this = expression{other};
  }
  return *this;
}

} // namespace tenzir::ast

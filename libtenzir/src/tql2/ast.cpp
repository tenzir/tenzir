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

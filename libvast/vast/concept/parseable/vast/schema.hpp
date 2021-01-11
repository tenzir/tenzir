/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include "vast/concept/parseable/core/operators.hpp"
#include "vast/concept/parseable/core/parser.hpp"
#include "vast/concept/parseable/string.hpp"
#include "vast/concept/parseable/vast/identifier.hpp"
#include "vast/concept/parseable/vast/type.hpp"
#include "vast/schema.hpp"
#include "vast/type.hpp"

namespace vast {

struct schema_parser : parser<schema_parser> {
  using attribute = schema;

  /// The parser for an identifier.
  // clang-format off
  static constexpr auto id
    = +( parsers::alnum
       | parsers::ch<'_'>
       | parsers::ch<'.'>
       );
  // clang-format on

  static constexpr auto skp = type_parser::skp;

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, schema& sch) const {
    type_table symbols;
    auto to_type = [&](std::tuple<std::string, type> t) -> type {
      auto& [name, ty] = t;
      // If the type has already a name, we're dealing with a symbol and have
      // to create an alias.
      if (!ty.name().empty())
        ty = alias_type{ty}; // TODO: attributes
      ty.name(name);
      symbols.add(name, ty); // FIXME: check return value
      return ty;
    };
    // We can't use & because the operand is a parser, and our DSL overloads &.
    auto tp = type_parser{std::addressof(symbols)};
    // clang-format off
    auto decl = ("type" >> skp >> id >> skp >> '=' >> skp >> tp) ->* to_type;
    // clang-format on
    auto declarations = +(skp >> decl) >> skp;
    std::vector<type> v;
    if (!declarations(f, l, v))
      return false;
    sch.clear();
    for (auto& t : v)
      if (!sch.add(t))
        return false;
    return true;
  }
};

template <>
struct parser_registry<schema> {
  using type = schema_parser;
};

namespace parsers {

constexpr auto schema = schema_parser{};

} // namespace parsers
} // namespace vast

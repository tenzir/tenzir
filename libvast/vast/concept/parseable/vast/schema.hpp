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

#include "vast/concept/parseable/core/parser.hpp"
#include "vast/concept/parseable/core/operators.hpp"
#include "vast/concept/parseable/vast/identifier.hpp"
#include "vast/concept/parseable/vast/type.hpp"
#include "vast/type.hpp"
#include "vast/schema.hpp"

namespace vast {

struct schema_parser : parser<schema_parser> {
  using attribute = schema;

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, schema& sch) const {
    type_table symbols;
    auto to_type = [&](std::tuple<std::string, type> t) -> type {
      auto& name = std::get<0>(t);
      auto& ty = std::get<1>(t);
      // If the type has already a name, we're dealing with a symbol and have
      // to create an alias.
      if (!ty.name().empty())
        ty = alias_type{ty}; // TODO: attributes
      ty = ty.name(name);
      symbols.add(name, ty);
      return ty;
    };
    using parsers::alnum;
    using parsers::chr;
    auto id = +(alnum | chr{'_'} | chr{'-'} | chr{':'});
    auto ws = ignore(*parsers::space);
    auto tp = type_parser{std::addressof(symbols)};
    auto decl = ("type" >> ws >> id >> ws >> '=' >> ws >> tp) ->* to_type;
    auto declarations = +(ws >> decl);
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

static auto const schema = make_parser<vast::schema>();

} // namespace parsers
} // namespace vast


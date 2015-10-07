#ifndef VAST_CONCEPT_PARSEABLE_VAST_SCHEMA_H
#define VAST_CONCEPT_PARSEABLE_VAST_SCHEMA_H

#include "vast/type.h"
#include "vast/schema.h"

#include "vast/concept/parseable/core/parser.h"
#include "vast/concept/parseable/core/operators.h"
#include "vast/concept/parseable/vast/identifier.h"
#include "vast/concept/parseable/vast/type.h"

namespace vast {

struct schema_parser : parser<schema_parser> {
  using attribute = schema;

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, schema& sch) const {
    type_table symbols;
    auto to_type = [&](std::tuple<std::string, type> t) -> type {
      auto& name = std::get<0>(t);
      auto& ty = std::get<1>(t);
      // If the type has already a name, we're dealing with a symbol and have
      // to create an alias.
      if (!ty.name().empty())
        ty = type::alias{ty}; // TODO: attributes
      ty.name(name);
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
    if (!declarations.parse(f, l, v))
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

#endif

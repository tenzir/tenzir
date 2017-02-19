#ifndef VAST_CONCEPT_PARSEABLE_VAST_JSON_HPP
#define VAST_CONCEPT_PARSEABLE_VAST_JSON_HPP

#include "vast/json.hpp"
#include "vast/concept/parseable/core.hpp"
#include "vast/concept/parseable/string/char_class.hpp"
#include "vast/concept/parseable/string/quoted_string.hpp"
#include "vast/concept/parseable/numeric/bool.hpp"
#include "vast/concept/parseable/numeric/real.hpp"

namespace vast {

struct json_parser : parser<json_parser> {
  using attribute = json;

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, json& x) const {
    using namespace parsers;
    rule<Iterator, json> j;
    auto ws = ignore(*parsers::space);
    auto lbracket = ws >> '[' >> ws;
    auto rbracket = ws >> ']' >> ws;
    auto lbrace = ws >> '{' >> ws;
    auto rbrace = ws >> '}' >> ws;
    auto delim = ws >> ',' >> ws;
    auto null = ws >> "null"_p ->* [] { return nil; };
    auto true_false = ws >> parsers::boolean;
    auto string = ws >> parsers::qq_str;
    auto number = ws >> parsers::real_opt_dot;
    auto array = as<json::array>(lbracket >> ~(j % delim) >> rbracket);
    auto key_value = ws >> string >> ws >> ':' >> ws >> j;
    auto object = as<json::object>(lbrace >> ~(key_value % delim) >> rbrace);
    j = null
      | true_false
      | number
      | string
      | array
      | object
      ;
    return j(f, l, x);
  }
};

template <>
struct parser_registry<json> {
  using type = json_parser;
};

namespace parsers {

static auto const json = make_parser<vast::json>();

} // namespace parsers

} // namespace vast

#endif

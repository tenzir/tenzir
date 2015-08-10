#ifndef VAST_CONCEPT_PARSEABLE_VAST_JSON_H
#define VAST_CONCEPT_PARSEABLE_VAST_JSON_H

#include "vast/json.h"
#include "vast/concept/parseable/core.h"
#include "vast/concept/parseable/string/char_class.h"
#include "vast/concept/parseable/string/quoted_string.h"
#include "vast/concept/parseable/numeric/bool.h"
#include "vast/concept/parseable/numeric/real.h"

namespace vast {

struct json_parser : parser<json_parser> {
  using attribute = json;

  template <typename Iterator>
  static auto make() {
    auto to_array = [](optional<std::vector<json>> v) {
      return v ? json::array(std::move(*v)) : json::array{};
    };
    using key_value_pair = std::tuple<std::string, json>;
    auto to_object = [](optional<std::vector<key_value_pair>> m) {
      json::object o;
      if (!m)
        return o;
      for (auto& p : *m)
        o.emplace(std::move(std::get<0>(p)), std::move(std::get<1>(p)));
      return o;
    };
    rule<Iterator, json> j;
    auto ws = ignore(*parsers::space);
    auto lbracket = ws >> '[' >> ws;
    auto rbracket = ws >> ']' >> ws;
    auto lbrace = ws >> '{' >> ws;
    auto rbrace = ws >> '}' >> ws;
    auto delim = ws >> ',' >> ws;
    auto null = ws >> parsers::lit("null") ->* [] { return nil; };
    auto true_false = ws >> parsers::boolean;
    auto string = ws >> parsers::qq_str;
    auto number = ws >> parsers::real_opt_dot;
    auto array = (lbracket >> -(j % delim) >> rbracket) ->* to_array;
    auto key_value = ws >> string >> ws >> ':' >> ws >> j;
    auto object = (lbrace >> -(key_value % delim) >> rbrace) ->* to_object;
    j = null
      | true_false
      | number
      | string
      | array
      | object
      ;
    return j;
  }

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, json& j) const {
    using namespace parsers;
    static auto p = make<Iterator>();
    return p.parse(f, l, j);
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

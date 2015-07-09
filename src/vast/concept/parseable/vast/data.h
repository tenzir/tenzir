#ifndef VAST_CONCEPT_PARSEABLE_VAST_DATA_H
#define VAST_CONCEPT_PARSEABLE_VAST_DATA_H

#include "vast/data.h"

#include "vast/concept/parseable/core/parser.h"
#include "vast/concept/parseable/core/rule.h"
#include "vast/concept/parseable/numeric.h"
#include "vast/concept/parseable/string/quoted_string.h"
#include "vast/concept/parseable/vast/address.h"
#include "vast/concept/parseable/vast/pattern.h"
#include "vast/concept/parseable/vast/port.h"
#include "vast/concept/parseable/vast/subnet.h"
#include "vast/concept/parseable/vast/time.h"

namespace vast {

template <>
struct access::parser<data> : vast::parser<access::parser<data>>
{
  using attribute = data;

  template <typename Iterator>
  static auto make()
  {
    auto to_record = [](std::vector<data>&& v) { return record{std::move(v)}; };
    auto to_vector = [](std::vector<data>&& v) { return vector{std::move(v)}; };
    auto to_set = [](std::vector<data>&& v) { return set{v}; };
    auto to_table =
      [](std::vector<std::tuple<data, data>>&& v) -> table
      {
        table t;
        for (auto& x : v)
          t.emplace(std::move(get<0>(x)), std::move(get<1>(x)));
        return t;
      };
    rule<Iterator, data> p;
    p = make_parser<time::point>()
      | make_parser<time::duration>()
      | make_parser<subnet>()
      | make_parser<port>()
      | make_parser<address>()
      | parsers::real
      | parsers::u64
      | parsers::i64
      | parsers::tf
      | parsers::qq_str
      | make_parser<pattern>()
      | '(' >> (p % ',') ->* to_record >> ')'
      | '[' >> (p % ',') ->* to_vector >> ']'
      | '{' >> (p % ',') ->* to_set >> '}'
      | '{' >> ((p >> "->" >> p) % ',') ->* to_table >> '}'
      | literal("none") ->* [] { return nil; }
      ;
    return p;
  }

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, unused_type) const
  {
    static auto p = make<Iterator>();
    return p.parse(f, l, unused);
  }

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, data& a) const
  {
    using namespace parsers;
    static auto p = make<Iterator>();
    return p.parse(f, l, a);
  }
};

template <>
struct parser_registry<data>
{
  using type = access::parser<data>;
};

} // namespace vast

#endif

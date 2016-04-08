#ifndef VAST_CONCEPT_PARSEABLE_VAST_DATA_HPP
#define VAST_CONCEPT_PARSEABLE_VAST_DATA_HPP

#include "vast/data.hpp"

#include "vast/concept/parseable/core/parser.hpp"
#include "vast/concept/parseable/core/rule.hpp"
#include "vast/concept/parseable/numeric.hpp"
#include "vast/concept/parseable/string/quoted_string.hpp"
#include "vast/concept/parseable/vast/address.hpp"
#include "vast/concept/parseable/vast/pattern.hpp"
#include "vast/concept/parseable/vast/port.hpp"
#include "vast/concept/parseable/vast/subnet.hpp"
#include "vast/concept/parseable/vast/time.hpp"

namespace vast {

template <>
struct access::parser<data> : vast::parser<access::parser<data>> {
  using attribute = data;

  template <typename Iterator>
  static auto make() {
    auto to_record = [](std::vector<data>&& v) { return record{std::move(v)}; };
    auto to_vector = [](std::vector<data>&& v) { return vector{std::move(v)}; };
    auto to_set = [](std::vector<data>&& v) { return set{v}; };
    auto to_table = [](std::vector<std::tuple<data, data>>&& v) -> table {
      table t;
      for (auto& x : v)
        t.emplace(std::move(get<0>(x)), std::move(get<1>(x)));
      return t;
    };
    rule<Iterator, data> p;
    p = parsers::time_point
      | parsers::time_duration
      | parsers::net
      | parsers::port
      | parsers::addr
      | parsers::real
      | parsers::u64
      | parsers::i64
      | parsers::tf
      | parsers::qq_str
      | parsers::pattern
      | '(' >> (p % ',') ->* to_record >> ')'
      | '[' >> (p % ',') ->* to_vector >> ']'
      | '{' >> (p % ',') ->* to_set >> '}'
      | '{' >> ((p >> "->" >> p) % ',') ->* to_table >> '}'
      | "nil"_p ->* [] { return nil; }
      ;
    return p;
  }

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, unused_type) const {
    static auto p = make<Iterator>();
    return p.parse(f, l, unused);
  }

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, data& a) const {
    using namespace parsers;
    static auto p = make<Iterator>();
    return p.parse(f, l, a);
  }
};

template <>
struct parser_registry<data> {
  using type = access::parser<data>;
};

namespace parsers {

static auto const data = make_parser<vast::data>();

} // namespace parsers
} // namespace vast

#endif

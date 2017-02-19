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
    rule<Iterator, data> p;
    auto ws = ignore(*parsers::space);
    auto x = ws >> p >> ws;
    p = parsers::timespan
      | parsers::timestamp
      | parsers::net
      | parsers::port
      | parsers::addr
      | parsers::real
      | parsers::u64
      | parsers::i64
      | parsers::tf
      | parsers::qq_str
      | parsers::pattern
      | '[' >> (x % ',') >> ']' // default: vector<data>
      | '{' >> as<set>(x % ',') >> '}'
      | '{' >> as<table>((x >> "->" >> x) % ',') >> '}'
      | as<none>("nil"_p)
      ;
    return p;
  }

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, unused_type) const {
    static auto p = make<Iterator>();
    return p(f, l, unused);
  }

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, data& a) const {
    using namespace parsers;
    static auto p = make<Iterator>();
    return p(f, l, a);
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

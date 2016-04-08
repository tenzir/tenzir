#ifndef VAST_CONCEPT_PARSEABLE_VAST_SUBNET_HPP
#define VAST_CONCEPT_PARSEABLE_VAST_SUBNET_HPP

#include "vast/subnet.hpp"

#include "vast/concept/parseable/core/parser.hpp"
#include "vast/concept/parseable/numeric/integral.hpp"
#include "vast/concept/parseable/vast/address.hpp"

namespace vast {

template <>
struct access::parser<subnet> : vast::parser<access::parser<subnet>> {
  using attribute = subnet;

  static auto make() {
    using namespace parsers;
    auto addr = make_parser<address>{};
    auto prefix = u8.with([](auto x) { return x <= 128; });
    return addr >> '/' >> prefix;
  }

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, unused_type) const {
    static auto p = make();
    return p.parse(f, l, unused);
  }

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, subnet& a) const {
    static auto p = make();
    auto t = std::tie(a.network_, a.length_);
    if (!p.parse(f, l, t))
      return false;
    a.initialize();
    return true;
  }
};

template <>
struct parser_registry<subnet> {
  using type = access::parser<subnet>;
};

namespace parsers {

static auto const net = make_parser<vast::subnet>();

} // namespace parsers

} // namespace vast

#endif

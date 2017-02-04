#ifndef VAST_CONCEPT_PARSEABLE_VAST_ENDPOINT_HPP
#define VAST_CONCEPT_PARSEABLE_VAST_ENDPOINT_HPP

#include "vast/endpoint.hpp"
#include "vast/concept/parseable/core.hpp"
#include "vast/concept/parseable/numeric/integral.hpp"
#include "vast/concept/parseable/string/char_class.hpp"

namespace vast {

struct endpoint_parser : parser<endpoint_parser> {
  using attribute = endpoint;

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, endpoint& e) const {
    using namespace parsers;
    using namespace std::string_literals;
    auto hostname = +(alnum | chr{'-'} | chr{'_'} | chr{'.'});
    auto port = ":"_p >> u16;
    auto p
      = (hostname >> ~port)
      | port ->* [](uint16_t x) { return std::make_tuple(""s, x); }
      ;
    return p(f, l, e.host, e.port);
  }
};

template <>
struct parser_registry<endpoint> {
  using type = endpoint_parser;
};

namespace parsers {

static auto const endpoint = make_parser<vast::endpoint>();

} // namespace parsers
} // namespace vast

#endif

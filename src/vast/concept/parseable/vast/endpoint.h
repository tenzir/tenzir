#ifndef VAST_CONCEPT_PARSEABLE_VAST_ENDPOINT_H
#define VAST_CONCEPT_PARSEABLE_VAST_ENDPOINT_H

#include "vast/endpoint.h"
#include "vast/concept/parseable/core.h"
#include "vast/concept/parseable/numeric/integral.h"
#include "vast/concept/parseable/string/char_class.h"

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
    auto t = std::tie(e.host, e.port);
    return p.parse(f, l, t);
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

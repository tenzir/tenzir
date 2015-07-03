#ifndef VAST_CONCEPT_PARSEABLE_VAST_PORT_H
#define VAST_CONCEPT_PARSEABLE_VAST_PORT_H

#include "vast/port.h"

#include "vast/concept/parseable/core/parser.h"
#include "vast/concept/parseable/numeric/integral.h"
#include "vast/concept/parseable/vast/address.h"

namespace vast {

template <>
struct access::parser<port> : vast::parser<access::parser<port>>
{
  using attribute = port;

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, unused_type) const
  {
    using namespace parsers;
    auto p = u16 >> '/' >> (lit("?") | "tcp" | "udp" | "icmp");
    return p.parse(f, l, unused);
  }

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, port& a) const
  {
    using namespace parsers;
    auto p
      =  u16 ->* [&](uint16_t n) {a.number_ = n; }
      >> '/'
      >> ( str{"?"} ->* [&] { a.type_ = port::unknown; }
         | str{"tcp"} ->* [&] { a.type_ = port::tcp; }
         | str{"udp"} ->* [&] { a.type_ = port::udp; }
         | str{"icmp"} ->* [&] { a.type_ = port::icmp; }
         );
    return p.parse(f, l, unused);
  }
};

template <>
struct parser_registry<port>
{
  using type = access::parser<port>;
};

} // namespace vast

#endif

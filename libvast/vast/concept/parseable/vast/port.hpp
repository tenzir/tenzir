/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#ifndef VAST_CONCEPT_PARSEABLE_VAST_PORT_HPP
#define VAST_CONCEPT_PARSEABLE_VAST_PORT_HPP

#include "vast/concept/parseable/core.hpp"
#include "vast/concept/parseable/numeric/integral.hpp"
#include "vast/port.hpp"

namespace vast {

template <>
struct access::parser<port> : vast::parser<access::parser<port>> {
  using attribute = port;

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, unused_type) const {
    using namespace parsers;
    auto p = u16 >> '/' >> ("?"_p | "tcp" | "udp" | "icmp");
    return p(f, l, unused);
  }

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, port& a) const {
    using namespace parsers;
    static auto p
      =  u16
      >> '/'
      >> ( "?"_p ->* [] { return port::unknown; }
         | "tcp"_p ->* [] { return port::tcp; }
         | "udp"_p ->* [] { return port::udp; }
         | "icmp"_p ->* [] { return port::icmp; }
         )
      ;
    return p(f, l, a.number_, a.type_);
  }
};

template <>
struct parser_registry<port> {
  using type = access::parser<port>;
};

namespace parsers {

static auto const port = make_parser<vast::port>();

} // namespace parsers

} // namespace vast

#endif

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

#pragma once

#include "vast/concept/parseable/core.hpp"
#include "vast/concept/parseable/numeric/integral.hpp"
#include "vast/port.hpp"

namespace vast {

struct port_parser : parser<port_parser> {
  using attribute = port;

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, unused_type) const {
    using namespace parsers;
    using namespace parser_literals;
    auto p = u16 >> '/' >> ("?"_p | "tcp" | "udp" | "icmp");
    return p(f, l, unused);
  }

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, port& x) const {
    using namespace parsers;
    using namespace parser_literals;
    static auto p
      =  u16
      >> '/'
      >> ( "?"_p ->* [] { return port::unknown; }
         | "tcp"_p ->* [] { return port::tcp; }
         | "udp"_p ->* [] { return port::udp; }
         | "icmp"_p ->* [] { return port::icmp; }
         )
      ;
    port::number_type n;
    port::port_type t;
    if (!p(f, l, n, t))
      return false;
    x = {n, t};
    return true;
  }
};

template <>
struct parser_registry<port> {
  using type = port_parser;
};

namespace parsers {

static auto const port = port_parser{};

} // namespace parsers

} // namespace vast


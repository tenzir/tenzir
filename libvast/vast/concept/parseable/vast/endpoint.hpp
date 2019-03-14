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

#include <cstdint>
#include <string>

#include "vast/concept/parseable/core.hpp"
#include "vast/concept/parseable/numeric/integral.hpp"
#include "vast/concept/parseable/string/char_class.hpp"
#include "vast/concept/parseable/vast/port.hpp"
#include "vast/endpoint.hpp"

namespace vast {

struct endpoint_parser : parser<endpoint_parser> {
  using attribute = endpoint;

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, endpoint& e) const {
    using namespace parsers;
    using namespace parser_literals;
    auto hostname = +(alnum | chr{'-'} | chr{'_'} | chr{'.'});
    auto host = hostname->*[&](std::string x) { e.host = std::move(x); };
    auto port = (parsers::port->*[&](vast::port x) { e.port = x; })
                | (u16->*[&](uint16_t x) { e.port = vast::port{x}; });
    auto port_part = ':' >> port;
    auto p = (host >> ~port_part) | port_part;
    return p(f, l, unused);
  }
};

template <>
struct parser_registry<endpoint> {
  using type = endpoint_parser;
};

namespace parsers {

auto const endpoint = make_parser<vast::endpoint>();

} // namespace parsers
} // namespace vast

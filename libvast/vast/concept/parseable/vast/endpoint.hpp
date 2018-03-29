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

#include "vast/endpoint.hpp"
#include "vast/concept/parseable/core.hpp"
#include "vast/concept/parseable/numeric/integral.hpp"
#include "vast/concept/parseable/string/char_class.hpp"

namespace vast {

struct endpoint_parser : parser<endpoint_parser> {
  using attribute = endpoint;

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, endpoint& e) const {
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


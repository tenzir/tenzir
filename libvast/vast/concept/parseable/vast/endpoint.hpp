//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/parseable/core.hpp"
#include "vast/concept/parseable/numeric/integral.hpp"
#include "vast/concept/parseable/string/char_class.hpp"
#include "vast/concept/parseable/vast/port.hpp"
#include "vast/endpoint.hpp"

#include <cstdint>
#include <string>

namespace vast {

struct endpoint_parser : parser_base<endpoint_parser> {
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

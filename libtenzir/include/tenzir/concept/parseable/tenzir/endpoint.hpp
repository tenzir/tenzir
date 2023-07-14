//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/parseable/core.hpp"
#include "tenzir/concept/parseable/numeric/integral.hpp"
#include "tenzir/concept/parseable/string/char_class.hpp"
#include "tenzir/concept/parseable/tenzir/port.hpp"
#include "tenzir/endpoint.hpp"

#include <cstdint>
#include <string>

namespace tenzir {

struct endpoint_parser : parser_base<endpoint_parser> {
  using attribute = endpoint;

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, endpoint& e) const {
    using namespace parsers;
    using namespace parser_literals;
    auto hostname = +(alnum | chr{'-'} | chr{'_'} | chr{'.'});
    auto host = hostname->*[&](std::string x) { e.host = std::move(x); };
    auto port = (parsers::port->*
                 [&](tenzir::port x) {
                   e.port = x;
                 })
                | (u16->*[&](uint16_t x) {
                    e.port = tenzir::port{x};
                  });
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

auto const endpoint = make_parser<tenzir::endpoint>();

} // namespace parsers
} // namespace tenzir

//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/parseable/core/parser.hpp"
#include "vast/concept/parseable/numeric/integral.hpp"
#include "vast/concept/parseable/vast/address.hpp"
#include "vast/subnet.hpp"

namespace vast {

struct subnet_parser : vast::parser_base<subnet_parser> {
  using attribute = subnet;

  static auto make() {
    using namespace parsers;
    auto addr = make_parser<address>{};
    auto prefix = u8.with([](auto x) { return x <= 128; });
    return addr >> '/' >> prefix;
  }

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, unused_type) const {
    static auto p = make();
    return p(f, l, unused);
  }

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, subnet& a) const {
    static auto p = make();
    address network;
    uint8_t length;
    if (!p(f, l, network, length))
      return false;
    a = {network, length};
    return true;
  }
};

template <>
struct parser_registry<subnet> {
  using type = subnet_parser;
};

namespace parsers {

static auto const net = make_parser<vast::subnet>();

} // namespace parsers

} // namespace vast

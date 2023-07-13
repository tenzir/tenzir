//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/parseable/core/parser.hpp"
#include "tenzir/concept/parseable/numeric/integral.hpp"
#include "tenzir/concept/parseable/tenzir/ip.hpp"
#include "tenzir/subnet.hpp"

namespace tenzir {

struct subnet_parser : tenzir::parser_base<subnet_parser> {
  using attribute = subnet;

  static auto make() {
    using parsers::ipv4, parsers::ipv6, parsers::u8;
    // clang-format off
    auto v4_prefix = u8.with([](auto x) { return x <= 32; })
                         ->* [](uint8_t x) { return x + 96; };
    auto v6_prefix = u8.with([](auto x) { return x <= 128; });
    // clang-format on
    return (ipv4 >> '/' >> v4_prefix) | (ipv6 >> '/' >> v6_prefix);
  }

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, unused_type) const {
    static auto p = make();
    return p(f, l, unused);
  }

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, subnet& a) const {
    static auto p = make();
    ip network;
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

static auto const net = make_parser<tenzir::subnet>();

} // namespace parsers

} // namespace tenzir

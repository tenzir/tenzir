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
#include "vast/port.hpp"

namespace vast {

struct port_type_parser : parser_base<port_type_parser> {
  using attribute = port_type;

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, unused_type) const {
    using namespace parsers;
    using namespace parser_literals;
    // clang-format off
    auto p
      = ( "?"_p
        | "icmp6"
        | "icmp"
        | "tcp"
        | "udp"
        | "sctp"
        );
    // clang-format on
    return p(f, l, unused);
  }

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, port_type& x) const {
    using namespace parsers;
    using namespace parser_literals;
    // clang-format off
    auto p
      = ( "?"_p ->* [] { return port_type::unknown; }
        | "icmp6"_p ->* [] { return port_type::icmp6; }
        | "icmp"_p ->* [] { return port_type::icmp; }
        | "tcp"_p ->* [] { return port_type::tcp; }
        | "udp"_p ->* [] { return port_type::udp; }
        | "sctp"_p ->* [] { return port_type::sctp; }
        );
    // clang-format on
    return p(f, l, x);
  }
};

template <>
struct parser_registry<port_type> {
  using type = port_type_parser;
};

namespace parsers {

auto const port_type = port_type_parser{};

} // namespace parsers

struct port_parser : parser_base<port_parser> {
  using attribute = port;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& x) const {
    using namespace parsers;
    using namespace parser_literals;
    auto p = u16 >> '/' >> parsers::port_type;
    if constexpr (std::is_same_v<Attribute, unused_type>) {
      return p(f, l, unused);
    } else {
      port::number_type n;
      auto t = port_type::unknown;
      if (!p(f, l, n, t))
        return false;
      x = port{n, t};
      return true;
    }
  }
};

template <>
struct parser_registry<port> {
  using type = port_parser;
};

namespace parsers {

auto const port = port_parser{};

} // namespace parsers

} // namespace vast

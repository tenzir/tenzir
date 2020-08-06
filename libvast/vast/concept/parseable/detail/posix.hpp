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

#include "vast/concept/parseable/core/action.hpp"
#include "vast/concept/parseable/core/choice.hpp"
#include "vast/concept/parseable/core/literal.hpp"
#include "vast/concept/parseable/core/operators.hpp"
#include "vast/concept/parseable/core/parser.hpp"
#include "vast/detail/posix.hpp"

namespace vast {

struct socket_type_parser : parser<socket_type_parser> {
  using attribute = detail::socket_type;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& x) const {
    using namespace parser_literals;
    // clang-format off
    auto p
      = ( "datagram"_p ->* [] { return detail::socket_type::datagram; }
        | "stream"_p ->* [] { return detail::socket_type::stream; }
        | "fd"_p ->* [] { return detail::socket_type::fd; }
        );
    // clang-format on
    return p(f, l, x);
  }
};

template <>
struct parser_registry<detail::socket_type> {
  using type = socket_type_parser;
};

namespace parsers {

auto const socket_type = socket_type_parser{};

} // namespace parsers

} // namespace vast

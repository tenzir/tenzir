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

#ifndef VAST_CONCEPT_PARSEABLE_VAST_SUBNET_HPP
#define VAST_CONCEPT_PARSEABLE_VAST_SUBNET_HPP

#include "vast/subnet.hpp"

#include "vast/concept/parseable/core/parser.hpp"
#include "vast/concept/parseable/numeric/integral.hpp"
#include "vast/concept/parseable/vast/address.hpp"

namespace vast {

template <>
struct access::parser<subnet> : vast::parser<access::parser<subnet>> {
  using attribute = subnet;

  static auto make() {
    using namespace parsers;
    auto addr = make_parser<address>{};
    auto prefix = u8.with([](auto x) { return x <= 128; });
    return addr >> '/' >> prefix;
  }

  template <typename Iterator>
  bool parse(Iterator& f, const Iterator& l, unused_type) const {
    static auto p = make();
    return p(f, l, unused);
  }

  template <typename Iterator>
  bool parse(Iterator& f, const Iterator& l, subnet& a) const {
    static auto p = make();
    if (!p(f, l, a.network_, a.length_))
      return false;
    a.initialize();
    return true;
  }
};

template <>
struct parser_registry<subnet> {
  using type = access::parser<subnet>;
};

namespace parsers {

static auto const net = make_parser<vast::subnet>();

} // namespace parsers

} // namespace vast

#endif

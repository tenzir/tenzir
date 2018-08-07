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

#include <caf/none.hpp>

#include "vast/data.hpp"

#include "vast/concept/parseable/core/parser.hpp"
#include "vast/concept/parseable/core/rule.hpp"
#include "vast/concept/parseable/numeric.hpp"
#include "vast/concept/parseable/string/quoted_string.hpp"
#include "vast/concept/parseable/vast/address.hpp"
#include "vast/concept/parseable/vast/pattern.hpp"
#include "vast/concept/parseable/vast/port.hpp"
#include "vast/concept/parseable/vast/subnet.hpp"
#include "vast/concept/parseable/vast/time.hpp"

namespace vast {

template <>
struct access::parser<data> : vast::parser<access::parser<data>> {
  using attribute = data;

  template <class Iterator>
  static auto make() {
    rule<Iterator, data> p;
    auto ws = ignore(*parsers::space);
    auto x = ws >> p >> ws;
    p = parsers::timespan
      | parsers::timestamp
      | parsers::net
      | parsers::port
      | parsers::addr
      | parsers::real
      | parsers::u64
      | parsers::i64
      | parsers::tf
      | parsers::qq_str
      | parsers::pattern
      | '[' >> (x % ',') >> ']' // default: vector<data>
      | '{' >> as<set>(x % ',') >> '}'
      | '{' >> as<map>((x >> "->" >> x) % ',') >> '}'
      | as<caf::none_t>("nil"_p)
      ;
    return p;
  }

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, unused_type) const {
    static auto p = make<Iterator>();
    return p(f, l, unused);
  }

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, data& a) const {
    using namespace parsers;
    static auto p = make<Iterator>();
    return p(f, l, a);
  }
};

template <>
struct parser_registry<data> {
  using type = access::parser<data>;
};

namespace parsers {

static auto const data = make_parser<vast::data>();

} // namespace parsers
} // namespace vast


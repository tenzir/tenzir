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

struct data_parser : parser<data_parser> {
  using attribute = data;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    auto p = make<Iterator>();
    return p(f, l, a);
  }

private:
  template <class Iterator>
  static auto make() {
    using namespace parser_literals;
    rule<Iterator, data> p;
    auto ws = ignore(*parsers::space);
    auto x = ws >> p >> ws;
    auto kvp = x >> "->" >> x;
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
      | '[' >> ~(x % ',') >> ']'
      | '{' >> ('-' | as<map>(kvp % ',')) >> '}'
      | '{' >> ~as<set>(x % ',') >> '}'
      | as<caf::none_t>("nil"_p)
      ;
    return p;
  }
};

template <>
struct parser_registry<data> {
  using type = data_parser;
};

namespace parsers {

static auto const data = data_parser{};

} // namespace parsers
} // namespace vast

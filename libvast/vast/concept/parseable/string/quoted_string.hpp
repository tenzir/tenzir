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

#include <cassert>
#include <string>

#include "vast/concept/parseable/core.hpp"
#include "vast/concept/parseable/string/char.hpp"
#include "vast/concept/parseable/string/char_class.hpp"

namespace vast {

template <char Quote, char Esc = '\\'>
class quoted_string_parser : public parser<quoted_string_parser<Quote, Esc>> {
public:
  using attribute = std::string;

  quoted_string_parser() = default;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    auto escaped_quote = Esc >> char_parser{Quote};
    auto p = Quote >> +(escaped_quote | (parsers::print - Quote)) >> Quote;
    return p(f, l, a);
  }
};

namespace parsers {

auto const q_str = quoted_string_parser<'\'', '\\'>{};
auto const qq_str = quoted_string_parser<'"', '\\'>{};

} // namespace parsers

template <>
struct parser_registry<std::string> {
  using type = quoted_string_parser<'"', '\\'>;
};

} // namespace vast


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

template <char Quote, char Esc>
class quoted_string_parser : public parser<quoted_string_parser<Quote, Esc>> {
public:
  using attribute = std::string;

  static constexpr auto esc = ignore(parsers::ch<Esc>);
  static constexpr auto quote = ignore(parsers::ch<Quote>);
  static constexpr auto esc_esc = esc >> parsers::ch<Esc>;
  static constexpr auto esc_quote = esc >> parsers::ch<Quote>;
  static constexpr auto str_chr
    = esc_esc | esc_quote | (parsers::print - quote);
  static constexpr auto quoted_str = quote >> *str_chr >> quote;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& x) const {
    return quoted_str(f, l, x);
  }
};

template <>
struct parser_registry<std::string> {
  using type = quoted_string_parser<'"', '\\'>;
};

namespace parsers {

template <char Quote, char Esc>
const auto quoted = quoted_string_parser<Quote, Esc>{};

const auto qstr = quoted<'\'', '\\'>;
const auto qqstr = quoted<'"', '\\'>;

} // namespace parsers
} // namespace vast

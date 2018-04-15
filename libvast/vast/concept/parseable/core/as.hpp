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

#include "vast/concept/parseable/core/parser.hpp"

namespace vast {

/// Casts a parser's attribute to a specific type.
template <class Parser, class Attribute>
class as_parser : public parser<as_parser<Parser, Attribute>> {
public:
  using attribute = Attribute;

  as_parser(Parser p) : parser_{std::move(p)} {
  }

  template <class Iterator, class Attr>
  bool parse(Iterator& f, const Iterator& l, Attr& a) const {
    attribute x;
    if (!parser_(f, l, x))
      return false;
    a = std::move(x);
    return true;
  }

private:
  Parser parser_;
};

template <class Attribute, class Parser>
auto as(Parser&& p)
-> std::enable_if_t<
     is_parser_v<std::decay_t<Parser>>,
     as_parser<std::decay_t<Parser>, Attribute>
   > {
  return as_parser<std::decay_t<Parser>, Attribute>{std::forward<Parser>(p)};
}

} // namespace vast



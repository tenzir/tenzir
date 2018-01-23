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

#ifndef VAST_CONCEPT_PARSEABLE_STRING_ANY_HPP
#define VAST_CONCEPT_PARSEABLE_STRING_ANY_HPP

#include "vast/concept/parseable/core/parser.hpp"

namespace vast {

struct any_parser : public parser<any_parser> {
  using attribute = char;

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    if (f == l)
      return false;
    detail::absorb(a, *f);
    ++f;
    return true;
  }
};

template <>
struct parser_registry<char> {
  using type = any_parser;
};

namespace parsers {

static auto const any = any_parser{};

} // namespace parsers
} // namespace vast

#endif

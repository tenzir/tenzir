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

#ifndef VAST_CONCEPT_PARSEABLE_STRING_CHAR_HPP
#define VAST_CONCEPT_PARSEABLE_STRING_CHAR_HPP

#include "vast/concept/parseable/core/parser.hpp"
#include "vast/concept/parseable/detail/char_helpers.hpp"

namespace vast {

/// Parses a specific character.
class char_parser : public parser<char_parser> {
public:
  using attribute = char;

  char_parser(char c) : c_{c} {
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    if (f == l || *f != c_)
      return false;
    detail::absorb(a, c_);
    ++f;
    return true;
  }

private:
  char c_;
};

namespace parsers {

using chr = char_parser;

} // namespace parsers
} // namespace vast

#endif

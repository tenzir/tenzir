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

#ifndef VAST_CONCEPT_PARSEABLE_STRING_C_STRING_HPP
#define VAST_CONCEPT_PARSEABLE_STRING_C_STRING_HPP

#include "vast/concept/parseable/core/parser.hpp"
#include "vast/detail/assert.hpp"

namespace vast {

class c_string_parser : public parser<c_string_parser> {
public:
  using attribute = const char*;

  c_string_parser(const char* str) : str_{str} {
    VAST_ASSERT(str != nullptr);
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    auto i = f;
    auto p = str_;
    while (*p != '\0')
      if (i == l || *i++ != *p++)
        return false;
    a = str_;
    f = i;
    return true;
  }

private:
  const char* str_;
};

template <>
struct parser_registry<const char*> {
  using type = c_string_parser;
};

} // namespace vast

#endif

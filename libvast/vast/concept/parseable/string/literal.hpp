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

#include <string_view>

namespace vast {

class literal_parser : public parser<literal_parser> {
public:
  using attribute = std::string_view;

  constexpr literal_parser(std::string_view str) : str_{str} {
  }

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, unused_type) const {
    auto i = f;
    auto begin = str_.begin();
    auto end = str_.end();
    while (begin != end)
      if (i == l || *i++ != *begin++)
        return false;
    f = i;
    return true;
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& x) const {
    if (!parse(f, l, unused))
      return false;
    x = Attribute{str_.data(), str_.size()};
    return true;
  }

private:
  std::string_view str_;
};

namespace parsers {

using lit = literal_parser;

} // namespace parsers
} // namespace vast

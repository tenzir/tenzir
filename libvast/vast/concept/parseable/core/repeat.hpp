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

#ifndef VAST_CONCEPT_PARSEABLE_CORE_REPEAT_HPP
#define VAST_CONCEPT_PARSEABLE_CORE_REPEAT_HPP

#include <vector>

#include "vast/concept/parseable/core/parser.hpp"
#include "vast/concept/parseable/detail/container.hpp"

namespace vast {

template <typename Parser, int Min, int Max = Min>
class repeat_parser : public parser<repeat_parser<Parser, Min, Max>> {
  static_assert(Min <= Max, "minimum must be smaller than maximum");

public:
  using container = detail::container<typename Parser::attribute>;
  using attribute = typename container::attribute;

  explicit repeat_parser(Parser p) : parser_{std::move(p)} {
  }

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    if (Max == 0)
      return true; // If we have nothing todo, we're succeeding.
    auto save = f;
    auto i = 0;
    while (i < Max) {
      if (!container::parse(parser_, f, l, a))
        break;
      ++i;
    }
    if (i >= Min)
      return true;
    f = save;
    return false;
  }

private:
  Parser parser_;
};

template <int Min, int Max = Min, typename Parser>
auto repeat(const Parser& p) {
  return repeat_parser<Parser, Min, Max>{p};
}

namespace parsers {

template <int Min, int Max = Min, typename Parser>
auto rep(const Parser& p) {
  return repeat<Min, Max, Parser>(p);
}

} // namespace parsers
} // namespace vast

#endif

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

#include <vector>

#include "vast/concept/parseable/core/parser.hpp"
#include "vast/concept/parseable/detail/container.hpp"

namespace vast {

template <class Lhs, class Rhs>
class list_parser : public parser<list_parser<Lhs, Rhs>> {
public:
  using lhs_attribute = typename Lhs::attribute;
  using rhs_attribute = typename Rhs::attribute;
  using container = detail::container<lhs_attribute>;
  using attribute = typename container::attribute;

  list_parser(Lhs lhs, Rhs rhs) : lhs_{std::move(lhs)}, rhs_{std::move(rhs)} {
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    if (!container::parse(lhs_, f, l, a))
      return false;
    auto save = f;
    while (rhs_(f, l, unused) && container::parse(lhs_, f, l, a))
      save = f;
    f = save;
    return true;
  }

private:
  Lhs lhs_;
  Rhs rhs_;
};

} // namespace vast


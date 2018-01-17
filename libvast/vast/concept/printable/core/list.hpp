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

#ifndef VAST_CONCEPT_PRINTABLE_CORE_LIST_HPP
#define VAST_CONCEPT_PRINTABLE_CORE_LIST_HPP

#include <vector>

#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/support/detail/attr_fold.hpp"

namespace vast {

template <typename Lhs, typename Rhs>
class list_printer : public printer<list_printer<Lhs, Rhs>> {
public:
  using lhs_attribute = typename Lhs::attribute;
  using rhs_attribute = typename Rhs::attribute;
  using attribute =
    typename detail::attr_fold<std::vector<lhs_attribute>>::type;

  list_printer(Lhs lhs, Rhs rhs) : lhs_{std::move(lhs)}, rhs_{std::move(rhs)} {
  }

  template <typename Iterator, typename Attribute>
  bool print(Iterator& out, Attribute const& a) const {
    using std::begin;
    using std::end;
    auto f = begin(a);
    auto l = end(a);
    if (f == l || !lhs_.print(out, *f))
      return false;
    for (++f; f != l; ++f)
      if (!(rhs_.print(out, unused) && lhs_.print(out, *f)))
        return false;
    return true;
  }

private:
  Lhs lhs_;
  Rhs rhs_;
};

} // namespace vast

#endif

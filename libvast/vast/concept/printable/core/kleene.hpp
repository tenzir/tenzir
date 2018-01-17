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

#ifndef VAST_CONCEPT_PRINTABLE_CORE_KLEENE_HPP
#define VAST_CONCEPT_PRINTABLE_CORE_KLEENE_HPP

#include <vector>

#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/support/detail/attr_fold.hpp"

namespace vast {

template <typename Printer>
class kleene_printer : public printer<kleene_printer<Printer>> {
public:
  using inner_attribute = typename Printer::attribute;
  using attribute =
    typename detail::attr_fold<std::vector<inner_attribute>>::type;

  explicit kleene_printer(Printer p) : printer_{std::move(p)} {
  }

  template <typename Iterator, typename Attribute>
  bool print(Iterator& out, Attribute const& a) const {
    using std::begin;
    using std::end;
    auto f = begin(a);
    auto l = end(a);
    for (; f != l; ++f)
      if (!printer_.print(out, *f))
        return false;
    return true;
  }

private:
  Printer printer_;
};

} // namespace vast

#endif

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

#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/support/detail/attr_fold.hpp"

namespace vast {

template <class Printer>
class kleene_printer : public printer<kleene_printer<Printer>> {
public:
  using inner_attribute = typename Printer::attribute;
  using attribute = detail::attr_fold_t<std::vector<inner_attribute>>;

  explicit kleene_printer(Printer p) : printer_{std::move(p)} {
  }

  template <class Iterator, class Attribute>
  bool print(Iterator& out, const Attribute& a) const {
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

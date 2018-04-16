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

#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/support/detail/attr_fold.hpp"
#include "vast/optional.hpp"

namespace vast {

template <class Printer>
class optional_printer : public printer<optional_printer<Printer>> {
public:
  using inner_attribute =
    typename detail::attr_fold<typename Printer::attribute>::type;

  using attribute =
    std::conditional_t<
      std::is_same<inner_attribute, unused_type>{},
      unused_type,
      optional<inner_attribute>
    >;

  explicit optional_printer(Printer p)
    : printer_{std::move(p)} {
  }

  template <class Iterator>
  bool print(Iterator& out, unused_type) const {
    printer_.print(out, unused);
    return true;
  }

  template <class Iterator, class Attribute>
  bool print(Iterator& out, const Attribute& a) const {
    return !a || printer_.print(out, *a);
  }

private:
  Printer printer_;
};

} // namespace vast



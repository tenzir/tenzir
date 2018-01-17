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

#ifndef VAST_CONCEPT_PRINTABLE_CORE_AND_HPP
#define VAST_CONCEPT_PRINTABLE_CORE_AND_HPP

#include "vast/concept/printable/core/printer.hpp"

namespace vast {

template <typename Printer>
class and_printer : public printer<and_printer<Printer>> {
public:
  using attribute = unused_type;

  explicit and_printer(Printer p) : printer_{std::move(p)} {
  }

  template <typename Iterator, typename Attribute>
  bool print(Iterator& out, Attribute const&) const {
    return printer_.print(out, unused);
  }

private:
  Printer printer_;
};

} // namespace vast

#endif



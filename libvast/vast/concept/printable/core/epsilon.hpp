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

#ifndef VAST_CONCEPT_PRINTABLE_CORE_EPSILON_HPP
#define VAST_CONCEPT_PRINTABLE_CORE_EPSILON_HPP

#include "vast/concept/printable/core/printer.hpp"

namespace vast {

class epsilon_printer : public printer<epsilon_printer> {
public:
  using attribute = unused_type;

  template <typename Iterator>
  bool print(Iterator&, unused_type) const {
    return true;
  }
};

namespace printers {

auto const eps = epsilon_printer{};

} // namespace printers
} // namespace vast

#endif


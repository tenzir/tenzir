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

#ifndef VAST_CONCEPT_PRINTABLE_VAST_PATTERN_HPP
#define VAST_CONCEPT_PRINTABLE_VAST_PATTERN_HPP

#include "vast/access.hpp"
#include "vast/pattern.hpp"
#include "vast/concept/printable/core.hpp"
#include "vast/concept/printable/string/any.hpp"
#include "vast/concept/printable/string/string.hpp"

namespace vast {

template <>
struct access::printer<vast::pattern>
  : vast::printer<access::printer<vast::pattern>> {
  using attribute = pattern;

  template <class Iterator>
  bool print(Iterator& out, const pattern& pat) const {
    auto p = '/' << printers::str << '/';
    return p.print(out, pat.str_);
  }
};

template <>
struct printer_registry<pattern> {
  using type = access::printer<pattern>;
};

} // namespace vast

#endif

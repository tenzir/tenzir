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

#ifndef VAST_CONCEPT_PRINTABLE_VAST_ATTRIBUTE_HPP
#define VAST_CONCEPT_PRINTABLE_VAST_ATTRIBUTE_HPP

#include "vast/attribute.hpp"
#include "vast/concept/printable/core.hpp"
#include "vast/concept/printable/string.hpp"

namespace vast {

using namespace std::string_literals;

struct attribute_printer : printer<attribute_printer> {
  using attribute = vast::attribute;

  template <typename Iterator>
  bool print(Iterator& out, vast::attribute const& attr) const {
    using namespace printers;
    auto prepend_eq = [](std::string const& x) { return '=' + x; };
    auto p = '&'_P << str << -(str ->* prepend_eq);
    return p(out, attr.key, attr.value);
  }
};

template <>
struct printer_registry<attribute> {
  using type = attribute_printer;
};

} // namespace vast

#endif

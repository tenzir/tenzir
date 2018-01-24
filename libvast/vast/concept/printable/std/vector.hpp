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

#ifndef VAST_CONCEPT_PRINTABLE_STD_VECTOR_HPP
#define VAST_CONCEPT_PRINTABLE_STD_VECTOR_HPP

#include <vector>

#include "vast/key.hpp"
#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/detail/print_delimited.hpp"

namespace vast {

template <class T>
struct std_vector_printer : printer<std_vector_printer<T>> {
  using attribute = std::vector<T>;

  std_vector_printer(const std::string& delim = ", ") : delim_{delim} {}

  template <class Iterator>
  bool print(Iterator& out, const attribute& a) const {
    return detail::print_delimited(a.begin(), a.end(), out, delim_);
  }

  std::string delim_;
};

template <class T>
struct printer_registry<std::vector<T>> {
  using type = std_vector_printer<T>;
};

} // namespace vast

#endif

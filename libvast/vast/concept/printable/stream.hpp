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

#ifndef VAST_CONCEPT_PRINTABLE_STREAM_HPP
#define VAST_CONCEPT_PRINTABLE_STREAM_HPP

#include <ostream>
#include <type_traits>

#include "vast/concept/printable/print.hpp"

namespace vast {

template <typename Char, typename Traits, typename T>
auto operator<<(std::basic_ostream<Char, Traits>& out, T const& x)
  -> std::enable_if_t<
       is_printable<std::ostreambuf_iterator<Char>, T>::value, decltype(out)
     > {
  using vast::print; // enable ADL
  if (!print(std::ostreambuf_iterator<Char>{out}, x))
    out.setstate(std::ios_base::failbit);
  return out;
}

} // namespace vast

#endif

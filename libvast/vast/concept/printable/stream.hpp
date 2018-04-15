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

#include <ostream>
#include <type_traits>

#include "vast/concept/printable/print.hpp"

namespace vast {

template <class Char, class Traits, class T>
auto operator<<(std::basic_ostream<Char, Traits>& out, const T& x)
  -> std::enable_if_t<
       is_printable_v<std::ostreambuf_iterator<Char>, T>, decltype(out)
     > {
  using vast::print; // enable ADL
  if (!print(std::ostreambuf_iterator<Char>{out}, x))
    out.setstate(std::ios_base::failbit);
  return out;
}

} // namespace vast


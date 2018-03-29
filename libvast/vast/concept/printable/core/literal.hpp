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

#include <cstddef>
#include <string>
#include <type_traits>

#include "vast/concept/printable/string/literal.hpp"

namespace vast {

inline auto operator"" _P(char c) {
  return literal_printer{c};
}

inline auto operator"" _P(const char* str) {
  return literal_printer{str};
}

inline auto operator"" _P(const char* str, size_t size) {
  return literal_printer{{str, size}};
}

inline auto operator"" _P(unsigned long long int x) {
  return literal_printer{x};
}

inline auto operator"" _P(long double x) {
  return literal_printer{x};
}

} // namespace vast


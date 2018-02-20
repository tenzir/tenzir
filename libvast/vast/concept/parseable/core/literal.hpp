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

#ifndef VAST_CONCEPT_PARSEABLE_CORE_LITERAL_HPP
#define VAST_CONCEPT_PARSEABLE_CORE_LITERAL_HPP

#include <cstddef>
#include <type_traits>
#include <string>

#include "vast/concept/parseable/core/ignore.hpp"
#include "vast/concept/parseable/string/char.hpp"
#include "vast/concept/parseable/string/string.hpp"

namespace vast {

inline auto operator"" _p(char c) {
  return ignore(char_parser{c});
}

inline auto operator"" _p(const char* str) {
  return ignore(string_parser{str});
}

inline auto operator"" _p(const char* str, size_t size) {
  return ignore(string_parser{{str, size}});
}

inline auto operator"" _p(unsigned long long int x) {
  return ignore(string_parser{std::to_string(x)});
}

inline auto operator"" _p(long double x) {
  return ignore(string_parser{std::to_string(x)});
}

} // namespace vast

#endif

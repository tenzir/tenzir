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

#include <string>
#include <type_traits>

#include "vast/optional.hpp"
#include "vast/concept/parseable/parse.hpp"

namespace vast {

template <
  class To,
  class Parser = make_parser<To>,
  class Iterator,
  class... Args
>
auto from_string(Iterator begin, Iterator end, Args&&... args)
  -> std::enable_if_t<is_parseable<Iterator, To>::value, optional<To>> {
  optional<To> t{To{}};
  if (Parser{std::forward<Args>(args)...}(begin, end, *t))
    return t;
  return {};
}

template <
  class To,
  class Parser = make_parser<To>,
  class... Args
>
auto from_string(const std::string& str, Args&&... args) {
  auto f = str.begin();
  auto l = str.end();
  return from_string<To, Parser, std::string::const_iterator, Args...>(
    f, l, std::forward<Args>(args)...);
}

template <
  class To,
  class Parser = make_parser<To>,
  size_t N,
  class... Args
>
auto from_string(char const (&str)[N], Args&&... args) {
  auto f = str;
  auto l = str + N - 1; // No NUL byte.
  return from_string<To, Parser, const char*, Args...>(
    f, l, std::forward<Args>(args)...);
}

} // namespace vast


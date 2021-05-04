//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/parseable/parse.hpp"

#include <caf/optional.hpp>

#include <string>
#include <type_traits>

namespace vast {

template <class To, class Parser = make_parser<To>, class Iterator,
          class... Args>
auto from_string(Iterator begin, Iterator end, Args&&... args)
  -> std::enable_if_t<is_parseable_v<Iterator, To>, caf::optional<To>> {
  caf::optional<To> t{To{}};
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


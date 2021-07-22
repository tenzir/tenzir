//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/parseable/parse.hpp"
#include "vast/error.hpp"

#include <caf/expected.hpp>

#include <iterator>
#include <type_traits>

namespace vast {

template <class To, class Iterator>
requires(parseable<Iterator, To>) auto to(Iterator& f, const Iterator& l)
  -> caf::expected<To> {
  caf::expected<To> t{To{}};
  if (!parse(f, l, *t))
    return caf::make_error(ec::parse_error);
  return t;
}

template <class To, class Range>
auto to(Range&& rng) -> caf::expected<To>
requires(parseable<decltype(std::begin(rng)), To>) {
  using std::begin;
  using std::end;
  auto f = begin(rng);
  auto l = end(rng);
  auto res = to<To>(f, l);
  if (res && f != l)
    return caf::make_error(ec::parse_error);
  return res;
}

template <class To, size_t N>
auto to(char const (&str)[N]) {
  auto first = str;
  auto last = str + N - 1; // No NUL byte.
  return to<To>(first, last);
}

} // namespace vast


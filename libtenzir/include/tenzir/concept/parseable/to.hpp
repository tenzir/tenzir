//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/parseable/parse.hpp"
#include "tenzir/error.hpp"

#include <caf/expected.hpp>

#include <iterator>
#include <type_traits>

namespace tenzir {

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
  caf::expected<To> t{To{}};
  if (! parse(f, l, *t)) {
    return caf::make_error(ec::parse_error);
  }
  return t;
}

template <class To, size_t N>
auto to(char const (&str)[N]) -> caf::expected<To> {
  auto first = str;
  auto last = str + N - 1; // No NUL byte.
  caf::expected<To> t{To{}};
  if (! parse(first, last, *t)) {
    return caf::make_error(ec::parse_error);
  }
  return t;
}

} // namespace tenzir

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

#include <vector>

#include "vast/concept/parseable/core/parser.hpp"
#include "vast/concept/parseable/detail/container.hpp"

#include "vast/detail/assert.hpp"

namespace vast {
namespace detail {

template <class Parser, class Iterator, class Attribute>
bool parse_repeat(Parser& p, Iterator& f, const Iterator& l, Attribute& a,
                  int min, int max) {
  if (max == 0)
    return true; // If we have nothing todo, we're succeeding.
  auto save = f;
  auto i = 0;
  while (i < max) {
    if (!container<typename Parser::attribute>::parse(p, f, l, a))
      break;
    ++i;
  }
  if (i >= min)
    return true;
  f = save;
  return false;
}

} // namespace detail

template <class Parser, int Min, int Max = Min>
class static_repeat_parser
  : public parser<static_repeat_parser<Parser, Min, Max>> {
  static_assert(Min <= Max, "minimum must be smaller than maximum");

public:
  using container = detail::container<typename Parser::attribute>;
  using attribute = typename container::attribute;

  explicit static_repeat_parser(Parser p) : parser_{std::move(p)} {
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    return detail::parse_repeat(parser_, f, l, a, Min, Max);
  }

private:
  Parser parser_;
};

template <class Parser, class T>
class dynamic_repeat_parser : public parser<dynamic_repeat_parser<Parser, T>> {
  static_assert(std::is_integral_v<T>, "T must be an an integral type");

public:
  using container = detail::container<typename Parser::attribute>;
  using attribute = typename container::attribute;

  dynamic_repeat_parser(Parser p, const T& min, const T& max)
    : parser_{std::move(p)},
      min_{min},
      max_{max} {
    VAST_ASSERT(min <= max);
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    return detail::parse_repeat(parser_, f, l, a, min_, max_);
  }

private:
  Parser parser_;
  const T& min_;
  const T& max_;
};

template <int Min, int Max = Min, class Parser>
auto repeat(const Parser& p) {
  return static_repeat_parser<Parser, Min, Max>{p};
}

template <class Parser, class T>
auto repeat(const Parser& p, const T& n) {
  return dynamic_repeat_parser<Parser, T>{p, n, n};
}

template <class Parser, class T>
auto repeat(const Parser& p, const T& min, const T& max) {
  return dynamic_repeat_parser<Parser, T>{p, min, max};
}

namespace parsers {

template <int Min, int Max = Min, class Parser>
auto rep(const Parser& p) {
  return repeat<Min, Max>(p);
}

template <class Parser, class T>
auto rep(const Parser& p, const T& n) {
  return repeat(p, n);
}

template <class Parser, class T>
auto rep(const Parser& p, const T& min, const T& max) {
  return repeat(p, min, max);
}

} // namespace parsers
} // namespace vast


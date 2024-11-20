//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/parseable/core/parser.hpp"
#include "tenzir/concept/parseable/detail/container.hpp"
#include "tenzir/concepts.hpp"
#include "tenzir/detail/assert.hpp"

#include <vector>

namespace tenzir {
namespace detail {

template <class Parser, class Iterator, class Attribute, class T, class U>
bool parse_repeat(Parser& p, Iterator& f, const Iterator& l, Attribute& a,
                  T min, U max) {
  if (max == 0) {
    return true; // If we have nothing todo, we're succeeding.
  }
  auto save = f;
  auto i = 0;
  while (i < max) {
    if (!container_t<typename Parser::attribute>::parse(p, f, l, a)) {
      break;
    }
    ++i;
  }
  if (i >= min) {
    return true;
  }
  f = save;
  return false;
}

} // namespace detail

template <class Parser, int Min, int Max = Min>
class static_repeat_parser
  : public parser_base<static_repeat_parser<Parser, Min, Max>> {
  static_assert(Min <= Max, "minimum must be smaller than maximum");

public:
  using container = detail::container_t<typename Parser::attribute>;
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

template <class Parser, std::integral T, std::integral U = T>
class dynamic_repeat_parser
  : public parser_base<dynamic_repeat_parser<Parser, T, U>> {
public:
  using container = detail::container_t<typename Parser::attribute>;
  using attribute = typename container::attribute;

  dynamic_repeat_parser(Parser p, T min, U max)
    : parser_{std::move(p)}, min_{min}, max_{max} {
    TENZIR_ASSERT(min <= max);
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    return detail::parse_repeat(parser_, f, l, a, min_, max_);
  }

private:
  Parser parser_;
  T min_;
  U max_;
};

template <int Min, int Max = Min, class Parser>
auto repeat(const Parser& p) {
  return static_repeat_parser<Parser, Min, Max>{p};
}

template <class Parser, class T>
auto repeat(const Parser& p, T n) {
  return dynamic_repeat_parser<Parser, T>{p, n, n};
}

template <class Parser, class T, class U>
auto repeat(const Parser& p, T min, U max) {
  return dynamic_repeat_parser<Parser, T, U>{p, min, max};
}

namespace parsers {

template <int Min, int Max = Min, class Parser>
auto rep(const Parser& p) {
  return repeat<Min, Max>(p);
}

template <class Parser, class T>
auto rep(const Parser& p, T n) {
  return repeat(p, n);
}

template <class Parser, class T, class U>
auto rep(const Parser& p, T min, U max) {
  return repeat(p, min, max);
}

} // namespace parsers
} // namespace tenzir

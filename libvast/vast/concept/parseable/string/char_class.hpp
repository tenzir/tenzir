//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/parseable/core.hpp"
#include "vast/concept/parseable/detail/char_helpers.hpp"
#include "vast/concept/parseable/string/any.hpp"

#include <cctype>
#include <string>
#include <vector>

namespace vast {

struct alnum_class {};
struct alpha_class {};
struct blank_class {};
struct cntrl_class {};
struct digit_class {};
struct graph_class {};
struct lower_class {};
struct print_class {};
struct punct_class {};
struct space_class {};
struct upper_class {};
struct xdigit_class {};

template <class CharClass>
class char_class_parser : public parser<char_class_parser<CharClass>> {
public:
  using attribute = char;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    if (f == l || !test_char(*f, CharClass{}))
      return false;
    detail::absorb(a, *f);
    ++f;
    return true;
  }

private:
#define VAST_DEFINE_CHAR_TEST(klass, fun)                                      \
  static bool test_char(int c, klass##_class) {                                \
    return std::fun(c);                                                        \
  }

  VAST_DEFINE_CHAR_TEST(alnum, isalnum)
  VAST_DEFINE_CHAR_TEST(alpha, isalpha)
  VAST_DEFINE_CHAR_TEST(blank, isblank)
  VAST_DEFINE_CHAR_TEST(cntrl, iscntrl)
  VAST_DEFINE_CHAR_TEST(digit, isdigit)
  VAST_DEFINE_CHAR_TEST(graph, isgraph)
  VAST_DEFINE_CHAR_TEST(lower, islower)
  VAST_DEFINE_CHAR_TEST(print, isprint)
  VAST_DEFINE_CHAR_TEST(punct, ispunct)
  VAST_DEFINE_CHAR_TEST(space, isspace)
  VAST_DEFINE_CHAR_TEST(upper, isupper)
  VAST_DEFINE_CHAR_TEST(xdigit, isxdigit)

#undef VAST_DEFINE_CHAR_TEST
};

#define VAST_DEFINE_CHAR_CLASS_PARSER(klass)                                   \
  using klass##_parser = char_class_parser<klass##_class>

VAST_DEFINE_CHAR_CLASS_PARSER(alnum);
VAST_DEFINE_CHAR_CLASS_PARSER(alpha);
VAST_DEFINE_CHAR_CLASS_PARSER(blank);
VAST_DEFINE_CHAR_CLASS_PARSER(cntrl);
VAST_DEFINE_CHAR_CLASS_PARSER(digit);
VAST_DEFINE_CHAR_CLASS_PARSER(graph);
VAST_DEFINE_CHAR_CLASS_PARSER(lower);
VAST_DEFINE_CHAR_CLASS_PARSER(print);
VAST_DEFINE_CHAR_CLASS_PARSER(punct);
VAST_DEFINE_CHAR_CLASS_PARSER(space);
VAST_DEFINE_CHAR_CLASS_PARSER(upper);
VAST_DEFINE_CHAR_CLASS_PARSER(xdigit);

#undef VAST_DEFINE_CHAR_CLASS_PARSER

namespace parsers {

auto const alnum = char_class_parser<alnum_class>{};
auto const alpha = char_class_parser<alpha_class>{};
auto const blank = char_class_parser<blank_class>{};
auto const cntrl = char_class_parser<cntrl_class>{};
auto const digit = char_class_parser<digit_class>{};
auto const graph = char_class_parser<graph_class>{};
auto const lower = char_class_parser<lower_class>{};
[[deprecated("Consider using the UTF-8 friendly `printable` "
             "instead.")]] auto const print
  = char_class_parser<print_class>{};
auto const punct = char_class_parser<punct_class>{};
auto const space = char_class_parser<space_class>{};
auto const upper = char_class_parser<upper_class>{};
auto const xdigit = char_class_parser<xdigit_class>{};

// UTF-8 friendly, as it blacklists control characters rather than whitelisting
// printable ASCII characters.
constexpr auto printable = any - cntrl;

} // namespace parsers
} // namespace vast

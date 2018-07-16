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

#include "vast/concept/parseable/from_string.hpp"
#include "vast/concept/parseable/vast/pattern.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/pattern.hpp"
#include "vast/pattern.hpp"

#define SUITE pattern
#include "test.hpp"

using namespace vast;
using namespace std::string_literals;
using namespace std::string_view_literals;

TEST(functionality) {
  std::string str = "1";
  CHECK(pattern("[0-9]").match(str));
  CHECK(!pattern("[^1]").match(str));
  str = "foobarbaz";
  CHECK(pattern("bar").search(str));
  CHECK(!pattern("^bar$").search(str));
  CHECK(pattern("^\\w{3}\\w{3}\\w{3}$").match(str));
  CHECK(pattern::glob("foo*baz").match(str));
  CHECK(pattern::glob("foo???baz").match(str));
  CHECK(pattern::glob(str).match(str));
  str = "Holla die Waldfee!";
  pattern p{"\\w+ die Waldfe{2}."};
  CHECK(p.match(str));
  CHECK(p.search(str));
  p = pattern("(\\w+ )");
  CHECK(!p.match(str));
  CHECK(p.search(str));
}

TEST(composition) {
  auto foo = pattern{"foo"};
  auto bar = pattern{"bar"};
  auto foobar = "^" + foo + bar + "$";
  CHECK(foobar.match("foobar"));
  CHECK(!foobar.match("foo"));
  CHECK(!foobar.match("bar"));
  auto foo_or_bar = foo | bar;
  CHECK(!foo_or_bar.match("foobar"));
  CHECK(foo_or_bar.search("foobar"));
  CHECK(foo_or_bar.match("foo"));
  CHECK(foo_or_bar.match("bar"));
  auto foo_and_bar = foo & bar;
  CHECK(foo_and_bar.search("foobar"));
  CHECK(foo_and_bar.match("foobar"));
  CHECK(!foo_and_bar.match("foo"));
  CHECK(!foo_and_bar.match("bar"));
}

TEST(printable) {
  auto p = pattern("(\\w+ )");
  CHECK_EQUAL(to_string(p), "/(\\w+ )/");
}

TEST(parseable) {
  auto p = make_parser<pattern>{};
  auto str = R"(/^\w{3}\w{3}\w{3}$/)"s;
  auto f = str.begin();
  auto l = str.end();
  pattern pat;
  CHECK(p(f, l, pat));
  CHECK(f == l);
  CHECK(to_string(pat) == str);

  str = R"(/foo\+(bar){2}|"baz"*/)";
  pat = {};
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, pat));
  CHECK(f == l);
  CHECK(to_string(pat) == str);
}

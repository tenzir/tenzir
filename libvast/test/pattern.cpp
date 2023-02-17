//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/concept/parseable/vast/pattern.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/pattern.hpp"
#include "vast/pattern.hpp"
#include "vast/test/test.hpp"

using namespace vast;
using namespace std::string_literals;
using namespace std::string_view_literals;

TEST(functionality) {
  std::string str = "1";
  CHECK(pattern("[0-9]").match(str));
  CHECK(!pattern("[^1]").match(str));
  str = "foobarbaz";
  CHECK(pattern("bar").search(str));
  CHECK(!pattern("bar").search("FOOBARBAZ"));
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

TEST(comparison with string) {
  auto rx = pattern{"foo.*baz"};
  CHECK("foobarbaz"sv == rx);
  CHECK(rx == "foobarbaz"sv);
}

TEST(composition) {
  auto foo = pattern{"foo"};
  auto bar = pattern{"bar"};
  auto foobar = "^" + foo + bar + "$";
  CHECK(foobar.match("foobar"));
  CHECK(!foobar.match("FOOBAR"));
  CHECK(!foobar.match("fooBAR"));
  CHECK(!foobar.match("FOObar"));
  CHECK(!foobar.match("foo"));
  CHECK(!foobar.match("bar"));
  auto foo_or_bar = foo | bar;
  CHECK(!foo_or_bar.match("foobar"));
  CHECK(foo_or_bar.search("foobar"));
  CHECK(foo_or_bar.match("foo"));
  CHECK(foo_or_bar.match("bar"));
  CHECK(!foo_or_bar.match("FOO"));
  CHECK(!foo_or_bar.match("BAR"));
  auto foo_and_bar = foo & bar;
  CHECK(foo_and_bar.search("foobar"));
  CHECK(foo_and_bar.match("foobar"));
  CHECK(!foo_and_bar.search("FOOBAR"));
  CHECK(!foo_and_bar.match("FOOBAR"));
  CHECK(!foo_and_bar.match("foo"));
  CHECK(!foo_and_bar.match("bar"));
}

TEST(case insensitive) {
  auto pat = pattern("bar", true);
  CHECK(pat.search("bar"));
  CHECK(pat.search("BAR"));
  CHECK(pat.search("Bar"));
  CHECK(pat.search("bAr"));
  CHECK(pat.search("baR"));
  CHECK(pat.search("BAr"));
  CHECK(pat.search("bAR"));
  CHECK(pat.match("bar"));
  CHECK(pat.match("BAR"));
  CHECK(pat.match("Bar"));
  CHECK(pat.match("bAr"));
  CHECK(pat.match("baR"));
  CHECK(pat.match("BAr"));
  CHECK(pat.match("bAR"));
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
  CHECK_EQUAL(to_string(pat), str);

  str = R"(/foo\+(bar){2}|"baz"*/)";
  pat = {};
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, pat));
  CHECK(f == l);
  CHECK_EQUAL(to_string(pat), str);

  str = R"(/foobar/i)";
  pat = {};
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, pat));
  CHECK(f == l);
  CHECK_EQUAL(to_string(pat), str);
  CHECK(pat.match("foobar"));
  CHECK(pat.match("FOOBAR"));

  str = R"(/foobar/a)";
  pat = {};
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, pat));
  CHECK(f != l);
  CHECK_EQUAL(to_string(pat), "/foobar/");
  CHECK(pat.match("foobar"));
  CHECK(!pat.match("FOOBAR"));
}

TEST(to pattern) {
  auto p1 = to<pattern>("/test/");
  CHECK(p1);
  CHECK_EQUAL(p1->string(), "test");
  CHECK(!p1->case_insensitive());
  auto p2 = to<pattern>("/test/i");
  CHECK(p2);
  CHECK_EQUAL(p2->string(), "test");
  CHECK(p2->case_insensitive());
}

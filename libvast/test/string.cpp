//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/detail/string.hpp"

#include "vast/test/test.hpp"

using namespace vast;
using namespace detail;

TEST(string byte escaping) {
  // Identities.
  CHECK_EQUAL(byte_escape(""), "");
  CHECK_EQUAL(byte_escape("foo"), "foo");
  CHECK_EQUAL(byte_escape("foo bar"), "foo bar");

  CHECK_EQUAL(byte_escape("foobar", "o"), "f\\o\\obar");

  CHECK_EQUAL(byte_escape("foob\ar"), "foob\\x07r");
  CHECK_EQUAL(byte_escape("foo\tbar"), "foo\\x09bar");
  CHECK_EQUAL(byte_escape("foo\nbar"), "foo\\x0Abar");
  CHECK_EQUAL(byte_escape("foo\r\nbar"), "foo\\x0D\\x0Abar");

  CHECK_EQUAL(byte_unescape("f\\o\\obar"), "foobar");

  CHECK_EQUAL(byte_unescape("foob\\x07r"), "foob\ar");
  CHECK_EQUAL(byte_unescape("foo\\x09bar"), "foo\tbar");
  CHECK_EQUAL(byte_unescape("foo\\x0abar"), "foo\nbar");
  CHECK_EQUAL(byte_unescape("foo\\x0d\\x0abar"), "foo\r\nbar");

  CHECK_EQUAL(byte_escape_all("foo"), "\\x66\\x6F\\x6F");
  CHECK_EQUAL(byte_unescape("\\x66\\x6f\\x6F"), "foo");

  CHECK_EQUAL(byte_unescape("foo\\"), ""); // Invalid '\' at end of string.
}

TEST(JSON string escaping) {
  CHECK_EQUAL(json_escape(""), "\"\"");
  CHECK_EQUAL(json_escape("\r"), "\"\\r\"");
  CHECK_EQUAL(json_escape("\r\n"), "\"\\r\\n\"");
  CHECK_EQUAL(json_escape("\begin"), "\"\\begin\"");
  CHECK_EQUAL(json_escape("end\n"), "\"end\\n\"");

  CHECK_EQUAL(json_unescape("\"\""), "");
  CHECK_EQUAL(json_unescape("\"\\r\""), "\r");
  CHECK_EQUAL(json_unescape("\"\\r\\n\""), "\r\n");
  CHECK_EQUAL(json_unescape("\"\\begin\""), "\begin");
  CHECK_EQUAL(json_unescape("\"end\\n\""), "end\n");
  CHECK_EQUAL(json_unescape("\"end\\\\\""), "end\\");
  CHECK_EQUAL(json_unescape("\"end\\uaaaa\""), "end\\uaaaa");

  CHECK_EQUAL(json_escape("foo\"bar"), "\"foo\\\"bar\"");
  CHECK_EQUAL(json_escape("foo\\bar"), "\"foo\\\\bar\"");
  CHECK_EQUAL(json_escape("foo\bbar"), "\"foo\\bbar\"");
  CHECK_EQUAL(json_escape("foo\fbar"), "\"foo\\fbar\"");
  CHECK_EQUAL(json_escape("foo\rbar"), "\"foo\\rbar\"");
  CHECK_EQUAL(json_escape("foo\nbar"), "\"foo\\nbar\"");
  CHECK_EQUAL(json_escape("foo\tbar"), "\"foo\\tbar\"");
  CHECK_EQUAL(json_escape("foo\xFF\xFF"), "\"foo\xFF\xFF\"");

  CHECK_EQUAL(json_unescape("\"foo\\\"bar\""), "foo\"bar");
  CHECK_EQUAL(json_unescape("\"foo\\\\bar\""), "foo\\bar");
  CHECK_EQUAL(json_unescape("\"foo\\/bar\""), "foo/bar");
  CHECK_EQUAL(json_unescape("\"foo\\bbar\""), "foo\bbar");
  CHECK_EQUAL(json_unescape("\"foo\\fbar\""), "foo\fbar");
  CHECK_EQUAL(json_unescape("\"foo\\rbar\""), "foo\rbar");
  CHECK_EQUAL(json_unescape("\"foo\\nbar\""), "foo\nbar");
  CHECK_EQUAL(json_unescape("\"foo\\tbar\""), "foo\tbar");
  CHECK_EQUAL(json_unescape("\"foo\\u00FF_\\u0033\""), "foo\xFF_\x33");
  CHECK_EQUAL(json_unescape("\"\\u10FF\""), "\\u10FF");
  CHECK_EQUAL(json_unescape("\"\\u01FF\""), "\\u01FF");
  CHECK_EQUAL(json_unescape("\"\\u11FF\""), "\\u11FF");

  // Invalid.
  CHECK_EQUAL(json_unescape("unquoted"), "");
  CHECK_EQUAL(json_unescape("\""), "");
  CHECK_EQUAL(json_unescape("\"invalid \\x escape sequence\""), "");
  CHECK_EQUAL(json_unescape("\"unescaped\"quote\""), "");

  // Registered Sign: ®
  CHECK_EQUAL(json_escape("®"), "\"®\"");
  CHECK_EQUAL(json_unescape("\"\\u00C2\\u00AE\""), "®");
  CHECK_EQUAL(json_unescape("\"®\""), "®");
  CHECK_EQUAL(json_unescape("\"Hello, world!\""), "Hello, world!");
  CHECK_EQUAL(json_unescape("\"Hello®, world!\""), "Hello®, world!");
}

TEST(percent escaping) {
  CHECK_EQUAL(percent_escape(""), "");
  CHECK_EQUAL(percent_unescape(""), "");
  CHECK_EQUAL(percent_escape("ABC"), "ABC");

  CHECK(percent_escape("/f o o/index.html&foo=b@r")
        == "%2Ff%20o%20o%2Findex.html%26foo%3Db%40r");
  CHECK(percent_unescape("/f%20o%20o/index.html&foo=b@r")
        == "/f o o/index.html&foo=b@r");

  CHECK_EQUAL(percent_escape("&text"), "%26text");
  CHECK_EQUAL(percent_unescape("%26text"), "&text");
  CHECK_EQUAL(percent_unescape("text%3C"), "text<");

  auto esc = "%21%2A%27%28%29%3B%3A%40%26%3D%2B%24%2C%2F%3F%23%5B%5D%25%22%20";
  CHECK_EQUAL(percent_escape("!*'();:@&=+$,/?#[]%\" "), esc);
  CHECK_EQUAL(percent_unescape(esc), "!*'();:@&=+$,/?#[]%\" ");
}

TEST(double escaping) {
  CHECK_EQUAL(double_escape("a|b|c", "|"), "a||b||c");
  CHECK_EQUAL(double_escape("a|b|", "|"), "a||b||");
  CHECK_EQUAL(double_escape("|b|c", "|"), "||b||c");
  CHECK_EQUAL(double_escape("a|b|c", "|"), "a||b||c");
  CHECK_EQUAL(double_escape("abc", "|"), "abc");
  CHECK_EQUAL(double_escape("|", "|"), "||");
  CHECK_EQUAL(double_escape("||", "|"), "||||");
  CHECK_EQUAL(double_unescape("||||", "|"), "||");
  CHECK_EQUAL(double_unescape("|||", "|"), "||");
  CHECK_EQUAL(double_unescape("||", "|"), "|");
  CHECK_EQUAL(double_unescape("|", "|"), "|");
}

TEST(splitting) {
  using namespace std::string_literals;
  MESSAGE("split words");
  auto str = "Der Geist, der stets verneint."s;
  auto s = split(str, " ");
  REQUIRE(s.size() == 5);
  CHECK_EQUAL(s[0], "Der");
  CHECK_EQUAL(s[1], "Geist,");
  CHECK_EQUAL(s[2], "der");
  CHECK_EQUAL(s[3], "stets");
  CHECK_EQUAL(s[4], "verneint.");
  MESSAGE("split with invalid delimiter");
  str = "foo";
  s = split("foo", "x");
  REQUIRE(s.size() == 1);
  CHECK_EQUAL(s[0], "foo");
  MESSAGE("split with empty tokens");
  // TODO: it would be more consistent if split considered not only before the
  // first seperator, but also after the last one. But this is not how many
  // split implementations operate.
  str = ",,";
  s = split(str, ",");
  REQUIRE(s.size() == 2);
  CHECK_EQUAL(s[0], "");
  CHECK_EQUAL(s[1], "");
  MESSAGE("split with partially empty tokens");
  str = ",a,b,c,";
  s = split(str, ",");
  REQUIRE(s.size() == 4);
  CHECK_EQUAL(s[0], "");
  CHECK_EQUAL(s[1], "a");
  CHECK_EQUAL(s[2], "b");
  CHECK_EQUAL(s[3], "c");
  MESSAGE("split with escaping");
  str = "a*,b,c";
  s = split(str, ",", "*");
  REQUIRE(s.size() == 2);
  CHECK_EQUAL(s[0], "a*,b");
  CHECK_EQUAL(s[1], "c");
  MESSAGE("split with max splits");
  str = "a,b,c,d,e,f";
  s = split(str, ",", "", 1);
  REQUIRE(s.size() == 2);
  CHECK_EQUAL(s[0], "a");
  CHECK_EQUAL(s[1], "b,c,d,e,f");
  str = "a-b-c*-d";
  MESSAGE("split that includes the delimiter");
  s = split(str, "-", "*", -1, true);
  REQUIRE(s.size() == 5);
  CHECK_EQUAL(s[0], "a");
  CHECK_EQUAL(s[1], "-");
  CHECK_EQUAL(s[2], "b");
  CHECK_EQUAL(s[3], "-");
  CHECK_EQUAL(s[4], "c*-d");
}

TEST(join) {
  std::vector<std::string> xs{"a", "-", "b", "-", "c*-d"};
  CHECK_EQUAL(join(xs, ""), "a-b-c*-d");
  CHECK_EQUAL(join(xs, " "), "a - b - c*-d");
}

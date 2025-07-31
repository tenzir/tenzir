//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/string.hpp"

#include "tenzir/test/test.hpp"

using namespace tenzir;
using namespace detail;

TEST("string byte escaping") {
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

TEST("JSON string escaping") {
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

TEST("control character escaping") {
  CHECK_EQUAL(control_char_escape(""), "");
  CHECK_EQUAL(control_char_escape("\r"), R"(\r)");
  CHECK_EQUAL(control_char_escape("\r\n"), R"(\r\n)");
  CHECK_EQUAL(control_char_escape("\begin"), R"(\begin)");
  CHECK_EQUAL(control_char_escape("end\n"), R"(end\n)");

  CHECK_EQUAL(control_char_escape("foo\"bar"), R"(foo"bar)");
  CHECK_EQUAL(control_char_escape("foo\\bar"), R"(foo\bar)");
  CHECK_EQUAL(control_char_escape("foo\bbar"), R"(foo\bbar)");
  CHECK_EQUAL(control_char_escape("foo\fbar"), R"(foo\fbar)");
  CHECK_EQUAL(control_char_escape("foo\rbar"), R"(foo\rbar)");
  CHECK_EQUAL(control_char_escape("foo\nbar"), R"(foo\nbar)");
  CHECK_EQUAL(control_char_escape("foo\tbar"), R"(foo\tbar)");
  CHECK_EQUAL(control_char_escape("foo\xFF\xFF"), "foo\xFF\xFF");

  // Registered Sign: ®
  CHECK_EQUAL(control_char_escape("®"), R"(®)");
}

TEST("percent escaping") {
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

TEST("double escaping") {
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

TEST("splitting") {
  using namespace std::string_literals;
  MESSAGE("split words");
  auto str = "Der Geist, der stets verneint."s;
  auto s = split(str, " ");
  REQUIRE_EQUAL(s.size(), 5ull);
  CHECK_EQUAL(s[0], "Der");
  CHECK_EQUAL(s[1], "Geist,");
  CHECK_EQUAL(s[2], "der");
  CHECK_EQUAL(s[3], "stets");
  CHECK_EQUAL(s[4], "verneint.");
  MESSAGE("split with invalid delimiter");
  str = "foo";
  s = split("foo", "x");
  REQUIRE_EQUAL(s.size(), 1ull);
  CHECK_EQUAL(s[0], "foo");
  MESSAGE("split with empty input");
  str = "";
  s = split(str, ",");
  REQUIRE_EQUAL(s.size(), 1ull);
  CHECK_EQUAL(s[0], "");
  MESSAGE("split with empty last token");
  str = "a,";
  s = split(str, ",");
  REQUIRE_EQUAL(s.size(), 2ull);
  CHECK_EQUAL(s[0], "a");
  CHECK_EQUAL(s[1], "");
  MESSAGE("split with empty first token");
  str = ",a";
  s = split(str, ",");
  REQUIRE_EQUAL(s.size(), 2ull);
  CHECK_EQUAL(s[0], "");
  CHECK_EQUAL(s[1], "a");
  MESSAGE("split with empty tokens");
  str = ",,";
  s = split(str, ",");
  REQUIRE_EQUAL(s.size(), 3ull);
  CHECK_EQUAL(s[0], "");
  CHECK_EQUAL(s[1], "");
  CHECK_EQUAL(s[2], "");
  MESSAGE("split with partially empty tokens");
  str = ",a,b,c,";
  s = split(str, ",");
  REQUIRE_EQUAL(s.size(), 5ull);
  CHECK_EQUAL(s[0], "");
  CHECK_EQUAL(s[1], "a");
  CHECK_EQUAL(s[2], "b");
  CHECK_EQUAL(s[3], "c");
  CHECK_EQUAL(s[4], "");
  MESSAGE("split with max splits");
  str = "a,b,c,d,e,f";
  s = split(str, ",", 1);
  REQUIRE_EQUAL(s.size(), 2ull);
  CHECK_EQUAL(s[0], "a");
  CHECK_EQUAL(s[1], "b,c,d,e,f");
  MESSAGE("split with correct number of max splits");
  str = "a,b";
  s = split(str, ",", 1);
  REQUIRE_EQUAL(s.size(), 2ull);
  CHECK_EQUAL(s[0], "a");
  CHECK_EQUAL(s[1], "b");
  MESSAGE("split with max split number larger by one");
  str = "a,b";
  s = split(str, ",", 2);
  REQUIRE_EQUAL(s.size(), 2ull);
  CHECK_EQUAL(s[0], "a");
  CHECK_EQUAL(s[1], "b");
  MESSAGE("split with max splits and trailing separator");
  str = "a,b,";
  s = split(str, ",", 1);
  REQUIRE_EQUAL(s.size(), 2ull);
  CHECK_EQUAL(s[0], "a");
  CHECK_EQUAL(s[1], "b,");
  MESSAGE("split with one larger number of max splits and trailing separator");
  str = "a,b,";
  s = split(str, ",", 2);
  REQUIRE_EQUAL(s.size(), 3ull);
  CHECK_EQUAL(s[0], "a");
  CHECK_EQUAL(s[1], "b");
  CHECK_EQUAL(s[2], "");
}

TEST("escaped splitting") {
  using namespace std::string_literals;
  MESSAGE("split with escaping");
  auto str = "a*,b,c"s;
  auto s = split_escaped(str, ",", "*");
  REQUIRE_EQUAL(s.size(), 2ull);
  CHECK_EQUAL(s[0], "a,b");
  CHECK_EQUAL(s[1], "c");
  MESSAGE("escaped split with empty input");
  str = "";
  s = split_escaped(str, ",", "*");
  REQUIRE_EQUAL(s.size(), 1ull);
  CHECK_EQUAL(s[0], "");
  MESSAGE("escaped split with empty input");
  str = "";
  s = split_escaped(str, ",", "*");
  REQUIRE_EQUAL(s.size(), 1ull);
  CHECK_EQUAL(s[0], "");
  MESSAGE("escaped split with empty last token");
  str = "a,";
  s = split_escaped(str, ",", "*");
  REQUIRE_EQUAL(s.size(), 2ull);
  CHECK_EQUAL(s[0], "a");
  CHECK_EQUAL(s[1], "");
  MESSAGE("escaped split with empty first token");
  str = ",a";
  s = split_escaped(str, ",", "*");
  REQUIRE_EQUAL(s.size(), 2ull);
  CHECK_EQUAL(s[0], "");
  CHECK_EQUAL(s[1], "a");
  MESSAGE("escaped split with max splits");
  str = "a,b*,c,d,e,f";
  s = split_escaped(str, ",", "*", 2);
  REQUIRE_EQUAL(s.size(), 3ull);
  CHECK_EQUAL(s[0], "a");
  CHECK_EQUAL(s[1], "b,c");
  CHECK_EQUAL(s[2], "d,e,f");
  MESSAGE("escaped split with correct number of splits");
  str = "a,b*,c,d";
  s = split_escaped(str, ",", "*", 2);
  REQUIRE_EQUAL(s.size(), 3ull);
  CHECK_EQUAL(s[0], "a");
  CHECK_EQUAL(s[1], "b,c");
  CHECK_EQUAL(s[2], "d");
  MESSAGE("escaped split with max split number larger by one");
  str = "a,b*,c,d";
  s = split_escaped(str, ",", "*", 3);
  REQUIRE_EQUAL(s.size(), 3ull);
  CHECK_EQUAL(s[0], "a");
  CHECK_EQUAL(s[1], "b,c");
  CHECK_EQUAL(s[2], "d");
  MESSAGE("escaped split with max splits and trailing separator");
  str = "a*,b,c,";
  s = split_escaped(str, ",", "*", 1);
  REQUIRE_EQUAL(s.size(), 2ull);
  CHECK_EQUAL(s[0], "a,b");
  CHECK_EQUAL(s[1], "c,");
  MESSAGE("escaped split with one larger number of max splits and trailing "
          "separator");
  str = "a*,b,c,";
  s = split_escaped(str, ",", "*", 2);
  REQUIRE_EQUAL(s.size(), 3ull);
  CHECK_EQUAL(s[0], "a,b");
  CHECK_EQUAL(s[1], "c");
  CHECK_EQUAL(s[2], "");
  MESSAGE("escaped split with trailing, possibly escaped separators");
  str = "foo:=@bar";
  s = split_escaped(str, ":=@", "\\", 1);
  REQUIRE_EQUAL(s.size(), 2ull);
  CHECK_EQUAL(s[0], "foo");
  CHECK_EQUAL(s[1], "bar");
  str = "foo:=@bar:=@";
  s = split_escaped(str, ":=@", "\\", 1);
  REQUIRE_EQUAL(s.size(), 2ull);
  CHECK_EQUAL(s[0], "foo");
  CHECK_EQUAL(s[1], "bar:=@");
  str = "foo:=@bar\\:=@";
  s = split_escaped(str, ":=@", "\\", 1);
  REQUIRE_EQUAL(s.size(), 2ull);
  CHECK_EQUAL(s[0], "foo");
  CHECK_EQUAL(s[1], "bar\\:=@");
  str = "foo:=@bar:=@:=@";
  s = split_escaped(str, ":=@", "\\", 1);
  REQUIRE_EQUAL(s.size(), 2ull);
  CHECK_EQUAL(s[0], "foo");
  CHECK_EQUAL(s[1], "bar:=@:=@");
  str = "foo:=@bar\\:=@:=@";
  s = split_escaped(str, ":=@", "\\", 1);
  REQUIRE_EQUAL(s.size(), 2ull);
  CHECK_EQUAL(s[0], "foo");
  CHECK_EQUAL(s[1], "bar\\:=@:=@");
  str = "foo:=@bar:=@\\:=@";
  s = split_escaped(str, ":=@", "\\", 1);
  REQUIRE_EQUAL(s.size(), 2ull);
  CHECK_EQUAL(s[0], "foo");
  CHECK_EQUAL(s[1], "bar:=@\\:=@");
  str = "foo:=@bar\\:=@\\:=@";
  s = split_escaped(str, ":=@", "\\", 1);
  REQUIRE_EQUAL(s.size(), 2ull);
  CHECK_EQUAL(s[0], "foo");
  CHECK_EQUAL(s[1], "bar\\:=@\\:=@");
  str = "foo:=@bar\\:=@\\:=@baz";
  s = split_escaped(str, ":=@", "\\", 1);
  REQUIRE_EQUAL(s.size(), 2ull);
  CHECK_EQUAL(s[0], "foo");
  CHECK_EQUAL(s[1], "bar\\:=@\\:=@baz");
  str = "foo\\:=@bar:=@baz\\:=@\\:=@quux";
  s = split_escaped(str, ":=@", "\\", 1);
  REQUIRE_EQUAL(s.size(), 2ull);
  CHECK_EQUAL(s[0], "foo:=@bar");
  CHECK_EQUAL(s[1], "baz\\:=@\\:=@quux");
  str = "foo:=@bar\\:=@:=@";
  s = split_escaped(str, ":=@", "\\", 2);
  REQUIRE_EQUAL(s.size(), 3ull);
  CHECK_EQUAL(s[0], "foo");
  CHECK_EQUAL(s[1], "bar:=@");
  CHECK_EQUAL(s[2], "");
  str = "foo:=@bar\\:=@:=@baz";
  s = split_escaped(str, ":=@", "\\", 2);
  REQUIRE_EQUAL(s.size(), 3ull);
  CHECK_EQUAL(s[0], "foo");
  CHECK_EQUAL(s[1], "bar:=@");
  CHECK_EQUAL(s[2], "baz");
}

TEST("join") {
  std::vector<std::string> xs{"a", "-", "b", "-", "c*-d"};
  CHECK_EQUAL(join(xs, ""), "a-b-c*-d");
  CHECK_EQUAL(join(xs, " "), "a - b - c*-d");
}

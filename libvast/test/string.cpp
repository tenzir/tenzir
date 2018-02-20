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

#include "vast/detail/string.hpp"

#define SUITE string
#include "test.hpp"

using namespace vast;
using namespace detail;

TEST(string byte escaping) {
  // Identities.
  CHECK(byte_escape("") == "");
  CHECK(byte_escape("foo") == "foo");
  CHECK(byte_escape("foo bar") == "foo bar");

  CHECK(byte_escape("foobar", "o") == "f\\o\\obar");

  CHECK(byte_escape("foob\ar") == "foob\\x07r");
  CHECK(byte_escape("foo\tbar") == "foo\\x09bar");
  CHECK(byte_escape("foo\nbar") == "foo\\x0Abar");
  CHECK(byte_escape("foo\r\nbar") == "foo\\x0D\\x0Abar");

  CHECK(byte_unescape("f\\o\\obar") == "foobar");

  CHECK(byte_unescape("foob\\x07r") == "foob\ar");
  CHECK(byte_unescape("foo\\x09bar") == "foo\tbar");
  CHECK(byte_unescape("foo\\x0abar") == "foo\nbar");
  CHECK(byte_unescape("foo\\x0d\\x0abar") == "foo\r\nbar");

  CHECK(byte_escape_all("foo") == "\\x66\\x6F\\x6F");
  CHECK(byte_unescape("\\x66\\x6f\\x6F") == "foo");

  CHECK(byte_unescape("foo\\") == ""); // Invalid '/' at end of string.
}

TEST(JSON string escaping) {
  CHECK(json_escape("") == "\"\"");
  CHECK(json_escape("\r") == "\"\\r\"");
  CHECK(json_escape("\r\n") == "\"\\r\\n\"");
  CHECK(json_escape("\begin") == "\"\\begin\"");
  CHECK(json_escape("end\n") == "\"end\\n\"");

  CHECK(json_unescape("\"\"") == "");
  CHECK(json_unescape("\"\\r\"") == "\r");
  CHECK(json_unescape("\"\\r\\n\"") == "\r\n");
  CHECK(json_unescape("\"\\begin\"") == "\begin");
  CHECK(json_unescape("\"end\\n\"") == "end\n");
  CHECK(json_unescape("\"end\\uaaaa\"") == "end\\uaaaa");

  CHECK(json_escape("foo\"bar") == "\"foo\\\"bar\"");
  CHECK(json_escape("foo\\bar") == "\"foo\\\\bar\"");
  CHECK(json_escape("foo\bbar") == "\"foo\\bbar\"");
  CHECK(json_escape("foo\fbar") == "\"foo\\fbar\"");
  CHECK(json_escape("foo\rbar") == "\"foo\\rbar\"");
  CHECK(json_escape("foo\nbar") == "\"foo\\nbar\"");
  CHECK(json_escape("foo\tbar") == "\"foo\\tbar\"");
  CHECK(json_escape("foo\xFF\xFF") == "\"foo\\u00FF\\u00FF\"");

  CHECK(json_unescape("\"foo\\\"bar\"") == "foo\"bar");
  CHECK(json_unescape("\"foo\\\\bar\"") == "foo\\bar");
  CHECK(json_unescape("\"foo\\/bar\"")  == "foo/bar");
  CHECK(json_unescape("\"foo\\bbar\"")  == "foo\bbar");
  CHECK(json_unescape("\"foo\\fbar\"")  == "foo\fbar");
  CHECK(json_unescape("\"foo\\rbar\"")  == "foo\rbar");
  CHECK(json_unescape("\"foo\\nbar\"")  == "foo\nbar");
  CHECK(json_unescape("\"foo\\tbar\"")  == "foo\tbar");
  CHECK(json_unescape("\"foo\\u00FF_\\u0033\"") == "foo\xFF_\x33");
  CHECK(json_unescape("\"\\u10FF\"") == "\\u10FF");
  CHECK(json_unescape("\"\\u01FF\"") == "\\u01FF");
  CHECK(json_unescape("\"\\u11FF\"") == "\\u11FF");

  // Invalid.
  CHECK(json_unescape("unquoted") == "");
  CHECK(json_unescape("\"") == "");
  CHECK(json_unescape("\"invalid \\x escape sequence\"") == "");
  CHECK(json_unescape("\"unescaped\"quote\"") == "");
}

TEST(percent escaping) {
  CHECK(percent_escape("") == "");
  CHECK(percent_unescape("") == "");
  CHECK(percent_escape("ABC") == "ABC");

  CHECK(percent_escape("/f o o/index.html&foo=b@r") 
        == "%2Ff%20o%20o%2Findex.html%26foo%3Db%40r");
  CHECK(percent_unescape("/f%20o%20o/index.html&foo=b@r") 
        == "/f o o/index.html&foo=b@r");

  CHECK(percent_escape("&text") == "%26text");
  CHECK(percent_unescape("%26text") == "&text");
  CHECK(percent_unescape("text%3C") == "text<");

  auto esc = "%21%2A%27%28%29%3B%3A%40%26%3D%2B%24%2C%2F%3F%23%5B%5D%25%22%20";
  CHECK(percent_escape("!*'();:@&=+$,/?#[]%\" ") == esc);
  CHECK(percent_unescape(esc) == "!*'();:@&=+$,/?#[]%\" ");
}

TEST(double escaping) {
  CHECK(double_escape("a|b|c", "|") == "a||b||c");
  CHECK(double_escape("a|b|", "|") == "a||b||");
  CHECK(double_escape("|b|c", "|") == "||b||c");
  CHECK(double_escape("a|b|c", "|") == "a||b||c");
  CHECK(double_escape("abc", "|") == "abc");
  CHECK(double_escape("|", "|") == "||");
  CHECK(double_escape("||", "|") == "||||");
  CHECK(double_unescape("||||", "|") == "||");
  CHECK(double_unescape("|||", "|") == "||");
  CHECK(double_unescape("||", "|") == "|");
  CHECK(double_unescape("|", "|") == "|");
}

TEST(string splitting and joining) {
  using namespace std::string_literals;

  auto s = split_to_str("Der Geist, der stets verneint."s, " ");
  REQUIRE(s.size() == 5);
  CHECK(s[0] == "Der");
  CHECK(s[1] == "Geist,");
  CHECK(s[2] == "der");
  CHECK(s[3] == "stets");
  CHECK(s[4] == "verneint.");

  s = split_to_str("foo"s, "x");
  REQUIRE(s.size() == 1);
  CHECK(s[0] == "foo");

  // TODO: it would be more consistent if split considered not only before the
  // first seperator, but also after the last one. But this is not how many
  // split implementations operate.
  s = split_to_str(",,"s, ",");
  REQUIRE(s.size() == 2);
  CHECK(s[0] == "");
  CHECK(s[1] == "");

  s = split_to_str(",a,b,c,"s, ",");
  REQUIRE(s.size() == 4);
  CHECK(s[0] == "");
  CHECK(s[1] == "a");
  CHECK(s[2] == "b");
  CHECK(s[3] == "c");

  s = split_to_str("a*,b,c"s, ",", "*");
  REQUIRE(s.size() == 2);
  CHECK(s[0] == "a*,b");
  CHECK(s[1] == "c");

  s = split_to_str("a,b,c,d,e,f"s, ",", "", 1);
  REQUIRE(s.size() == 2);
  CHECK(s[0] == "a");
  CHECK(s[1] == "b,c,d,e,f");

  s = split_to_str("a-b-c*-d"s, "-", "*", -1, true);
  REQUIRE(s.size() == 5);
  CHECK(s[0] == "a");
  CHECK(s[1] == "-");
  CHECK(s[2] == "b");
  CHECK(s[3] == "-");
  CHECK(s[4] == "c*-d");

  auto str = join(s, "");
  CHECK(str == "a-b-c*-d");
  str = join(s, " ");
  CHECK(str == "a - b - c*-d");
}

TEST(boyer-moore) {
  std::string needle = "foo";
  std::string haystack = "hello foo world";

  // Function-object API.
  auto bm = make_boyer_moore(needle.begin(), needle.end());
  auto i = bm(haystack.begin(), haystack.end());
  REQUIRE(i != haystack.end());

  // Free-function API
  auto j = search_boyer_moore(needle.begin(), needle.end(),
                              haystack.begin(), haystack.end());
  REQUIRE(j != haystack.end());
  REQUIRE(i == j);
  CHECK(needle == std::string(i, i + needle.size()));

  haystack = "Da steh ich nun, ich armer Tor! Und bin so klug als wie zuvor";
  needle = "ich";
  bm = make_boyer_moore(needle.begin(), needle.end());
  for (size_t i = 0; i < 9; ++i)
    CHECK((bm(haystack.begin() + i, haystack.end()) - haystack.begin()) == 8);
  for (size_t i = 9; i < 18; ++i)
    CHECK((bm(haystack.begin() + i, haystack.end()) - haystack.begin()) == 17);
  for (size_t i = 18; i < haystack.size() - needle.size(); ++i)
    CHECK(bm(haystack.begin() + i, haystack.end()) == haystack.end());
}

TEST(knuth-morris-pratt) {
  std::string needle = "foo";
  std::string haystack = "hello foo world";

  // Function-object API.
  auto kmp = make_knuth_morris_pratt(needle.begin(), needle.end());
  auto i = kmp(haystack.begin(), haystack.end());
  REQUIRE(i != haystack.end());

  // Free-function API
  auto j = search_knuth_morris_pratt(needle.begin(), needle.end(),
                                     haystack.begin(), haystack.end());
  REQUIRE(j != haystack.end());
  REQUIRE(i == j);
  CHECK(needle == std::string(i, i + needle.size()));

  haystack = "Da steh ich nun, ich armer Tor! Und bin so klug als wie zuvor";
  needle = "ich";
  kmp = make_knuth_morris_pratt(needle.begin(), needle.end());
  for (size_t i = 0; i < 9; ++i)
    CHECK((kmp(haystack.begin() + i, haystack.end()) - haystack.begin()) == 8);
  for (size_t i = 9; i < 18; ++i)
    CHECK((kmp(haystack.begin() + i, haystack.end()) - haystack.begin()) == 17);
  for (size_t i = 18; i < haystack.size() - needle.size(); ++i)
    CHECK(kmp(haystack.begin() + i, haystack.end()) == haystack.end());
}

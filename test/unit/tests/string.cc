#include "framework/unit.h"
#include "vast/util/string.h"

using namespace vast;
using namespace util;

SUITE("util")

TEST("string byte escaping")
{
  // Identities.
  CHECK(byte_escape("") == "");
  CHECK(byte_escape("foo") == "foo");
  CHECK(byte_escape("foo bar") == "foo bar");

  CHECK(byte_escape("foob\ar") == "foob\\x07r");
  CHECK(byte_escape("foo\tbar") == "foo\\x09bar");
  CHECK(byte_escape("foo\nbar") == "foo\\x0abar");
  CHECK(byte_escape("foo\r\nbar") == "foo\\x0d\\x0abar");

  CHECK(byte_unescape("foob\\x07r") == "foob\ar");
  CHECK(byte_unescape("foo\\x09bar") == "foo\tbar");
  CHECK(byte_unescape("foo\\x0abar") == "foo\nbar");
  CHECK(byte_unescape("foo\\x0d\\x0abar") == "foo\r\nbar");

  CHECK(byte_escape("foo", true) == "\\x66\\x6f\\x6f");
  CHECK(byte_unescape("\\x66\\x6f\\x6f") == "foo");
}

TEST("JSON string escaping")
{
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
  CHECK(json_unescape("\"end\uaaaa\"")  == "end\uaaaa");

  CHECK(json_escape("foo\"bar") == "\"foo\\\"bar\"");
  CHECK(json_escape("foo\\bar") == "\"foo\\\\bar\"");
  CHECK(json_escape("foo/bar")  == "\"foo\\/bar\"");
  CHECK(json_escape("foo\bbar") == "\"foo\\bbar\"");
  CHECK(json_escape("foo\fbar") == "\"foo\\fbar\"");
  CHECK(json_escape("foo\rbar") == "\"foo\\rbar\"");
  CHECK(json_escape("foo\nbar") == "\"foo\\nbar\"");
  CHECK(json_escape("foo\tbar") == "\"foo\\tbar\"");

  CHECK(json_unescape("\"foo\\\"bar\"") == "foo\"bar");
  CHECK(json_unescape("\"foo\\\\bar\"") == "foo\\bar");
  CHECK(json_unescape("\"foo\\/bar\"")  == "foo/bar");
  CHECK(json_unescape("\"foo\\bbar\"")  == "foo\bbar");
  CHECK(json_unescape("\"foo\\fbar\"")  == "foo\fbar");
  CHECK(json_unescape("\"foo\\rbar\"")  == "foo\rbar");
  CHECK(json_unescape("\"foo\\nbar\"")  == "foo\nbar");
  CHECK(json_unescape("\"foo\\tbar\"")  == "foo\tbar");
  CHECK(json_unescape("\"foo\uaaaabar\"")  == "foo\uaaaabar");

  // Invalid.
  CHECK(json_unescape("unquoted") == "");
  CHECK(json_unescape("\"") == "");
  CHECK(json_unescape("\"invalid \\x escape sequence\"") == "");
  CHECK(json_unescape("\"unescaped\"quote\"") == "");
}

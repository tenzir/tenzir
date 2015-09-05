#include <sstream>

#include "vast/concept/parseable/core.h"
#include "vast/concept/parseable/numeric.h"
#include "vast/concept/parseable/string.h"
#include "vast/concept/parseable/stream.h"
#include "vast/concept/parseable/to.h"
#include "vast/concept/parseable/vast/key.h"

#define SUITE parseable
#include "test.h"

using namespace vast;
using namespace std::string_literals;

TEST(container attribute folding) {
  using namespace parsers;
  auto spaces = *' '_p;
  static_assert(std::is_same<decltype(spaces)::attribute, unused_type>::value,
                "container attribute folding failed");
}

TEST(char) {
  using namespace parsers;
  MESSAGE("equality");
  auto character = '.';
  auto f = &character;
  auto l = f + 1;
  char c;
  CHECK(chr{'.'}.parse(f, l, c));
  CHECK(c == character);
  CHECK(f == l);

  MESSAGE("inequality");
  character = 'x';
  f = &character;
  CHECK(!chr{'y'}.parse(f, l, c));
  CHECK(f != l);
}

TEST(char class) {
  using namespace parsers;
  MESSAGE("xdigit");
  auto str = "deadbeef"s;
  auto attr = ""s;
  auto f = str.begin();
  auto l = str.end();
  auto p = +xdigit;
  CHECK(p.parse(f, l, attr));
  CHECK(attr == str);
  CHECK(f == l);

  MESSAGE("xdigit fail");
  str = "deadXbeef"s;
  attr.clear();
  f = str.begin();
  l = str.end();
  CHECK(p.parse(f, l, attr));
  CHECK(attr == "dead");
  CHECK(f == str.begin() + 4);
  CHECK(!p.parse(f, l, attr));
  ++f;
  CHECK(p.parse(f, l, attr));
  CHECK(f == l);
  CHECK(attr == "deadbeef");
}

TEST(quoted string) {
  auto p = quoted_string_parser<'\'', '#'>{};
  auto attr = ""s;

  MESSAGE("no escaped chars");
  auto str = "'foobar'"s;
  auto f = str.begin();
  auto l = str.end();
  CHECK(p.parse(f, l, attr));
  CHECK(attr == "foobar");
  CHECK(f == l);

  MESSAGE("escaped char in middle");
  str = "'foo#'bar'"s;
  f = str.begin();
  l = str.end();
  attr.clear();
  CHECK(p.parse(f, l, attr));
  CHECK(attr == "foo'bar");
  CHECK(f == l);

  MESSAGE("escaped char at beginning");
  str = "'#'foobar'"s;
  f = str.begin();
  l = str.end();
  attr.clear();
  CHECK(p.parse(f, l, attr));
  CHECK(attr == "'foobar");
  CHECK(f == l);

  MESSAGE("escaped char at end");
  str = "'foobar#''"s;
  f = str.begin();
  l = str.end();
  attr.clear();
  CHECK(p.parse(f, l, attr));
  CHECK(attr == "foobar'");
  CHECK(f == l);

  MESSAGE("missing trailing quote");
  str = "'foobar"s;
  f = str.begin();
  l = str.end();
  attr.clear();
  CHECK(!p.parse(f, l, attr));
  CHECK(attr == "foobar");

  MESSAGE("missing trailing quote after escaped quote");
  str = "'foobar#'"s;
  f = str.begin();
  l = str.end();
  attr.clear();
  CHECK(!p.parse(f, l, attr));
  CHECK(attr == "foobar'");
}

TEST(attribute compatibility : string) {
  auto str = "..."s;
  auto attr = ""s;
  auto f = str.begin();
  auto l = str.end();
  auto p = char_parser{'.'};

  MESSAGE("char into string");
  CHECK(p.parse(f, l, attr));
  CHECK(attr == ".");
  CHECK(p.parse(f, l, attr));
  CHECK(attr == "..");
  CHECK(p.parse(f, l, attr));
  CHECK(attr == str);
  CHECK(f == l);

  MESSAGE("plus(+)");
  attr.clear();
  f = str.begin();
  auto plus = +p;
  CHECK(plus.parse(f, l, attr));
  CHECK(str == attr);
  CHECK(f == l);

  MESSAGE("kleene (*)");
  attr.clear();
  f = str.begin();
  auto kleene = *p;
  CHECK(kleene.parse(f, l, attr));
  CHECK(str == attr);
  CHECK(f == l);

  MESSAGE("sequence (>>)");
  attr.clear();
  f = str.begin();
  auto seq = p >> p >> p;
  CHECK(seq.parse(f, l, attr));
  CHECK(str == attr);
  CHECK(f == l);
}

TEST(attribute compatibility : pair) {
  using namespace parsers;
  auto str = "xy"s;
  auto attr = ""s;
  auto f = str.begin();
  auto l = str.end();
  auto c = chr{'x'} >> chr{'y'};

  MESSAGE("pair<char, char>");
  std::pair<char, char> p0;
  CHECK(c.parse(f, l, p0));
  CHECK(p0.first == 'x');
  CHECK(p0.second == 'y');

  MESSAGE("pair<string, string>");
  f = str.begin();
  std::pair<std::string, std::string> p1;
  CHECK(c.parse(f, l, p1));
  CHECK(p1.first == "x");
  CHECK(p1.second == "y");
}

TEST(bool) {
  auto p0 = single_char_bool_parser{};
  auto p1 = zero_one_bool_parser{};
  auto p2 = literal_bool_parser{};
  auto str = "T0trueFfalse1"s;
  auto i = str.begin();
  auto l = str.end();
  auto f = i;
  bool b;

  MESSAGE("successful 'T'");
  CHECK(p0.parse(i, l, b));
  CHECK(b);
  CHECK(i == f + 1);
  // Wrong parser
  CHECK(!p0.parse(i, l, b));
  CHECK(i == f + 1);
  // Correct parser
  CHECK(p1.parse(i, l, b));
  CHECK(!b);
  CHECK(i == f + 2);
  CHECK(p2.parse(i, l, b));
  CHECK(b);
  CHECK(i == f + 6);
  // Wrong parser
  CHECK(!p2.parse(i, l, b));
  CHECK(i == f + 6);
  // Correct parser
  CHECK(p0.parse(i, l, b));
  CHECK(!b);
  CHECK(i == f + 7);
  b = true;
  CHECK(p2.parse(i, l, b));
  CHECK(!b);
  CHECK(i == f + 12);
  CHECK(p1.parse(i, l, b));
  CHECK(b);
  CHECK(i == f + 13);
  CHECK(i == l);

  MESSAGE("unused type");
  i = f;
  CHECK(p0.parse(i, l, unused));
  CHECK(p0(str));
}

TEST(integral) {
  MESSAGE("signed integers");
  auto str = "-1024"s;
  auto p0 = integral_parser<int>{};
  int n;
  auto f = str.begin();
  auto l = str.end();
  CHECK(p0.parse(f, l, n));
  CHECK(n == -1024);
  CHECK(f == l);
  f = str.begin() + 1;
  n = 0;
  CHECK(p0.parse(f, l, n));
  CHECK(n == 1024);
  CHECK(f == l);
  str[0] = '+';
  f = str.begin();
  n = 0;
  CHECK(p0.parse(f, l, n));
  CHECK(n == 1024);
  CHECK(f == l);

  MESSAGE("unsigned integers");
  auto p1 = integral_parser<unsigned>{};
  unsigned u;
  f = str.begin() + 1; // no sign
  CHECK(p1.parse(f, l, u));
  CHECK(u == 1024);
  CHECK(f == l);
  f = str.begin() + 1;
  u = 0;
  CHECK(p1.parse(f, l, u));
  CHECK(n == 1024);
  CHECK(f == l);

  MESSAGE("digit constraints");
  auto p2 = integral_parser<int, 4, 2>{};
  n = 0;
  str[0] = '-';
  f = str.begin();
  CHECK(p2.parse(f, l, n));
  CHECK(n == -1024);
  CHECK(f == l);
  // Not enough digits.
  str = "-1";
  f = str.begin();
  l = str.end();
  CHECK(!p2.parse(f, l, n));
  CHECK(f == str.begin());
  // Too many digits.
  str = "-123456";
  f = str.begin();
  l = str.end();
  CHECK(!p2.parse(f, l, unused));
  CHECK(f == str.begin());
}

TEST(real) {
  auto p = make_parser<double>{};
  MESSAGE("integral plus fractional part, negative");
  auto str = "-123.456789"s;
  auto f = str.begin();
  auto l = str.end();
  double d;
  CHECK(p.parse(f, l, d));
  CHECK(d == -123.456789);
  CHECK(f == l);
  MESSAGE("integral plus fractional part, positive");
  d = 0;
  f = str.begin() + 1;
  CHECK(p.parse(f, l, d));
  CHECK(d == 123.456789);
  CHECK(f == l);
  MESSAGE("no integral part, positive");
  d = 0;
  f = str.begin() + 4;
  CHECK(p.parse(f, l, d));
  CHECK(d == 0.456789);
  CHECK(f == l);
  MESSAGE("no integral part, negative");
  str = "-.456789";
  f = str.begin();
  l = str.end();
  CHECK(p.parse(f, l, d));
  CHECK(d == -0.456789);
  CHECK(f == l);
  //  MESSAGE("no fractional part, negative");
  //  d = 0;
  //  f = str.begin();
  //  CHECK(p.parse(f, f + 4, d));
  //  CHECK(d == -123);
  //  CHECK(f == str.begin() + 4);
  //  MESSAGE("no fractional part, positive");
  //  d = 0;
  //  f = str.begin() + 1;
  //  CHECK(p.parse(f, f + 3, d));
  //  CHECK(d == 123);
  //  CHECK(f == str.begin() + 4);
}

TEST(binary) {
  using namespace parsers;
  auto str = "\x01\x02\x03\x04\x05\x06\x07\x08"s;

  MESSAGE("big endian");
  auto f = str.begin();
  auto l = f + 1;
  auto u8 = uint8_t{0};
  CHECK(b8be.parse(f, l, u8));
  CHECK(u8 == 0x01);
  CHECK(f == l);
  f = str.begin();
  l = f + 2;
  auto u16 = uint16_t{0};
  CHECK(b16be.parse(f, l, u16));
  CHECK(u16 == 0x0102);
  CHECK(f == l);
  f = str.begin();
  l = f + 4;
  auto u32 = uint32_t{0};
  CHECK(b32be.parse(f, l, u32));
  CHECK(u32 == 0x01020304);
  CHECK(f == l);
  f = str.begin();
  l = f + 8;
  auto u64 = uint64_t{0};
  CHECK(b64be.parse(f, l, u64));
  CHECK(u64 == 0x0102030405060708);
  CHECK(f == l);

  MESSAGE("little endian");
  f = str.begin();
  l = f + 1;
  CHECK(b8le.parse(f, l, u8));
  CHECK(u8 == 0x01);
  CHECK(f == l);
  f = str.begin();
  l = f + 2;
  CHECK(b16le.parse(f, l, u16));
  CHECK(u16 == 0x0201);
  CHECK(f == l);
  f = str.begin();
  l = f + 4;
  CHECK(b32le.parse(f, l, u32));
  CHECK(u32 == 0x04030201);
  CHECK(f == l);
  f = str.begin();
  l = f + 8;
  CHECK(b64le.parse(f, l, u64));
  CHECK(u64 == 0x0807060504030201);
  CHECK(f == l);
}

TEST(recursive rule) {
  using namespace parsers;
  rule<std::string::iterator, char> r;
  r = alpha | '[' >> r >> ']';
  auto str = "[[[x]]]"s;
  auto f = str.begin();
  auto l = str.end();

  MESSAGE("unused type");
  CHECK(r.parse(f, l, unused));
  CHECK(f == l);

  MESSAGE("attribute");
  char c;
  f = str.begin();
  CHECK(r.parse(f, l, c));
  CHECK(f == l);
  CHECK(c == 'x');
}

TEST(stream) {
  std::istringstream ss{"a.b.c"};
  key k;
  ss >> k;
  CHECK(ss.good());
  CHECK(k == key{"a", "b", "c"});
}

TEST(to) {
  auto k = to<key>("a.b.c"); // John!
  REQUIRE(k);
  CHECK(*k == key{"a", "b", "c"});
}

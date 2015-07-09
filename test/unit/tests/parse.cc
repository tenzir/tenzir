#include "vast/data.h"
#include "vast/concept/parseable/core.h"
#include "vast/concept/parseable/numeric.h"
#include "vast/concept/parseable/string.h"
#include "vast/concept/parseable/string/quoted_string.h"
#include "vast/concept/parseable/vast/address.h"
#include "vast/concept/parseable/vast/pattern.h"
#include "vast/concept/parseable/vast/port.h"
#include "vast/concept/parseable/vast/subnet.h"
#include "vast/concept/parseable/vast/time.h"

#define SUITE parseable
#include "test.h"

using namespace vast;
using namespace std::string_literals;

TEST(char)
{
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
  CHECK(! chr{'y'}.parse(f, l, c));
  CHECK(f != l);
}

TEST(char class)
{
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
  CHECK(! p.parse(f, l, attr));
  ++f;
  CHECK(p.parse(f, l, attr));
  CHECK(f == l);
  CHECK(attr == "deadbeef");
}

TEST(quoted string)
{
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
  CHECK(! p.parse(f, l, attr));
  CHECK(attr == "foobar");

  MESSAGE("missing trailing quote after escaped quote");
  str = "'foobar#'"s;
  f = str.begin();
  l = str.end();
  attr.clear();
  CHECK(! p.parse(f, l, attr));
  CHECK(attr == "foobar'");
}

TEST(attribute compatibility: string)
{
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

TEST(attribute compatibility: pair)
{
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

TEST(bool)
{
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
  CHECK(! p0.parse(i, l, b));
  CHECK(i == f + 1);
  // Correct parser
  CHECK(p1.parse(i, l, b));
  CHECK(! b);
  CHECK(i == f + 2);
  CHECK(p2.parse(i, l, b));
  CHECK(b);
  CHECK(i == f + 6);
  // Wrong parser
  CHECK(! p2.parse(i, l, b));
  CHECK(i == f + 6);
  // Correct parser
  CHECK(p0.parse(i, l, b));
  CHECK(! b);
  CHECK(i == f + 7);
  b = true;
  CHECK(p2.parse(i, l, b));
  CHECK(! b);
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

TEST(integral)
{
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
  f = str.begin() + 1;  // no sign
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
  CHECK(! p2.parse(f, l, n));
  CHECK(f == str.begin());
  // Too many digits.
  str = "-123456";
  f = str.begin();
  l = str.end();
  CHECK(! p2.parse(f, l, unused));
  CHECK(f == str.begin());
}

TEST(real)
{
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

TEST(time::duration)
{
  auto p = make_parser<time::duration>{};
  auto str = "1000ms"s;
  auto f = str.begin();
  auto l = str.end();
  time::duration d;
  CHECK(p.parse(f, l, d));
  CHECK(f == l);
  CHECK(d == time::milliseconds(1000));
  str = "42s";
  f = str.begin();
  l = str.end();
  CHECK(p.parse(f, l, d));
  CHECK(f == l);
  CHECK(d == time::seconds(42));
}

TEST(time::point)
{
  auto p = make_parser<time::point>{};
  auto str = "2012-08-12+23:55:04"s;
  auto f = str.begin();
  auto l = str.end();
  time::point tp;
  CHECK(p.parse(f, l, tp));
  CHECK(f == l);
  CHECK(tp == time::point::utc(2012, 8, 12, 23, 55, 4));
}

TEST(pattern)
{
  auto p = make_parser<pattern>{};
  auto str = "/^\\w{3}\\w{3}\\w{3}$/"s;
  auto f = str.begin();
  auto l = str.end();
  pattern pat;
  CHECK(p.parse(f, l, pat));
  CHECK(f == l);
  CHECK(to_string(pat) == str);

  str = "/foo\\+(bar){2}|\"baz\"*/";
  pat = {};
  f = str.begin();
  l = str.end();
  CHECK(p.parse(f, l, pat));
  CHECK(f == l);
  CHECK(to_string(pat) == str);
}

TEST(address)
{
  auto p = make_parser<address>{};

  MESSAGE("IPv4");
  auto str = "192.168.0.1"s;
  auto f = str.begin();
  auto l = str.end();
  address a;
  CHECK(p.parse(f, l, a));
  CHECK(f == l);
  CHECK(a.is_v4());
  CHECK(to_string(a) == str);

  MESSAGE("IPv6");
  str = "::";
  f = str.begin();
  l = str.end();
  CHECK(p.parse(f, l, a));
  CHECK(f == l);
  CHECK(a.is_v6());
  CHECK(to_string(a) == str);
  str = "beef::cafe";
  f = str.begin();
  l = str.end();
  CHECK(p.parse(f, l, a));
  CHECK(f == l);
  CHECK(a.is_v6());
  CHECK(to_string(a) == str);
  str = "f00::cafe";
  f = str.begin();
  l = str.end();
  CHECK(p.parse(f, l, a));
  CHECK(f == l);
  CHECK(a.is_v6());
  CHECK(to_string(a) == str);
}

TEST(subnet)
{
  auto p = make_parser<subnet>{};

  MESSAGE("IPv4");
  auto str = "192.168.0.0/24"s;
  auto f = str.begin();
  auto l = str.end();
  subnet s;
  CHECK(p.parse(f, l, s));
  CHECK(f == l);
  CHECK(s == subnet{*to<address>("192.168.0.0"), 24});
  CHECK(s.network().is_v4());

  MESSAGE("IPv6");
  str = "beef::cafe/40";
  f = str.begin();
  l = str.end();
  CHECK(p.parse(f, l, s));
  CHECK(f == l);
  CHECK(s == subnet{*to<address>("beef::cafe"), 40});
  CHECK(s.network().is_v6());
}

TEST(port)
{
  auto p = make_parser<port>();

  MESSAGE("tcp");
  auto str = "22/tcp"s;
  auto f = str.begin();
  auto l = str.end();
  port prt;
  CHECK(p.parse(f, l, prt));
  CHECK(f == l);
  CHECK(prt == port{22, port::tcp});

  MESSAGE("udp");
  str = "53/udp"s;
  f = str.begin();
  l = str.end();
  CHECK(p.parse(f, l, prt));
  CHECK(f == l);
  CHECK(prt == port{53, port::udp});

  MESSAGE("icmp");
  str = "7/icmp"s;
  f = str.begin();
  l = str.end();
  CHECK(p.parse(f, l, prt));
  CHECK(f == l);
  CHECK(prt == port{7, port::icmp});

  MESSAGE("unknown");
  str = "42/?"s;
  f = str.begin();
  l = str.end();
  CHECK(p.parse(f, l, prt));
  CHECK(f == l);
  CHECK(prt == port{42, port::unknown});
}

//
// TODO: convert to parseable concept from here
//

TEST(containers)
{
  auto str = "{1, 2, 3}"s;
  auto i = str.begin();
  auto s = parse<set>(i, str.end(), type::integer{});
  REQUIRE(s);
  CHECK(i == str.end());
  CHECK(*s == set{1, 2, 3});

  str = "[a--b--c]";
  i = str.begin();
  auto v = parse<vector>(i, str.end(), type::string{}, "--");
  REQUIRE(v);
  CHECK(i == str.end());
  CHECK(*v == vector{"a", "b", "c"});

  auto roots = "a.root-servers.net,b.root-servers.net,c.root-servers.net";
  v = to<vector>(roots, type::string{}, ",", "", "");
  REQUIRE(v);
  REQUIRE(v->size() == 3);
  CHECK(v->front() == "a.root-servers.net");
  CHECK(v->back() == "c.root-servers.net");
}

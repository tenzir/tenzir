#include "vast/data.h"
#include "vast/concept/parseable/core.h"
#include "vast/concept/parseable/numeric.h"
#include "vast/concept/parseable/string.h"
#include "vast/concept/parseable/string/quoted_string.h"
#include "vast/concept/parseable/vast/address.h"
#include "vast/concept/parseable/vast/time.h"

#define SUITE parseable
#include "test.h"

using namespace vast;
using namespace std::string_literals;

TEST(char)
{
  auto chr = '.';
  auto got = '_';
  auto f = &chr;
  auto l = f + 1;
  auto c = char_parser{'.'};
  CHECK(c.parse(f, l, got));
  CHECK(got == chr);
  CHECK(f == l);

  chr = 'x';
  f = &chr;
  CHECK(! c.parse(f, l, got));
  CHECK(f != l);
}

TEST(char class)
{
  MESSAGE("xdigit");
  auto str = "deadbeef"s;
  auto got = ""s;
  auto f = str.begin();
  auto l = str.end();
  auto c = +xdigit_parser{};
  CHECK(c.parse(f, l, got));
  CHECK(got == str);
  CHECK(f == l);

  MESSAGE("xdigit fail");
  str = "deadXbeef"s;
  got.clear();
  f = str.begin();
  l = str.end();
  CHECK(c.parse(f, l, got));
  CHECK(got == "dead");
  CHECK(f == str.begin() + 4);
  CHECK(! c.parse(f, l, got));
  ++f;
  CHECK(c.parse(f, l, got));
  CHECK(f == l);
  CHECK(got == "deadbeef");
}

TEST(quoted string)
{
  auto p = quoted_string_parser<'\''>{};
  auto got = ""s;

  MESSAGE("no escaped chars");
  auto str = "'foobar'"s;
  auto f = str.begin();
  auto l = str.end();
  CHECK(p.parse(f, l, got));
  CHECK(got == "foobar");
  CHECK(f == l);

  MESSAGE("escaped char in middle");
  str = "'foo\\'bar'"s;
  f = str.begin();
  l = str.end();
  got.clear();
  CHECK(p.parse(f, l, got));
  CHECK(got == "foo'bar");
  CHECK(f == l);

  MESSAGE("escaped char at beginning");
  str = "'\\'foobar'"s;
  f = str.begin();
  l = str.end();
  got.clear();
  CHECK(p.parse(f, l, got));
  CHECK(got == "'foobar");
  CHECK(f == l);

  MESSAGE("escaped char at end");
  str = "'foobar\\''"s;
  f = str.begin();
  l = str.end();
  got.clear();
  CHECK(p.parse(f, l, got));
  CHECK(got == "foobar'");
  CHECK(f == l);

  MESSAGE("missing trailing quote");
  str = "'foobar"s;
  f = str.begin();
  l = str.end();
  got.clear();
  CHECK(! p.parse(f, l, got));
  CHECK(got == "foobar");

  MESSAGE("missing trailing quote after escaped quote");
  str = "'foobar\\'"s;
  f = str.begin();
  l = str.end();
  got.clear();
  CHECK(! p.parse(f, l, got));
  CHECK(got == "foobar'");
}

TEST(attribute compatibility: string)
{
  auto str = "..."s;
  auto got = ""s;
  auto f = str.begin();
  auto l = str.end();
  auto c = char_parser{'.'};

  MESSAGE("char into string");
  CHECK(c.parse(f, l, got));
  CHECK(got == ".");
  CHECK(c.parse(f, l, got));
  CHECK(got == "..");
  CHECK(c.parse(f, l, got));
  CHECK(got == str);
  CHECK(f == l);

  MESSAGE("plus(+)");
  got.clear();
  f = str.begin();
  auto plus = +c;
  CHECK(plus.parse(f, l, got));
  CHECK(str == got);
  CHECK(f == l);

  MESSAGE("kleene (*)");
  got.clear();
  f = str.begin();
  auto kleene = *c;
  CHECK(kleene.parse(f, l, got));
  CHECK(str == got);
  CHECK(f == l);

  MESSAGE("and (>>)");
  got.clear();
  f = str.begin();
  auto seq = c >> c >> c;
  CHECK(seq.parse(f, l, got));
  CHECK(str == got);
  CHECK(f == l);
}

TEST(attribute compatibility: pair)
{
  auto str = "xy"s;
  auto got = ""s;
  auto f = str.begin();
  auto l = str.end();
  auto c = char_parser{'x'} >> char_parser{'y'};

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
  // Successful 'T'
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
  // Unusued type
  i = f;
  CHECK(p0.parse(i, l, unused));
  CHECK(p0(str));
}

TEST(integral)
{
  // Default parser for signed integers.
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
  // Default parser for unsigned integers.
  auto p1 = integral_parser<unsigned>{};
  unsigned u;
  f = str.begin();
  CHECK(p1.parse(f, l, u));
  CHECK(u == 1024);
  CHECK(f == l);
  f = str.begin() + 1;
  u = 0;
  CHECK(p1.parse(f, l, u));
  CHECK(n == 1024);
  CHECK(f == l);
  // Parser with digit constraints.
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
  // Integral plus fractional part, negative.
  auto str = "-123.456789"s;
  auto f = str.begin();
  auto l = str.end();
  double d;
  CHECK(p.parse(f, l, d));
  CHECK(d == -123.456789);
  CHECK(f == l);
  // Integral plus fractional part, positive.
  d = 0;
  f = str.begin() + 1;
  CHECK(p.parse(f, l, d));
  CHECK(d == 123.456789);
  CHECK(f == l);
  // No integral part, positive.
  d = 0;
  f = str.begin() + 4;
  CHECK(p.parse(f, l, d));
  CHECK(d == 0.456789);
  CHECK(f == l);
  // No integral part, negative.
  str = "-.456789";
  f = str.begin();
  l = str.end();
  CHECK(p.parse(f, l, d));
  CHECK(d == -0.456789);
  CHECK(f == l);
//  // No fractional part, negative.
//  d = 0;
//  f = str.begin();
//  CHECK(p.parse(f, f + 4, d));
//  CHECK(d == -123);
//  CHECK(f == str.begin() + 4);
//  // No fractional part, positive.
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
  // No unit (=> seconds)
  f = str.begin();
  CHECK(p.parse(f, l - 2, d));
  CHECK(f == l - 2);
  CHECK(d == time::seconds(1000));
  // Fractional timestamp (e.g., UNIX epoch).
  str = "123.456789";
  f = str.begin();
  l = str.end();
  CHECK(p.parse(f, l, d));
  CHECK(f == l);
  CHECK(d == time::fractional(123.456789));
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

//
// TODO: convert to parseable concept from here
//

TEST(pattern)
{
  auto str = "/^\\w{3}\\w{3}\\w{3}$/"s;
  auto i = str.begin();
  auto p = parse<pattern>(i, str.end());
  CHECK(p);
  CHECK(i == str.end());

  str = "/foo\\+(bar){2}|\"baz\"*/";
  i = str.begin();
  p = parse<pattern>(i, str.end());
  CHECK(p);
  CHECK(i == str.end());
}

TEST(address)
{
  auto p = make_parser<address>{};
  // IPv4
  auto str = "192.168.0.1"s;
  auto f = str.begin();
  auto l = str.end();
  address a;
  CHECK(p.parse(f, l, a));
  CHECK(f == l);
  CHECK(a.is_v4());
  CHECK(to_string(a) == str);
  // IPv6
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
  auto str = "192.168.0.0/24";
  auto start = str;
  auto end = str + std::strlen(str);
  auto s = parse<subnet>(start, end);
  CHECK(start == end);
  CHECK(*s == subnet{*to<address>("192.168.0.0"), 24});

  str = "::/40";
  start = str;
  end = str + std::strlen(str);
  s = parse<subnet>(start, end);
  CHECK(start == end);
  CHECK(*s == subnet{*to<address>("::"), 40});
}

TEST(port)
{
  {
    auto s = "22/tcp";
    auto i = s;
    auto end = s + std::strlen(s);
    auto p = parse<port>(i, end);
    REQUIRE(p);
    CHECK(i == end);
    CHECK(*p == port{22, port::tcp});
  }

  {
    auto s = "42/unknown";
    auto i = s;
    auto end = s + std::strlen(s);
    auto p = parse<port>(i, end);
    REQUIRE(p);
    CHECK(i == end);
    CHECK(*p, port{42, port::unknown});
  }

  {
    auto s = "53/udp";
    auto i = s;
    auto end = s + std::strlen(s);
    auto p = parse<port>(i, end);
    REQUIRE(p);
    CHECK(i == end);
    CHECK(*p == port{53, port::udp});
  }

  {
    auto s = "7/icmp";
    auto i = s;
    auto end = s + std::strlen(s);
    auto p = parse<port>(i, end);
    REQUIRE(p);
    CHECK(i == end);
    CHECK(*p == port{7, port::icmp});
  }
}

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

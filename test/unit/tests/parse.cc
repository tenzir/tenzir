#include "vast/data.h"
#include "vast/concept/parseable/core.h"
#include "vast/concept/parseable/numeric.h"
#include "vast/concept/parseable/vast/address.h"
#include "vast/concept/parseable/vast/time.h"

#include "framework/unit.h"
#include "framework/unit.h"

#define SUITE parse
#include "test.h"

using namespace vast;

TEST(bool)
{
  auto p0 = single_char_bool_parser{};
  auto p1 = zero_one_bool_parser{};
  auto p2 = literal_bool_parser{};
  auto str = std::string{"T0trueFfalse1"};
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
  auto str = std::string{"-1024"};
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
  auto str = std::string{"-123.456789"};
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
  auto pt = make_parser<time::duration>{};
  auto str = std::string{"1000ms"};
  auto f = str.begin();
  auto l = str.end();
  time::duration d;
  CHECK(pt.parse(f, l, d));
  CHECK(f == l);
  CHECK(d == time::milliseconds(1000));
  // No unit (=> seconds)
  f = str.begin();
  CHECK(pt.parse(f, l - 2, d));
  CHECK(f == l - 2);
  CHECK(d == time::seconds(1000));
  // Fractional timestamp (e.g., UNIX epoch).
  str = "123.456789";
  f = str.begin();
  l = str.end();
  CHECK(pt.parse(f, l, d));
  CHECK(f == l);
  CHECK(d == time::fractional(123.456789));
}

//
// TODO: convert to parseable concept from here
//

TEST(time::point)
{
  auto expected = time::point::utc(2012, 8, 12, 23, 55, 4);
  std::string str("2012-08-12+23:55:04");
  auto i = str.begin();
  auto t = parse<time::point>(i, str.end(), time::point::format);
  CHECK(i == str.end());
  CHECK(*t == expected);
}

TEST(pattern)
{
  std::string str = "/^\\w{3}\\w{3}\\w{3}$/";
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
  auto str = std::string("192.168.0.1");
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
  std::string str = "{1, 2, 3}";
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

#include "vast/concept/parseable/vast/address.h"
#include "vast/concept/parseable/vast/pattern.h"
#include "vast/concept/parseable/vast/port.h"
#include "vast/concept/parseable/vast/subnet.h"
#include "vast/concept/parseable/vast/time.h"

#define SUITE parseable
#include "test.h"

using namespace vast;
using namespace std::string_literals;

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

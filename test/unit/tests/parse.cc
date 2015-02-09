#include "framework/unit.h"

#include "vast/parse.h"
#include "vast/detail/parser/query.h"

SUITE("parse")

using namespace vast;

TEST("bool")
{
  char const* str = "T";
  auto start = str;
  auto end = str + 1;
  auto b = parse<bool>(start, end);
  CHECK(start == end);
  REQUIRE(b);
  CHECK(*b);

  str = "F";
  start = str;
  b = parse<bool>(start, str + 1);
  CHECK(start == str + 1);
  REQUIRE(b);
  CHECK(! *b);

  str = "x";
  start = str;
  CHECK(! parse<bool>(start, str + 1));
}

TEST("int")
{
  auto str = "-1024";
  auto start = str;
  auto end = str + 5;
  auto i = parse<int64_t>(start, end);
  CHECK(start == end);
  CHECK(*i == -1024ll);

  str = "+1024";
  start = str;
  end = str + 5;
  i = parse<int64_t>(start, end);
  CHECK(start == end);
  CHECK(*i == 1024ll);

  str = "1337";
  start = str;
  end = str + 4;
  i = parse<int64_t>(start, end);
  CHECK(start == end);
  CHECK(*i == 1337ll);
}

TEST("uint")
{
  auto str = "1024";
  auto start = str;
  auto end = str + 4;
  auto u = parse<int64_t>(start, end);
  CHECK(start == end);
  CHECK(*u == 1024ull);

  str = "+1024";
  start = str;
  ++end;
  CHECK(! parse<uint64_t>(start, end));
}

TEST("double")
{
  auto str = "-123.456789";
  auto start = str;
  auto end = str + std::strlen(str);
  auto d = parse<double>(start, end);
  CHECK(start == end);
  CHECK(*d == -123.456789);

  bool is_double = true;
  d = to<double>("-123", &is_double);
  REQUIRE(d);
  CHECK(! is_double);
  CHECK(*d == -123.0);

  is_double = false;
  d = to<double>("-123.0", &is_double);
  REQUIRE(d);
  CHECK(is_double);
  CHECK(*d == -123.0);
}

TEST("time::duration")
{
  auto str = "1000ms";
  auto start = str;
  auto end = str + 6;
  auto r = parse<time::duration>(start, end);
  CHECK(start == end);
  CHECK(*r == time::milliseconds(1000));

  str = "1000";
  start = str;
  end = str + 4;
  r = parse<time::duration>(start, end);
  CHECK(start == end);
  CHECK(*r == time::seconds(1000));

  str = "123.456789";
  start = str;
  end = str + 10;
  r = parse<time::duration>(start, end);
  CHECK(start == end);
  CHECK(*r == time::fractional(123.456789));
}

TEST("time::point")
{
  auto expected = time::point::utc(2012, 8, 12, 23, 55, 4);
  std::string str("2012-08-12+23:55:04");
  auto i = str.begin();
  auto t = parse<time::point>(i, str.end(), time::point::format);
  CHECK(i == str.end());
  CHECK(*t == expected);
}

TEST("pattern")
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

TEST("address")
{
  auto str = "192.168.0.1";
  auto start = str;
  auto end = str + std::strlen(str);
  auto a = parse<address>(start, end);
  REQUIRE(a);
  CHECK(start == end);
  CHECK(*a == *address::from_v4(str));

  str = "f00::cafe";
  start = str;
  end = str + std::strlen(str);
  a = parse<address>(start, end);
  CHECK(start == end);
  CHECK(*a == *address::from_v6(str));
}

TEST("subnet")
{
  auto str = "192.168.0.0/24";
  auto start = str;
  auto end = str + std::strlen(str);
  auto s = parse<subnet>(start, end);
  CHECK(start == end);
  CHECK(*s == subnet{*address::from_v4("192.168.0.0"), 24});

  str = "::/40";
  start = str;
  end = str + std::strlen(str);
  s = parse<subnet>(start, end);
  CHECK(start == end);
  CHECK(*s == subnet{*address::from_v6("::"), 40});
}

TEST("port")
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

TEST("containers")
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

TEST("value")
{
  // Booleans
  auto v = to<value>("T");
  REQUIRE(v);
  REQUIRE(is<boolean>(*v));
  CHECK(*get<bool>(*v));

  v = to<value>("F");
  REQUIRE(v);
  REQUIRE(is<boolean>(*v));
  CHECK(! *get<bool>(*v));

  // Numbers
  v = to<value>("123456789");
  REQUIRE(v);
  REQUIRE(is<count>(*v));
  CHECK(*get<count>(*v) == 123456789ll);

  v = to<value>("+123456789");
  REQUIRE(v);
  REQUIRE(is<integer>(*v));
  CHECK(*get<integer>(*v) == 123456789ll);

  v = to<value>("-123456789");
  REQUIRE(v);
  REQUIRE(is<integer>(*v));
  CHECK(*get<integer>(*v) == -123456789ll);

  v = to<value>("-123.456789");
  REQUIRE(v);
  REQUIRE(is<real>(*v));
  CHECK(*get<real>(*v) == -123.456789);

  // Time ranges
  v = to<value>("42 nsecs");
  REQUIRE(v);
  REQUIRE(is<time::duration>(*v));
  CHECK(get<time::duration>(*v)->count() == 42ll);

  v = to<value>("42 musec");
  REQUIRE(v);
  REQUIRE(is<time::duration>(*v));
  CHECK(get<time::duration>(*v)->count() == 42000ll);

  v = to<value>("-42 msec");
  REQUIRE(v);
  REQUIRE(is<time::duration>(*v));
  CHECK(get<time::duration>(*v)->count() == -42000000ll);

  v = to<value>("99 secs");
  REQUIRE(v);
  REQUIRE(is<time::duration>(*v));
  CHECK(get<time::duration>(*v)->count() == 99000000000ll);

  v = to<value>("5 mins");
  REQUIRE(v);
  REQUIRE(is<time::duration>(*v));
  CHECK(get<time::duration>(*v)->count() == 300000000000ll);

  v = to<value>("3 hours");
  REQUIRE(v);
  REQUIRE(is<time::duration>(*v));
  CHECK(get<time::duration>(*v)->count() == 10800000000000ll);

  v = to<value>("4 days");
  REQUIRE(v);
  REQUIRE(is<time::duration>(*v));
  CHECK(get<time::duration>(*v)->count() == 345600000000000ll);

  v = to<value>("7 weeks");
  REQUIRE(v);
  REQUIRE(is<time::duration>(*v));
  CHECK(get<time::duration>(*v)->count() == 4233600000000000ll);

  v = to<value>("2 months");
  REQUIRE(v);
  REQUIRE(is<time::duration>(*v));
  CHECK(get<time::duration>(*v)->count() == 5184000000000000ll);

  v= to<value>("-8 years");
  REQUIRE(v);
  REQUIRE(is<time::duration>(*v));
  CHECK(get<time::duration>(*v)->count() == -252288000000000000ll);

  // Compound durations
  v = to<value>("5m99s");
  REQUIRE(v);
  REQUIRE(is<time::duration>(*v));
  CHECK(get<time::duration>(*v)->count() == 399000000000ll);

  // Time points
  v = to<value>("2012-08-12+23:55:04");
  CHECK(*get<time::point>(*v) == time::point::utc(2012, 8, 12, 23, 55, 4));

  v = to<value>("2012-08-12+00:00:00");
  REQUIRE(v);
  REQUIRE(is<time::point>(*v));
  CHECK(get<time::point>(*v)->since_epoch().count() == 1344729600000000000ll);

  v = to<value>("2012-08-12");
  REQUIRE(v);
  REQUIRE(is<time::point>(*v));
  CHECK(get<time::point>(*v)->since_epoch().count() == 1344729600000000000ll);

  v = to<value>("2012-08-12+23");
  REQUIRE(v);
  REQUIRE(is<time::point>(*v));
  CHECK(get<time::point>(*v)->since_epoch().count() == 1344812400000000000ll);

  v = to<value>("2012-08-12+23:55");
  REQUIRE(v);
  REQUIRE(is<time::point>(*v));
  CHECK(get<time::point>(*v)->since_epoch().count() == 1344815700000000000ll);

  v = to<value>("2012-08-12+23:55:04");
  REQUIRE(v);
  REQUIRE(is<time::point>(*v));
  CHECK(get<time::point>(*v)->since_epoch().count() == 1344815704000000000ll);

  // Strings
  v = to<value>("\"new\\nline\\\"esc\"");
  REQUIRE(v);
  CHECK(is<std::string>(*v));
  CHECK(*v == "new\nline\"esc");

  // Regexes
  v = to<value>("/../");
  REQUIRE(v);
  REQUIRE(is<pattern>(*v));
  CHECK(*v == pattern{".."});

  v = to<value>("/\\/../");
  REQUIRE(v);
  REQUIRE(is<pattern>(*v));
  CHECK(*v == pattern{"/.."});

  // Vectors
  v = to<value>("[1, 2, 3]");
  REQUIRE(v);
  CHECK(is<vector>(*v));
  CHECK(*v == vector{1u, 2u, 3u});

  // Sets
  v = to<value>("{+1, +2, +3}");
  REQUIRE(v);
  CHECK(is<set>(*v));
  CHECK(*v == set{1, 2, 3});

  v = to<value>("{\"foo\", \"bar\"}");
  REQUIRE(v);
  CHECK(is<set>(*v));
  CHECK(*v == set{"foo", "bar"});

  // Tables
  v = to<value>("{\"x\" -> T, \"y\" -> F}");
  REQUIRE(v);
  CHECK(is<table>(*v));
  CHECK(*v == table{{"x", true}, {"y", false}});

  // Records
  v = to<value>("(\"x\", T, 42, +42)");
  REQUIRE(v);
  CHECK(is<record>(*v));
  CHECK(*v == record{"x", true, 42u, 42});

  // Addresses
  v = to<value>("127.0.0.1");
  REQUIRE(v);
  CHECK(is<address>(*v));
  CHECK(*v == *address::from_v4("127.0.0.1"));

  v = to<value>("::");
  REQUIRE(v);
  CHECK(is<address>(*v));
  CHECK(*v == *address::from_v6("::"));

  v = to<value>("f00::");
  REQUIRE(v);
  CHECK(is<address>(*v));
  CHECK(*v == *address::from_v6("f00::"));

  // Subnets
  v = to<value>("10.0.0.0/8");
  REQUIRE(v);
  CHECK(is<subnet>(*v));
  CHECK(*v == subnet{*address::from_v4("10.0.0.0"), 8});

  v = to<value>("2001:db8:0:0:8:800:200c:417a/64");
  REQUIRE(v);
  CHECK(is<subnet>(*v));
  auto pfx = subnet{*address::from_v6("2001:db8:0:0:8:800:200c:417a"), 64};
  CHECK(*v == pfx);

  // Ports
  v = to<value>("53/udp");
  REQUIRE(v);
  CHECK(is<port>(*v));
  CHECK(*v == port{53, port::udp});
}

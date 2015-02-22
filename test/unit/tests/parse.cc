#include "framework/unit.h"

#include "vast/data.h"

using namespace vast;

SUITE("parse")

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

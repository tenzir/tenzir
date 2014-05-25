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

TEST("time_range")
{
  auto str = "1000ms";
  auto start = str;
  auto end = str + 6;
  auto r = parse<time_range>(start, end);
  CHECK(start == end);
  CHECK(*r == time_range::milliseconds(1000));

  str = "1000";
  start = str;
  end = str + 4;
  r = parse<time_range>(start, end);
  CHECK(start == end);
  CHECK(*r == time_range::seconds(1000));

  str = "123.456789";
  start = str;
  end = str + 10;
  r = parse<time_range>(start, end);
  CHECK(start == end);
  CHECK(*r == time_range::fractional(123.456789));
}

TEST("time_point")
{
  time_point expected(2012, 8, 12, 23, 55, 4);
  string str("2012-08-12+23:55:04");
  CHECK(time_point(to_string(str)) == expected);

  auto i = str.begin();
  auto t = parse<time_point>(i, str.end(), time_point::format);
  CHECK(i == str.end());
  CHECK(*t == expected);
}

TEST("string")
{
  // Here is a difference: the value parser grammar expects strings with
  // double quotes whereas this version does not.
  auto str = "\"f\\oo\\\"bar\"";
  auto start = str;
  auto end = str + std::strlen(str);

  auto s0 = parse<string>(start, end);
  REQUIRE(s0);
  CHECK(start == end);

  auto v = to<value>(str);
  REQUIRE(v);
  CHECK(*v != invalid);
  CHECK(s0->thin("\"", "\\") == *v);
}

TEST("regex")
{
  {
    string str("/^\\w{3}\\w{3}\\w{3}$/");
    auto i = str.begin();
    auto rx = parse<regex>(i, str.end());
    CHECK(rx);
    CHECK(i == str.end());
  }
  {
    auto str = "/foo\\+(bar){2}|\"baz\"*/";
    auto start = str;
    auto end = str + std::strlen(str);
    auto rx = parse<regex>(start, end);
    CHECK(rx);
    CHECK(start == end);
  }
}

TEST("containers")
{
  {
    string str{"{1, 2, 3}"};
    auto i = str.begin();
    auto s = parse<set>(i, str.end(), type::make<int_type>());
    REQUIRE(s);
    CHECK(i == str.end());

    set expected{1, 2, 3};
    CHECK(*s == expected);
  }
  {
    string str("a--b--c");
    auto i = str.begin();
    auto v = parse<vector>(i, str.end(), type::make<string_type>(), "--");
    REQUIRE(v);
    CHECK(i == str.end());

    vector expected{"a", "b", "c"};
    CHECK(*v == expected);
  }

  auto roots = "a.root-servers.net,b.root-servers.net,c.root-servers.net";
  auto v = to<vector>(roots, type::make<string_type>(), ",");
  REQUIRE(v);
  REQUIRE(v->size() == 3);
  CHECK(v->front() == "a.root-servers.net");
  CHECK(v->back() == "c.root-servers.net");
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

TEST("prefix")
{
  auto str = "192.168.0.0/24";
  auto start = str;
  auto end = str + std::strlen(str);
  auto p = parse<prefix>(start, end);
  CHECK(start == end);
  CHECK(*p == prefix{*address::from_v4("192.168.0.0"), 24});

  str = "::/40";
  start = str;
  end = str + std::strlen(str);
  p = parse<prefix>(start, end);
  CHECK(start == end);
  CHECK(*p == prefix{*address::from_v6("::"), 40});
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

TEST("value")
{
  // Booleans
  {
    auto v = to<value>("T");
    REQUIRE(v);
    CHECK(v->which() == bool_value);
    CHECK(v->get<bool>());

    v = to<value>("F");
    REQUIRE(v);
    CHECK(v->which() == bool_value);
    CHECK(! v->get<bool>());
  }

  // Numbers
  {
    auto v = to<value>("123456789");
    CHECK(v->which() == uint_value);
    CHECK(v->get<uint64_t>() == 123456789ll);

    v = to<value>("+123456789");
    CHECK(v->which() == int_value);
    CHECK(v->get<int64_t>() == 123456789ll);

    v = to<value>("-123456789");
    CHECK(v->which() == int_value);
    CHECK(v->get<int64_t>() == -123456789ll);

    v = to<value>("-123.456789");
    CHECK(v->which() == double_value);
    CHECK(v->get<double>() == -123.456789);
  }

  // Time ranges
  {
    auto v = to<value>("42 nsecs");
    CHECK(v->which() == time_range_value);
    CHECK(v->get<time_range>().count() == 42ll);

    v = to<value>("42 musec");
    CHECK(v->which() == time_range_value);
    CHECK(v->get<time_range>().count() == 42000ll);

    v = to<value>("-42 msec");
    CHECK(v->which() == time_range_value);
    CHECK(v->get<time_range>().count() == -42000000ll);

    v = to<value>("99 secs");
    CHECK(v->which() == time_range_value);
    CHECK(v->get<time_range>().count() == 99000000000ll);

    v = to<value>("5 mins");
    CHECK(v->which() == time_range_value);
    CHECK(v->get<time_range>().count() == 300000000000ll);

    v = to<value>("3 hours");
    CHECK(v->which() == time_range_value);
    CHECK(v->get<time_range>().count() == 10800000000000ll);

    v = to<value>("4 days");
    CHECK(v->which() == time_range_value);
    CHECK(v->get<time_range>().count() == 345600000000000ll);

    v = to<value>("7 weeks");
    CHECK(v->which() == time_range_value);
    CHECK(v->get<time_range>().count() == 4233600000000000ll);

    v = to<value>("2 months");
    CHECK(v->which() == time_range_value);
    CHECK(v->get<time_range>().count() == 5184000000000000ll);

    v= to<value>("-8 years");
    CHECK(v->which() == time_range_value);
    CHECK(v->get<time_range>().count() == -252288000000000000ll);

    // Compound durations
    v = to<value>("5m99s");
    CHECK(v->which() == time_range_value);
    CHECK(v->get<time_range>().count() == 399000000000ll);
  }

  // Time points
  {
    auto v = to<value>("2012-08-12+23:55:04");
    auto t = v->get<time_point>();
    CHECK(t, time_point(2012, 8, 12, 23, 55 == 4));

    v = to<value>("2012-08-12+00:00:00");
    CHECK(v->which() == time_point_value);
    CHECK(v->get<time_point>().since_epoch().count() == 1344729600000000000ll);

    v = to<value>("2012-08-12");
    CHECK(v->get<time_point>().since_epoch().count() == 1344729600000000000ll);

    v = to<value>("2012-08-12+23");
    CHECK(v->get<time_point>().since_epoch().count() == 1344812400000000000ll);

    v = to<value>("2012-08-12+23:55");
    CHECK(v->get<time_point>().since_epoch().count() == 1344815700000000000ll);

    v = to<value>("2012-08-12+23:55:04");
    CHECK(v->get<time_point>().since_epoch().count() == 1344815704000000000ll);
  }

  // Strings
  {
    // Escaped
    auto v = to<value>("\"new\\nline\\\"esc\"");
    CHECK(v->which() == string_value);
    CHECK(*v == "new\nline\"esc");
  }

  // Regexes
  {
    auto v = to<value>("/../");
    CHECK(v->which() == regex_value);
    CHECK(*v == regex{".."});

    v = to<value>("/\\/../");
    CHECK(v->which() == regex_value);
    CHECK(*v == regex{"/.."});
  }

  // Vectors
  {
    auto v = to<value>("[1, 2, 3]");
    CHECK(v->which() == vector_value);
    CHECK(*v == value(vector{1u, 2u, 3u}));
  }

  // Sets
  {
    auto v = to<value>("{+1, +2, +3}");
    CHECK(v->which() == set_value);
    CHECK(*v == value(set{1, 2, 3}));

    v = to<value>("{\"foo\", \"bar\"}");
    CHECK(v->which() == set_value);
    CHECK(*v == value(set{"foo", "bar"}));
  }

  // Tables
  {
    auto v = to<value>("{\"x\" -> T, \"y\" -> F}");
    CHECK(v->which() == table_value);
    CHECK(*v == value(table{{"x", true}, {"y", false}}));
  }

  // Records
  {
    auto v = to<value>("(\"x\", T, 42, +42)");
    CHECK(v->which() == record_value);
    CHECK(*v, value(record{"x", true, 42u == 42}));
  }

  // Addresses
  {
    auto v = to<value>("127.0.0.1");
    CHECK(v->which() == address_value);
    CHECK(*v == *address::from_v4("127.0.0.1"));

    v = to<value>("::");
    CHECK(v->which() == address_value);
    CHECK(*v == *address::from_v6("::"));

    v = to<value>("f00::");
    CHECK(v->which() == address_value);
    CHECK(*v == *address::from_v6("f00::"));
  }

  // Prefixes
  {
    auto v = to<value>("10.0.0.0/8");
    CHECK(v->which() == prefix_value);
    CHECK(*v == prefix{*address::from_v4("10.0.0.0"), 8});

    v = to<value>("2001:db8:0:0:8:800:200c:417a/64");
    CHECK(v->which() == prefix_value);
    auto pfx = prefix{*address::from_v6("2001:db8:0:0:8:800:200c:417a"), 64};
    CHECK(*v == pfx);
  }

  // Ports
  {
    auto v = to<value>("53/udp");
    CHECK(v->which() == port_value);
    CHECK(*v, (port{53 == port::udp}));
  }
}

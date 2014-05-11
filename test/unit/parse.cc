#define BOOST_SPIRIT_QI_DEBUG

#include "test.h"
#include "vast/expression.h"
#include "vast/parse.h"
#include "vast/detail/parser/query.h"

using namespace vast;

BOOST_AUTO_TEST_SUITE(parse_test_suite)

BOOST_AUTO_TEST_CASE(parse_bool)
{
  char const* str = "T";
  auto start = str;
  auto end = str + 1;
  auto b = parse<bool>(start, end);
  BOOST_CHECK_EQUAL(start, end);
  BOOST_REQUIRE(b);
  BOOST_CHECK(*b);

  str = "F";
  start = str;
  b = parse<bool>(start, str + 1);
  BOOST_CHECK_EQUAL(start, str + 1);
  BOOST_REQUIRE(b);
  BOOST_CHECK(! *b);

  str = "x";
  start = str;
  BOOST_CHECK(! parse<bool>(start, str + 1));
}

BOOST_AUTO_TEST_CASE(parse_int)
{
  auto str = "-1024";
  auto start = str;
  auto end = str + 5;
  auto i = parse<int64_t>(start, end);
  BOOST_CHECK_EQUAL(start, end);
  BOOST_CHECK_EQUAL(*i, -1024ll);

  str = "+1024";
  start = str;
  i = parse<int64_t>(start, end);
  BOOST_CHECK_EQUAL(start, end);
  BOOST_CHECK_EQUAL(*i, 1024ll);

  str = "1337";
  start = str;
  end = str + 4;
  i = parse<int64_t>(start, end);
  BOOST_CHECK_EQUAL(start, end);
  BOOST_CHECK_EQUAL(*i, 1337ll);
}

BOOST_AUTO_TEST_CASE(parse_uint)
{
  auto str = "1024";
  auto start = str;
  auto end = str + 4;
  auto u = parse<int64_t>(start, end);
  BOOST_CHECK_EQUAL(start, end);
  BOOST_CHECK_EQUAL(*u, 1024ull);

  str = "+1024";
  start = str;
  ++end;
  BOOST_CHECK(! parse<uint64_t>(start, end));
}

BOOST_AUTO_TEST_CASE(parse_double)
{
  auto str = "-123.456789";
  auto start = str;
  auto end = str + std::strlen(str);
  auto d = parse<double>(start, end);
  BOOST_CHECK_EQUAL(start, end);
  BOOST_CHECK_EQUAL(*d, -123.456789);

  bool is_double = true;
  d = to<double>("-123", &is_double);
  BOOST_REQUIRE(d);
  BOOST_CHECK(! is_double);
  BOOST_CHECK_EQUAL(*d, -123.0);

  is_double = false;
  d = to<double>("-123.0", &is_double);
  BOOST_REQUIRE(d);
  BOOST_CHECK(is_double);
  BOOST_CHECK_EQUAL(*d, -123.0);
}

BOOST_AUTO_TEST_CASE(parse_time_range)
{
  auto str = "1000ms";
  auto start = str;
  auto end = str + 6;
  auto r = parse<time_range>(start, end);
  BOOST_CHECK_EQUAL(start, end);
  BOOST_CHECK_EQUAL(*r, time_range::milliseconds(1000));

  str = "1000";
  start = str;
  end = str + 4;
  r = parse<time_range>(start, end);
  BOOST_CHECK_EQUAL(start, end);
  BOOST_CHECK_EQUAL(*r, time_range::seconds(1000));

  str = "123.456789";
  start = str;
  end = str + 10;
  r = parse<time_range>(start, end);
  BOOST_CHECK_EQUAL(start, end);
  BOOST_CHECK_EQUAL(*r, time_range::fractional(123.456789));
}

BOOST_AUTO_TEST_CASE(parse_time_point)
{
  time_point expected(2012, 8, 12, 23, 55, 4);
  string str("2012-08-12+23:55:04");
  BOOST_CHECK_EQUAL(time_point(to_string(str)), expected);

  auto i = str.begin();
  auto t = parse<time_point>(i, str.end(), time_point::format);
  BOOST_CHECK(i == str.end());
  BOOST_CHECK_EQUAL(*t, expected);
}

BOOST_AUTO_TEST_CASE(parse_string)
{
  // Here is a difference: the value parser grammar expects strings with
  // double quotes whereas this version does not.
  auto str = "\"f\\oo\\\"bar\"";
  auto start = str;
  auto end = str + std::strlen(str);

  auto s0 = parse<string>(start, end);
  BOOST_REQUIRE(s0);
  BOOST_CHECK_EQUAL(start, end);

  auto v = to<value>(str);
  BOOST_REQUIRE(v);
  BOOST_CHECK(*v != invalid);
  BOOST_CHECK_EQUAL(s0->thin("\"", "\\"), *v);
}

BOOST_AUTO_TEST_CASE(parse_regex)
{
  {
    string str("/^\\w{3}\\w{3}\\w{3}$/");
    auto i = str.begin();
    auto rx = parse<regex>(i, str.end());
    BOOST_CHECK(rx);
    BOOST_CHECK(i == str.end());
  }
  {
    auto str = "/foo\\+(bar){2}|\"baz\"*/";
    auto start = str;
    auto end = str + std::strlen(str);
    auto rx = parse<regex>(start, end);
    BOOST_CHECK(rx);
    BOOST_CHECK_EQUAL(start, end);
  }
}

BOOST_AUTO_TEST_CASE(parse_containers)
{
  {
    string str{"{1, 2, 3}"};
    auto i = str.begin();
    auto s = parse<set>(i, str.end(), type::make<int_type>());
    BOOST_REQUIRE(s);
    BOOST_CHECK(i == str.end());

    set expected{1, 2, 3};
    BOOST_CHECK_EQUAL(*s, expected);
  }
  {
    string str("a--b--c");
    auto i = str.begin();
    auto v = parse<vector>(i, str.end(), type::make<string_type>(), "--");
    BOOST_REQUIRE(v);
    BOOST_CHECK(i == str.end());

    vector expected{"a", "b", "c"};
    BOOST_CHECK_EQUAL(*v, expected);
  }

  auto roots = "a.root-servers.net,b.root-servers.net,c.root-servers.net";
  auto v = to<vector>(roots, type::make<string_type>(), ",");
  BOOST_REQUIRE(v);
  BOOST_REQUIRE_EQUAL(v->size(), 3);
  BOOST_CHECK_EQUAL(v->front(), "a.root-servers.net");
  BOOST_CHECK_EQUAL(v->back(), "c.root-servers.net");
}

BOOST_AUTO_TEST_CASE(parse_address)
{
  auto str = "192.168.0.1";
  auto start = str;
  auto end = str + std::strlen(str);
  auto a = parse<address>(start, end);
  BOOST_REQUIRE(a);
  BOOST_CHECK_EQUAL(start, end);
  BOOST_CHECK_EQUAL(*a, *address::from_v4(str));

  str = "f00::cafe";
  start = str;
  end = str + std::strlen(str);
  a = parse<address>(start, end);
  BOOST_CHECK_EQUAL(start, end);
  BOOST_CHECK_EQUAL(*a, *address::from_v6(str));
}

BOOST_AUTO_TEST_CASE(parse_prefix)
{
  auto str = "192.168.0.0/24";
  auto start = str;
  auto end = str + std::strlen(str);
  auto p = parse<prefix>(start, end);
  BOOST_CHECK_EQUAL(start, end);
  BOOST_CHECK_EQUAL(*p, prefix(*address::from_v4("192.168.0.0"), 24));

  str = "::/40";
  start = str;
  end = str + std::strlen(str);
  p = parse<prefix>(start, end);
  BOOST_CHECK_EQUAL(start, end);
  BOOST_CHECK_EQUAL(*p, prefix(*address::from_v6("::"), 40));
}

BOOST_AUTO_TEST_CASE(parse_port)
{
  {
    auto s = "22/tcp";
    auto i = s;
    auto end = s + std::strlen(s);
    auto p = parse<port>(i, end);
    BOOST_REQUIRE(p);
    BOOST_CHECK_EQUAL(i, end);
    BOOST_CHECK_EQUAL(*p, port(22, port::tcp));
  }

  {
    auto s = "42/unknown";
    auto i = s;
    auto end = s + std::strlen(s);
    auto p = parse<port>(i, end);
    BOOST_REQUIRE(p);
    BOOST_CHECK_EQUAL(i, end);
    BOOST_CHECK_EQUAL(*p, port(42, port::unknown));
  }

  {
    auto s = "53/udp";
    auto i = s;
    auto end = s + std::strlen(s);
    auto p = parse<port>(i, end);
    BOOST_REQUIRE(p);
    BOOST_CHECK_EQUAL(i, end);
    BOOST_CHECK_EQUAL(*p, port(53, port::udp));
  }

  {
    auto s = "7/icmp";
    auto i = s;
    auto end = s + std::strlen(s);
    auto p = parse<port>(i, end);
    BOOST_REQUIRE(p);
    BOOST_CHECK_EQUAL(i, end);
    BOOST_CHECK_EQUAL(*p, port(7, port::icmp));
  }
}

BOOST_AUTO_TEST_CASE(parse_value)
{
  // Booleans
  {
    auto v = to<value>("T");
    BOOST_REQUIRE(v);
    BOOST_CHECK_EQUAL(v->which(), bool_value);
    BOOST_CHECK(v->get<bool>());

    v = to<value>("F");
    BOOST_REQUIRE(v);
    BOOST_CHECK_EQUAL(v->which(), bool_value);
    BOOST_CHECK(! v->get<bool>());
  }

  // Numbers
  {
    auto v = to<value>("123456789");
    BOOST_CHECK_EQUAL(v->which(), uint_value);
    BOOST_CHECK_EQUAL(v->get<uint64_t>(), 123456789ll);

    v = to<value>("+123456789");
    BOOST_CHECK_EQUAL(v->which(), int_value);
    BOOST_CHECK_EQUAL(v->get<int64_t>(), 123456789ll);

    v = to<value>("-123456789");
    BOOST_CHECK_EQUAL(v->which(), int_value);
    BOOST_CHECK_EQUAL(v->get<int64_t>(), -123456789ll);

    v = to<value>("-123.456789");
    BOOST_CHECK_EQUAL(v->which(), double_value);
    BOOST_CHECK_EQUAL(v->get<double>(), -123.456789);
  }

  // Time ranges
  {
    auto v = to<value>("42 nsecs");
    BOOST_CHECK_EQUAL(v->which(), time_range_value);
    BOOST_CHECK_EQUAL(v->get<time_range>().count(), 42ll);

    v = to<value>("42 musec");
    BOOST_CHECK_EQUAL(v->which(), time_range_value);
    BOOST_CHECK_EQUAL(v->get<time_range>().count(), 42000ll);

    v = to<value>("-42 msec");
    BOOST_CHECK_EQUAL(v->which(), time_range_value);
    BOOST_CHECK_EQUAL(v->get<time_range>().count(), -42000000ll);

    v = to<value>("99 secs");
    BOOST_CHECK_EQUAL(v->which(), time_range_value);
    BOOST_CHECK_EQUAL(v->get<time_range>().count(), 99000000000ll);

    v = to<value>("5 mins");
    BOOST_CHECK_EQUAL(v->which(), time_range_value);
    BOOST_CHECK_EQUAL(v->get<time_range>().count(), 300000000000ll);

    v = to<value>("3 hours");
    BOOST_CHECK_EQUAL(v->which(), time_range_value);
    BOOST_CHECK_EQUAL(v->get<time_range>().count(), 10800000000000ll);

    v = to<value>("4 days");
    BOOST_CHECK_EQUAL(v->which(), time_range_value);
    BOOST_CHECK_EQUAL(v->get<time_range>().count(), 345600000000000ll);

    v = to<value>("7 weeks");
    BOOST_CHECK_EQUAL(v->which(), time_range_value);
    BOOST_CHECK_EQUAL(v->get<time_range>().count(), 4233600000000000ll);

    v = to<value>("2 months");
    BOOST_CHECK_EQUAL(v->which(), time_range_value);
    BOOST_CHECK_EQUAL(v->get<time_range>().count(), 5184000000000000ll);

    v= to<value>("-8 years");
    BOOST_CHECK_EQUAL(v->which(), time_range_value);
    BOOST_CHECK_EQUAL(v->get<time_range>().count(), -252288000000000000ll);

    // Compound durations
    v = to<value>("5m99s");
    BOOST_CHECK_EQUAL(v->which(), time_range_value);
    BOOST_CHECK_EQUAL(v->get<time_range>().count(), 399000000000ll);
  }

  // Time points
  {
    auto v = to<value>("2012-08-12+23:55:04");
    auto t = v->get<time_point>();
    BOOST_CHECK_EQUAL(t, time_point(2012, 8, 12, 23, 55, 4));

    v = to<value>("2012-08-12+00:00:00");
    BOOST_CHECK_EQUAL(v->which(), time_point_value);
    BOOST_CHECK_EQUAL(v->get<time_point>().since_epoch().count(),
                      1344729600000000000ll);

    v = to<value>("2012-08-12");
    BOOST_CHECK_EQUAL(v->get<time_point>().since_epoch().count(),
                      1344729600000000000ll);

    v = to<value>("2012-08-12+23");
    BOOST_CHECK_EQUAL(v->get<time_point>().since_epoch().count(),
                      1344812400000000000ll);

    v = to<value>("2012-08-12+23:55");
    BOOST_CHECK_EQUAL(v->get<time_point>().since_epoch().count(),
                      1344815700000000000ll);

    v = to<value>("2012-08-12+23:55:04");
    BOOST_CHECK_EQUAL(v->get<time_point>().since_epoch().count(),
                      1344815704000000000ll);
  }

  // Strings
  {
    // Escaped
    auto v = to<value>("\"new\\nline\\\"esc\"");
    BOOST_CHECK_EQUAL(v->which(), string_value);
    BOOST_CHECK_EQUAL(*v, "new\nline\"esc");
  }

  // Regexes
  {
    auto v = to<value>("/../");
    BOOST_CHECK_EQUAL(v->which(), regex_value);
    BOOST_CHECK_EQUAL(*v, regex{".."});

    v = to<value>("/\\/../");
    BOOST_CHECK_EQUAL(v->which(), regex_value);
    BOOST_CHECK_EQUAL(*v, regex{"/.."});
  }

  // Vectors
  {
    auto v = to<value>("[1, 2, 3]");
    BOOST_CHECK_EQUAL(v->which(), vector_value);
    BOOST_CHECK_EQUAL(*v, value(vector{1u, 2u, 3u}));
  }

  // Sets
  {
    auto v = to<value>("{+1, +2, +3}");
    BOOST_CHECK_EQUAL(v->which(), set_value);
    BOOST_CHECK_EQUAL(*v, value(set{1, 2, 3}));

    v = to<value>("{\"foo\", \"bar\"}");
    BOOST_CHECK_EQUAL(v->which(), set_value);
    BOOST_CHECK_EQUAL(*v, value(set{"foo", "bar"}));
  }

  // Tables
  {
    auto v = to<value>("{\"x\" -> T, \"y\" -> F}");
    BOOST_CHECK_EQUAL(v->which(), table_value);
    BOOST_CHECK_EQUAL(*v, value(table{{"x", true}, {"y", false}}));
  }

  // Records
  {
    auto v = to<value>("(\"x\", T, 42, +42)");
    BOOST_CHECK_EQUAL(v->which(), record_value);
    BOOST_CHECK_EQUAL(*v, value(record{"x", true, 42u, 42}));
  }

  // Addresses
  {
    auto v = to<value>("127.0.0.1");
    BOOST_CHECK_EQUAL(v->which(), address_value);
    BOOST_CHECK_EQUAL(*v, *address::from_v4("127.0.0.1"));

    v = to<value>("::");
    BOOST_CHECK_EQUAL(v->which(), address_value);
    BOOST_CHECK_EQUAL(*v, *address::from_v6("::"));

    v = to<value>("f00::");
    BOOST_CHECK_EQUAL(v->which(), address_value);
    BOOST_CHECK_EQUAL(*v, *address::from_v6("f00::"));
  }

  // Prefixes
  {
    auto v = to<value>("10.0.0.0/8");
    BOOST_CHECK_EQUAL(v->which(), prefix_value);
    BOOST_CHECK_EQUAL(*v, (prefix{*address::from_v4("10.0.0.0"), 8}));

    v = to<value>("2001:db8:0:0:8:800:200c:417a/64");
    BOOST_CHECK_EQUAL(v->which(), prefix_value);
    auto pfx = prefix{*address::from_v6("2001:db8:0:0:8:800:200c:417a"), 64};
    BOOST_CHECK_EQUAL(*v, pfx);
  }

  // Ports
  {
    auto v = to<value>("53/udp");
    BOOST_CHECK_EQUAL(v->which(), port_value);
    BOOST_CHECK_EQUAL(*v, (port{53, port::udp}));
  }
}

BOOST_AUTO_TEST_SUITE_END()

#define BOOST_SPIRIT_QI_DEBUG

#include "test.h"
#include "vast/detail/parser/query.h"
#include "vast/detail/parser/parse.h"
#include "vast/parse.h"

using namespace vast;

BOOST_AUTO_TEST_SUITE(parse_test_suite)

BOOST_AUTO_TEST_CASE(parse_bool)
{
  char const* str = "T";
  auto start = str;
  bool b;
  BOOST_CHECK(parse(start, str + 1, b));
  BOOST_CHECK_EQUAL(start, str + 1);
  BOOST_CHECK(b);

  str = "F";
  start = str;
  BOOST_CHECK(parse(start, str + 1, b));
  BOOST_CHECK_EQUAL(start, str + 1);
  BOOST_CHECK(! b);

  str = "x";
  start = str;
  BOOST_CHECK(! parse(start, str + 1, b));
}

BOOST_AUTO_TEST_CASE(parse_int)
{
  auto str = "-1024";
  auto start = str;
  auto end = str + 5;
  int64_t i;
  BOOST_CHECK(parse(start, end, i));
  BOOST_CHECK_EQUAL(start, end);
  BOOST_CHECK_EQUAL(i, -1024ll);

  str = "+1024";
  start = str;
  BOOST_CHECK(parse(start, end, i));
  BOOST_CHECK_EQUAL(start, end);
  BOOST_CHECK_EQUAL(i, 1024ll);

  str = "1337";
  start = str;
  end = str + 4;
  BOOST_CHECK(parse(start, end, i));
  BOOST_CHECK_EQUAL(start, end);
  BOOST_CHECK_EQUAL(i, 1337ll);
}

BOOST_AUTO_TEST_CASE(parse_uint)
{
  auto str = "1024";
  auto start = str;
  auto end = str + 4;
  uint64_t u;
  BOOST_CHECK(parse(start, end, u));
  BOOST_CHECK_EQUAL(start, end);
  BOOST_CHECK_EQUAL(u, 1024ull);

  str = "+1024";
  start = str;
  ++end;
  BOOST_CHECK(! parse(start, end, u));
}

BOOST_AUTO_TEST_CASE(parse_double)
{
  auto str = "-123.456789";
  auto start = str;
  auto end = str + std::strlen(str);
  double d;
  BOOST_CHECK(parse(start, end, d));
  BOOST_CHECK_EQUAL(start, end);
  BOOST_CHECK_EQUAL(d, -123.456789);
}

BOOST_AUTO_TEST_CASE(parse_time_range)
{
  auto str = "1000ms";
  auto start = str;
  auto end = str + 6;
  time_range r;
  BOOST_CHECK(parse(start, end, r));
  BOOST_CHECK_EQUAL(start, end);
  BOOST_CHECK_EQUAL(r, time_range::milliseconds(1000));

  str = "1000";
  start = str;
  end = str + 4;
  BOOST_CHECK(parse(start, end, r));
  BOOST_CHECK_EQUAL(start, end);
  BOOST_CHECK_EQUAL(r, time_range::seconds(1000));

  str = "123.456789";
  start = str;
  end = str + 10;
  BOOST_CHECK(parse(start, end, r));
  BOOST_CHECK_EQUAL(start, end);
  BOOST_CHECK_EQUAL(r, time_range::fractional(123.456789));

}

BOOST_AUTO_TEST_CASE(parse_time_point)
{
  time_point expected(2012, 8, 12, 23, 55, 4);
  string str("2012-08-12+23:55:04");
  BOOST_CHECK_EQUAL(time_point(to_string(str)), expected);
  time_point t;
  auto i = str.begin();
  BOOST_CHECK(parse(i, str.end(), t, time_point::format));
  BOOST_CHECK(i == str.end());
  BOOST_CHECK_EQUAL(t, expected);
}

BOOST_AUTO_TEST_CASE(parse_string)
{
  // Here is a difference: the value parser grammar expects strings with
  // double quotes whereas this version does not.
  auto str = "\"f\\oo\\\"bar\"";
  auto start = str;
  auto end = str + std::strlen(str);
  string s0;
  value v;
  BOOST_CHECK(parse(start, end, s0));
  BOOST_CHECK_EQUAL(start, end);
  BOOST_CHECK(parse(str, v));
  BOOST_CHECK_EQUAL(s0.thin("\"", "\\"), v);
}

#ifdef ZE_CLANG
BOOST_AUTO_TEST_CASE(parse_regex)
{
  {
    string str("/^\\w{3}\\w{3}\\w{3}$/");
    regex rx;
    auto i = str.begin();
    BOOST_CHECK(parse(i, str.end(), rx));
    BOOST_CHECK(i == str.end());
  }
  {
    auto str = "/foo\\+(bar){2}|\"baz\"*/";
    auto start = str;
    auto end = str + std::strlen(str);
    regex rx;
    BOOST_CHECK(parse(start, end, rx));
    BOOST_CHECK_EQUAL(start, end);
  }
}
#endif

BOOST_AUTO_TEST_CASE(parse_set)
{
  {
    string str("{1, 2, 3}");
    auto i = str.begin();
    set s;
    BOOST_CHECK(parse(i, str.end(), s, int_type));
    BOOST_CHECK(i == str.end());
    set expected{1, 2, 3};
    BOOST_CHECK_EQUAL(s, expected);
  }
  {
    string str("a--b--c");
    auto i = str.begin();
    set s;
    BOOST_CHECK(parse(i, str.end(), s, string_type, "--"));
    BOOST_CHECK(i == str.end());
    set expected{"a", "b", "c"};
    BOOST_CHECK_EQUAL(s, expected);
  }
}

BOOST_AUTO_TEST_CASE(parse_address)
{
  auto str = "192.168.0.1";
  auto start = str;
  auto end = str + std::strlen(str);
  address a;
  BOOST_CHECK(parse(start, end, a));
  BOOST_CHECK_EQUAL(start, end);
  BOOST_CHECK_EQUAL(a, address(str));

  str = "f00::cafe";
  start = str;
  end = str + std::strlen(str);
  BOOST_CHECK(parse(start, end, a));
  BOOST_CHECK_EQUAL(start, end);
  BOOST_CHECK_EQUAL(a, address(str));
}

BOOST_AUTO_TEST_CASE(parse_prefix)
{
  auto str = "192.168.0.0/24";
  auto start = str;
  auto end = str + std::strlen(str);
  prefix p;
  BOOST_CHECK(parse(start, end, p));
  BOOST_CHECK_EQUAL(start, end);
  BOOST_CHECK_EQUAL(p, prefix(address("192.168.0.0"), 24));

  str = "::/40";
  start = str;
  end = str + std::strlen(str);
  BOOST_CHECK(parse(start, end, p));
  BOOST_CHECK_EQUAL(start, end);
  BOOST_CHECK_EQUAL(p, prefix(address("::"), 40));
}

BOOST_AUTO_TEST_CASE(parse_port)
{
  {
    auto s = "22/tcp";
    auto i = s;
    auto end = s + std::strlen(s);
    port p;
    BOOST_CHECK(parse(i, end, p));
    BOOST_CHECK_EQUAL(i, end);
    BOOST_CHECK_EQUAL(p, port(22, port::tcp));
  }

  {
    auto s = "42/unknown";
    auto i = s;
    auto end = s + std::strlen(s);
    port p;
    BOOST_CHECK(parse(i, end, p));
    BOOST_CHECK_EQUAL(i, end);
    BOOST_CHECK_EQUAL(p, port(42, port::unknown));
  }

  {
    auto s = "53/udp";
    auto i = s;
    auto end = s + std::strlen(s);
    port p;
    BOOST_CHECK(parse(i, end, p));
    BOOST_CHECK_EQUAL(i, end);
    BOOST_CHECK_EQUAL(p, port(53, port::udp));
  }

  {
    auto s = "7/icmp";
    auto i = s;
    auto end = s + std::strlen(s);
    port p;
    BOOST_CHECK(parse(i, end, p));
    BOOST_CHECK_EQUAL(i, end);
    BOOST_CHECK_EQUAL(p, port(7, port::icmp));
  }
}

BOOST_AUTO_TEST_CASE(parse_value)
{
  // Booleans
  {
    value v;
    BOOST_CHECK(parse("T", v));
    BOOST_CHECK_EQUAL(v.which(), bool_type);
    BOOST_CHECK(v.get<bool>());

    parse("F", v);
    BOOST_CHECK_EQUAL(v.which(), bool_type);
    BOOST_CHECK(! v.get<bool>());
  }

  // Numbers
  {
    value v;
    BOOST_CHECK(parse("123456789", v));
    BOOST_CHECK_EQUAL(v.which(), uint_type);
    BOOST_CHECK_EQUAL(v.get<uint64_t>(), 123456789ll);

    BOOST_CHECK(parse("+123456789", v));
    BOOST_CHECK_EQUAL(v.which(), int_type);
    BOOST_CHECK_EQUAL(v.get<int64_t>(), 123456789ll);

    BOOST_CHECK(parse("-123456789", v));
    BOOST_CHECK_EQUAL(v.which(), int_type);
    BOOST_CHECK_EQUAL(v.get<int64_t>(), -123456789ll);

    BOOST_CHECK(parse("-123.456789", v));
    BOOST_CHECK_EQUAL(v.which(), double_type);
    BOOST_CHECK_EQUAL(v.get<double>(), -123.456789);
  }

  // Time ranges
  {
    value v;
    BOOST_CHECK(parse("42 nsecs", v));
    BOOST_CHECK_EQUAL(v.which(), time_range_type);
    BOOST_CHECK_EQUAL(v.get<time_range>().count(), 42ll);
    BOOST_CHECK(parse("42 musec", v));
    BOOST_CHECK_EQUAL(v.which(), time_range_type);
    BOOST_CHECK_EQUAL(v.get<time_range>().count(), 42000ll);
    BOOST_CHECK(parse("-42 msec", v));
    BOOST_CHECK_EQUAL(v.which(), time_range_type);
    BOOST_CHECK_EQUAL(v.get<time_range>().count(), -42000000ll);
    BOOST_CHECK(parse("99 secs", v));
    BOOST_CHECK_EQUAL(v.which(), time_range_type);
    BOOST_CHECK_EQUAL(v.get<time_range>().count(), 99000000000ll);
    BOOST_CHECK(parse("5 mins", v));
    BOOST_CHECK_EQUAL(v.which(), time_range_type);
    BOOST_CHECK_EQUAL(v.get<time_range>().count(), 300000000000ll);
    BOOST_CHECK(parse("3 hours", v));
    BOOST_CHECK_EQUAL(v.which(), time_range_type);
    BOOST_CHECK_EQUAL(v.get<time_range>().count(), 10800000000000ll);
    BOOST_CHECK(parse("4 days", v));
    BOOST_CHECK_EQUAL(v.which(), time_range_type);
    BOOST_CHECK_EQUAL(v.get<time_range>().count(), 345600000000000ll);
    BOOST_CHECK(parse("7 weeks", v));
    BOOST_CHECK_EQUAL(v.which(), time_range_type);
    BOOST_CHECK_EQUAL(v.get<time_range>().count(), 4233600000000000ll);
    BOOST_CHECK(parse("2 months", v));
    BOOST_CHECK_EQUAL(v.which(), time_range_type);
    BOOST_CHECK_EQUAL(v.get<time_range>().count(), 5184000000000000ll);
    BOOST_CHECK(parse("-8 years", v));
    BOOST_CHECK_EQUAL(v.which(), time_range_type);
    BOOST_CHECK_EQUAL(v.get<time_range>().count(), -252288000000000000ll);
  
    // Compound durations
    BOOST_CHECK(parse("5m99s", v));
    BOOST_CHECK_EQUAL(v.which(), time_range_type);
    BOOST_CHECK_EQUAL(v.get<time_range>().count(), 399000000000ll);
  }

  // Time points
  {
    value v;
    BOOST_CHECK(parse("2012-08-12+23:55:04", v));
    auto t = v.get<time_point>();
    BOOST_CHECK_EQUAL(t, time_point(2012, 8, 12, 23, 55, 4));

    BOOST_CHECK(parse("2012-08-12+00:00:00", v));
    BOOST_CHECK_EQUAL(v.which(), time_point_type);
    BOOST_CHECK_EQUAL(v.get<time_point>().since_epoch().count(),
                      1344729600000000000ll);
    BOOST_CHECK(parse("2012-08-12", v));
    BOOST_CHECK_EQUAL(v.get<time_point>().since_epoch().count(),
                      1344729600000000000ll);
    BOOST_CHECK(parse("2012-08-12+23", v));
    BOOST_CHECK_EQUAL(v.get<time_point>().since_epoch().count(),
                      1344812400000000000ll);
    BOOST_CHECK(parse("2012-08-12+23:55", v));
    BOOST_CHECK_EQUAL(v.get<time_point>().since_epoch().count(),
                      1344815700000000000ll);
    BOOST_CHECK(parse("2012-08-12+23:55:04", v));
    BOOST_CHECK_EQUAL(v.get<time_point>().since_epoch().count(),
                      1344815704000000000ll);
  }

  // Strings
  {
    // Escaped
    value v;
    BOOST_CHECK(parse("\"new\\nline\\\"esc\"", v));
    BOOST_CHECK_EQUAL(v.which(), string_type);
    BOOST_CHECK_EQUAL(v, "new\nline\"esc");
  }

  // Regexes
  {
    value v;
    BOOST_CHECK(parse("/../", v));
    BOOST_CHECK_EQUAL(v.which(), regex_type);
    BOOST_CHECK_EQUAL(v, regex(".."));
  
    BOOST_CHECK(parse("/\\/../", v));
    BOOST_CHECK_EQUAL(v.which(), regex_type);
    BOOST_CHECK_EQUAL(v, regex("/.."));
  }

  // Vectors
  {
    value v;
    BOOST_CHECK(parse("[1, 2, 3]", v));
    BOOST_CHECK_EQUAL(v.which(), vector_type);
    BOOST_CHECK_EQUAL(v, vector({1u, 2u, 3u}));
  }

  // Sets
  {
    value v;
    BOOST_CHECK(parse("{+1, +2, +3}", v));
    BOOST_CHECK_EQUAL(v.which(), set_type);
    BOOST_CHECK_EQUAL(v, set({1, 2, 3}));

    BOOST_CHECK(parse("{\"foo\", \"bar\"}", v));
    BOOST_CHECK_EQUAL(v.which(), set_type);
    BOOST_CHECK_EQUAL(v, set({"foo", "bar"}));
  }

  // Tables
  {
    value v;
    BOOST_CHECK(parse("{\"x\" -> T, \"y\" -> F}", v));
    BOOST_CHECK_EQUAL(v.which(), table_type);
    BOOST_CHECK_EQUAL(v, table({"x", true, "y", false}));
  }

  // Records
  {
    value v;
    BOOST_CHECK(parse("(\"x\", T, 42, +42)", v));
    BOOST_CHECK_EQUAL(v.which(), record_type);
    BOOST_CHECK_EQUAL(v, record({"x", true, 42u, 42}));
  }

  // Addresses
  {
    value v;
    BOOST_CHECK(parse("127.0.0.1", v));
    BOOST_CHECK_EQUAL(v.which(), address_type);
    BOOST_CHECK_EQUAL(v, address("127.0.0.1"));

    BOOST_CHECK(parse("::", v));
    BOOST_CHECK_EQUAL(v.which(), address_type);
    BOOST_CHECK_EQUAL(v, address("::"));

    BOOST_CHECK(parse("f00::", v));
    BOOST_CHECK_EQUAL(v.which(), address_type);
    BOOST_CHECK_EQUAL(v, address("f00::"));
  }

  // Prefixes
  {
    value v;
    BOOST_CHECK(parse("10.0.0.0/8", v));
    BOOST_CHECK_EQUAL(v.which(), prefix_type);
    BOOST_CHECK_EQUAL(v, prefix(address("10.0.0.0"), 8));

    BOOST_CHECK(parse("2001:db8:0:0:8:800:200c:417a/64", v));
    BOOST_CHECK_EQUAL(v.which(), prefix_type);
    BOOST_CHECK_EQUAL(v, prefix(address("2001:db8:0:0:8:800:200c:417a"), 64));
  }

  // Ports
  {
    value v;
    BOOST_CHECK(parse("53/udp", v));
    BOOST_CHECK_EQUAL(v.which(), port_type);
    BOOST_CHECK_EQUAL(v, port(53, port::udp));
  }
}

BOOST_AUTO_TEST_CASE(expressions)
{
  std::vector<std::string> expressions
  {
    "T",
    "53/udp",
    "192.168.0.1 + 127.0.0.1",
    "(42 - 24) / 2",
    "-(42 - 24) / 2"
  };

  detail::ast::query::expression expr;
  using detail::parser::parse;
  for (auto& e : expressions)
    BOOST_CHECK((parse<detail::parser::expression>(e, expr)));
}

BOOST_AUTO_TEST_CASE(queries)
{
  detail::ast::query::query query;
  using detail::parser::parse;

  // Type queries.
  auto q = ":port < 53/udp";
  BOOST_CHECK(parse<detail::parser::query>(q, query));
  q = ":set != {T, F}";
  BOOST_CHECK(parse<detail::parser::query>(q, query));
  q = ":addr == 192.168.0.1 && :port == 80/tcp";
  BOOST_CHECK(parse<detail::parser::query>(q, query));
  q = ":string ~ /evil.*/ && :prefix >= 10.0.0.0/8";
  BOOST_CHECK(parse<detail::parser::query>(q, query));
  q = ":addr == 1.2.3.4 ^ 5.6.7.8 || :prefix != 10.0.0.0/8";
  BOOST_CHECK(parse<detail::parser::query>(q, query));
  q = "! :int == +8 / +4 || ! :count < -(4 * 2)";
  BOOST_CHECK(parse<detail::parser::query>(q, query));

  // Event tags.
  q = "&name == \"foo\"";
  BOOST_CHECK(parse<detail::parser::query>(q, query));
  q = "&time < now - 5d10m3s";
  BOOST_CHECK(parse<detail::parser::query>(q, query));
  q = "&id == 42";
  BOOST_CHECK(parse<detail::parser::query>(q, query));

  // Offsets.
  q = "@5 in {1, 2, 3}";
  BOOST_CHECK(parse<detail::parser::query>(q, query));
  q = "@10,3 < now - 5d10m3s";
  BOOST_CHECK(parse<detail::parser::query>(q, query));
  q = "@0,3,2 ~ /yikes/";

  // Dereferencing event names.
  q = "foo$bar == T";
  BOOST_CHECK(parse<detail::parser::query>(q, query));
  q = "foo$c$id_orig == 192.168.1.1";
  BOOST_CHECK(parse<detail::parser::query>(q, query));
  q = "*$c$id_orig == 192.168.1.1";
  BOOST_CHECK(parse<detail::parser::query>(q, query));

  // In
  auto fail = ":foo == -42";
  BOOST_CHECK(! (parse<detail::parser::query>(fail, query)));
}

BOOST_AUTO_TEST_SUITE_END()

#define BOOST_SPIRIT_QI_DEBUG

#include "test.h"
#include "vast/container.h"
#include "vast/expression.h"
#include "vast/util/parse.h"
#include "vast/detail/parser/query.h"

using namespace vast;

BOOST_AUTO_TEST_SUITE(parse_test_suite)

BOOST_AUTO_TEST_CASE(parse_bool)
{
  char const* str = "T";
  auto start = str;
  bool b;
  BOOST_CHECK(extract(start, str + 1, b));
  BOOST_CHECK_EQUAL(start, str + 1);
  BOOST_CHECK(b);

  str = "F";
  start = str;
  BOOST_CHECK(extract(start, str + 1, b));
  BOOST_CHECK_EQUAL(start, str + 1);
  BOOST_CHECK(! b);

  str = "x";
  start = str;
  BOOST_CHECK(! extract(start, str + 1, b));
}

BOOST_AUTO_TEST_CASE(parse_int)
{
  auto str = "-1024";
  auto start = str;
  auto end = str + 5;
  int64_t i;
  BOOST_CHECK(extract(start, end, i));
  BOOST_CHECK_EQUAL(start, end);
  BOOST_CHECK_EQUAL(i, -1024ll);

  str = "+1024";
  start = str;
  BOOST_CHECK(extract(start, end, i));
  BOOST_CHECK_EQUAL(start, end);
  BOOST_CHECK_EQUAL(i, 1024ll);

  str = "1337";
  start = str;
  end = str + 4;
  BOOST_CHECK(extract(start, end, i));
  BOOST_CHECK_EQUAL(start, end);
  BOOST_CHECK_EQUAL(i, 1337ll);
}

BOOST_AUTO_TEST_CASE(parse_uint)
{
  auto str = "1024";
  auto start = str;
  auto end = str + 4;
  uint64_t u;
  BOOST_CHECK(extract(start, end, u));
  BOOST_CHECK_EQUAL(start, end);
  BOOST_CHECK_EQUAL(u, 1024ull);

  str = "+1024";
  start = str;
  ++end;
  BOOST_CHECK(! extract(start, end, u));
}

BOOST_AUTO_TEST_CASE(parse_double)
{
  auto str = "-123.456789";
  auto start = str;
  auto end = str + std::strlen(str);
  double d;
  BOOST_CHECK(extract(start, end, d));
  BOOST_CHECK_EQUAL(start, end);
  BOOST_CHECK_EQUAL(d, -123.456789);
}

BOOST_AUTO_TEST_CASE(parse_time_range)
{
  auto str = "1000ms";
  auto start = str;
  auto end = str + 6;
  time_range r;
  BOOST_CHECK(extract(start, end, r));
  BOOST_CHECK_EQUAL(start, end);
  BOOST_CHECK_EQUAL(r, time_range::milliseconds(1000));

  str = "1000";
  start = str;
  end = str + 4;
  BOOST_CHECK(extract(start, end, r));
  BOOST_CHECK_EQUAL(start, end);
  BOOST_CHECK_EQUAL(r, time_range::seconds(1000));

  str = "123.456789";
  start = str;
  end = str + 10;
  BOOST_CHECK(extract(start, end, r));
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
  BOOST_CHECK(extract(i, str.end(), t, time_point::format));
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
  BOOST_CHECK(extract(start, end, s0));
  BOOST_CHECK_EQUAL(start, end);
  auto v = value::parse(str);
  BOOST_CHECK(v != invalid);
  BOOST_CHECK_EQUAL(s0.thin("\"", "\\"), v);
}

#ifdef ZE_CLANG
BOOST_AUTO_TEST_CASE(parse_regex)
{
  {
    string str("/^\\w{3}\\w{3}\\w{3}$/");
    regex rx;
    auto i = str.begin();
    BOOST_CHECK(extract(i, str.end(), rx));
    BOOST_CHECK(i == str.end());
  }
  {
    auto str = "/foo\\+(bar){2}|\"baz\"*/";
    auto start = str;
    auto end = str + std::strlen(str);
    regex rx;
    BOOST_CHECK(extract(start, end, rx));
    BOOST_CHECK_EQUAL(start, end);
  }
}
#endif

BOOST_AUTO_TEST_CASE(parse_record)
{
  {
    string str("(1, 2, 3)");
    auto i = str.begin();
    record r;
    BOOST_CHECK(extract(i, str.end(), r, int_type));
    BOOST_CHECK(i == str.end());
    record expected{1, 2, 3};
    BOOST_CHECK_EQUAL(r, expected);
  }
  {
    string str("a--b--c");
    auto i = str.begin();
    record r;
    BOOST_CHECK(extract(i, str.end(), r, string_type, "--"));
    BOOST_CHECK(i == str.end());
    record expected{"a", "b", "c"};
    BOOST_CHECK_EQUAL(r, expected);
  }
}

BOOST_AUTO_TEST_CASE(parse_address)
{
  auto str = "192.168.0.1";
  auto start = str;
  auto end = str + std::strlen(str);
  address a;
  BOOST_CHECK(extract(start, end, a));
  BOOST_CHECK_EQUAL(start, end);
  BOOST_CHECK_EQUAL(a, address(str));

  str = "f00::cafe";
  start = str;
  end = str + std::strlen(str);
  BOOST_CHECK(extract(start, end, a));
  BOOST_CHECK_EQUAL(start, end);
  BOOST_CHECK_EQUAL(a, address(str));
}

BOOST_AUTO_TEST_CASE(parse_prefix)
{
  auto str = "192.168.0.0/24";
  auto start = str;
  auto end = str + std::strlen(str);
  prefix p;
  BOOST_CHECK(extract(start, end, p));
  BOOST_CHECK_EQUAL(start, end);
  BOOST_CHECK_EQUAL(p, prefix(address("192.168.0.0"), 24));

  str = "::/40";
  start = str;
  end = str + std::strlen(str);
  BOOST_CHECK(extract(start, end, p));
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
    BOOST_CHECK(extract(i, end, p));
    BOOST_CHECK_EQUAL(i, end);
    BOOST_CHECK_EQUAL(p, port(22, port::tcp));
  }

  {
    auto s = "42/unknown";
    auto i = s;
    auto end = s + std::strlen(s);
    port p;
    BOOST_CHECK(extract(i, end, p));
    BOOST_CHECK_EQUAL(i, end);
    BOOST_CHECK_EQUAL(p, port(42, port::unknown));
  }

  {
    auto s = "53/udp";
    auto i = s;
    auto end = s + std::strlen(s);
    port p;
    BOOST_CHECK(extract(i, end, p));
    BOOST_CHECK_EQUAL(i, end);
    BOOST_CHECK_EQUAL(p, port(53, port::udp));
  }

  {
    auto s = "7/icmp";
    auto i = s;
    auto end = s + std::strlen(s);
    port p;
    BOOST_CHECK(extract(i, end, p));
    BOOST_CHECK_EQUAL(i, end);
    BOOST_CHECK_EQUAL(p, port(7, port::icmp));
  }
}

BOOST_AUTO_TEST_CASE(parse_value)
{
  // Booleans
  {
    auto v = value::parse("T");
    BOOST_CHECK_EQUAL(v.which(), bool_type);
    BOOST_CHECK(v.get<bool>());

    v = value::parse("F");
    BOOST_CHECK_EQUAL(v.which(), bool_type);
    BOOST_CHECK(! v.get<bool>());
  }

  // Numbers
  {
    auto v = value::parse("123456789");
    BOOST_CHECK_EQUAL(v.which(), uint_type);
    BOOST_CHECK_EQUAL(v.get<uint64_t>(), 123456789ll);

    v = value::parse("+123456789");
    BOOST_CHECK_EQUAL(v.which(), int_type);
    BOOST_CHECK_EQUAL(v.get<int64_t>(), 123456789ll);

    v = value::parse("-123456789");
    BOOST_CHECK_EQUAL(v.which(), int_type);
    BOOST_CHECK_EQUAL(v.get<int64_t>(), -123456789ll);

    v = value::parse("-123.456789");
    BOOST_CHECK_EQUAL(v.which(), double_type);
    BOOST_CHECK_EQUAL(v.get<double>(), -123.456789);
  }

  // Time ranges
  {
    auto v = value::parse("42 nsecs");
    BOOST_CHECK_EQUAL(v.which(), time_range_type);
    BOOST_CHECK_EQUAL(v.get<time_range>().count(), 42ll);

    v = value::parse("42 musec");
    BOOST_CHECK_EQUAL(v.which(), time_range_type);
    BOOST_CHECK_EQUAL(v.get<time_range>().count(), 42000ll);

    v = value::parse("-42 msec");
    BOOST_CHECK_EQUAL(v.which(), time_range_type);
    BOOST_CHECK_EQUAL(v.get<time_range>().count(), -42000000ll);

    v = value::parse("99 secs");
    BOOST_CHECK_EQUAL(v.which(), time_range_type);
    BOOST_CHECK_EQUAL(v.get<time_range>().count(), 99000000000ll);

    v = value::parse("5 mins");
    BOOST_CHECK_EQUAL(v.which(), time_range_type);
    BOOST_CHECK_EQUAL(v.get<time_range>().count(), 300000000000ll);
    
    v = value::parse("3 hours");
    BOOST_CHECK_EQUAL(v.which(), time_range_type);
    BOOST_CHECK_EQUAL(v.get<time_range>().count(), 10800000000000ll);

    v = value::parse("4 days");
    BOOST_CHECK_EQUAL(v.which(), time_range_type);
    BOOST_CHECK_EQUAL(v.get<time_range>().count(), 345600000000000ll);

    v = value::parse("7 weeks");
    BOOST_CHECK_EQUAL(v.which(), time_range_type);
    BOOST_CHECK_EQUAL(v.get<time_range>().count(), 4233600000000000ll);

    v = value::parse("2 months");
    BOOST_CHECK_EQUAL(v.which(), time_range_type);
    BOOST_CHECK_EQUAL(v.get<time_range>().count(), 5184000000000000ll);

    v= value::parse("-8 years");
    BOOST_CHECK_EQUAL(v.which(), time_range_type);
    BOOST_CHECK_EQUAL(v.get<time_range>().count(), -252288000000000000ll);
  
    // Compound durations
    v = value::parse("5m99s");
    BOOST_CHECK_EQUAL(v.which(), time_range_type);
    BOOST_CHECK_EQUAL(v.get<time_range>().count(), 399000000000ll);
  }

  // Time points
  {
    auto v = value::parse("2012-08-12+23:55:04");
    auto t = v.get<time_point>();
    BOOST_CHECK_EQUAL(t, time_point(2012, 8, 12, 23, 55, 4));

    v = value::parse("2012-08-12+00:00:00");
    BOOST_CHECK_EQUAL(v.which(), time_point_type);
    BOOST_CHECK_EQUAL(v.get<time_point>().since_epoch().count(),
                      1344729600000000000ll);

    v = value::parse("2012-08-12");
    BOOST_CHECK_EQUAL(v.get<time_point>().since_epoch().count(),
                      1344729600000000000ll);

    v = value::parse("2012-08-12+23");
    BOOST_CHECK_EQUAL(v.get<time_point>().since_epoch().count(),
                      1344812400000000000ll);

    v = value::parse("2012-08-12+23:55");
    BOOST_CHECK_EQUAL(v.get<time_point>().since_epoch().count(),
                      1344815700000000000ll);
    
    v = value::parse("2012-08-12+23:55:04");
    BOOST_CHECK_EQUAL(v.get<time_point>().since_epoch().count(),
                      1344815704000000000ll);
  }

  // Strings
  {
    // Escaped
    auto v = value::parse("\"new\\nline\\\"esc\"");
    BOOST_CHECK_EQUAL(v.which(), string_type);
    BOOST_CHECK_EQUAL(v, "new\nline\"esc");
  }

  // Regexes
  {
    auto v = value::parse("/../");
    BOOST_CHECK_EQUAL(v.which(), regex_type);
    BOOST_CHECK_EQUAL(v, regex{".."});
  
    v = value::parse("/\\/../");
    BOOST_CHECK_EQUAL(v.which(), regex_type);
    BOOST_CHECK_EQUAL(v, regex{"/.."});
  }

  // Vectors
  {
    auto v = value::parse("[1, 2, 3]");
    BOOST_CHECK_EQUAL(v.which(), record_type);
    BOOST_CHECK_EQUAL(v, value(record{1u, 2u, 3u}));
  }

  // Sets
  {
    auto v = value::parse("{+1, +2, +3}");
    BOOST_CHECK_EQUAL(v.which(), record_type);
    BOOST_CHECK_EQUAL(v, value(record{1, 2, 3}));

    v = value::parse("{\"foo\", \"bar\"}");
    BOOST_CHECK_EQUAL(v.which(), record_type);
    BOOST_CHECK_EQUAL(v, value(record{"foo", "bar"}));
  }

  // Tables
  {
    auto v = value::parse("{\"x\" -> T, \"y\" -> F}");
    BOOST_CHECK_EQUAL(v.which(), table_type);
    BOOST_CHECK_EQUAL(v, value(table{{"x", true}, {"y", false}}));
  }

  // Records
  {
    auto v = value::parse("(\"x\", T, 42, +42)");
    BOOST_CHECK_EQUAL(v.which(), record_type);
    BOOST_CHECK_EQUAL(v, value(record{"x", true, 42u, 42}));
  }

  // Addresses
  {
    auto v = value::parse("127.0.0.1");
    BOOST_CHECK_EQUAL(v.which(), address_type);
    BOOST_CHECK_EQUAL(v, address("127.0.0.1"));

    v = value::parse("::");
    BOOST_CHECK_EQUAL(v.which(), address_type);
    BOOST_CHECK_EQUAL(v, address{"::"});

    v = value::parse("f00::");
    BOOST_CHECK_EQUAL(v.which(), address_type);
    BOOST_CHECK_EQUAL(v, address{"f00::"});
  }

  // Prefixes
  {
    auto v = value::parse("10.0.0.0/8");
    BOOST_CHECK_EQUAL(v.which(), prefix_type);
    BOOST_CHECK_EQUAL(v, (prefix{address{"10.0.0.0"}, 8}));

    v = value::parse("2001:db8:0:0:8:800:200c:417a/64");
    BOOST_CHECK_EQUAL(v.which(), prefix_type);
    BOOST_CHECK_EQUAL(v, (prefix{address{"2001:db8:0:0:8:800:200c:417a"}, 64}));
  }

  // Ports
  {
    auto v = value::parse("53/udp");
    BOOST_CHECK_EQUAL(v.which(), port_type);
    BOOST_CHECK_EQUAL(v, (port{53, port::udp}));
  }
}

// TODO: Implement constant folding.
//BOOST_AUTO_TEST_CASE(expressions)
//{
//  auto exprs =
//  {
//    "T",
//    "53/udp",
//    "192.168.0.1 + 127.0.0.1",
//    "(42 - 24) / 2",
//    "-(42 - 24) / 2",
//    "1.2.3.4 ^ 5.6.7.8
//  };
//
//  for (auto& e : exprs)
//    expression::parse(e);
//}

BOOST_AUTO_TEST_CASE(queries)
{
  // Type queries.
  auto exprs = 
  {
    ":port < 53/udp",
    ":set != {T, F}",
    ":addr == 192.168.0.1 && :port == 80/tcp",
    ":string ~ /evil.*/ && :prefix >= 10.0.0.0/8",
    ":addr == 1.2.3.4 || :prefix != 10.0.0.0/8",
    "! :int == +8 || ! :count < 4",

  // Event tags.
    "&name == \"foo\"",
    "&time < now - 5d10m3s",
    "&id == 42",

  // Offsets.
    "@5 in {1, 2, 3}",
    "@10,3 < now - 5d10m3s",
    "@0,3,2 ~ /yikes/",
  };

  for (auto& e : exprs)
    expression::parse(e); // Tests implicitly by not throwing.

  BOOST_CHECK_THROW(expression::parse(":foo == -42"), error::query);
}

BOOST_AUTO_TEST_SUITE_END()

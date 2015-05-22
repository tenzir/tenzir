#include "vast/value.h"

#define SUITE parse
#include "test.h"

using namespace vast;

TEST(value)
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
  CHECK(get<time::point>(*v)->time_since_epoch().count()
        == 1344729600000000000ll);

  v = to<value>("2012-08-12");
  REQUIRE(v);
  REQUIRE(is<time::point>(*v));
  CHECK(get<time::point>(*v)->time_since_epoch().count()
        == 1344729600000000000ll);

  v = to<value>("2012-08-12+23");
  REQUIRE(v);
  REQUIRE(is<time::point>(*v));
  CHECK(get<time::point>(*v)->time_since_epoch().count()
        == 1344812400000000000ll);

  v = to<value>("2012-08-12+23:55");
  REQUIRE(v);
  REQUIRE(is<time::point>(*v));
  CHECK(get<time::point>(*v)->time_since_epoch().count()
        == 1344815700000000000ll);

  v = to<value>("2012-08-12+23:55:04");
  REQUIRE(v);
  REQUIRE(is<time::point>(*v));
  CHECK(get<time::point>(*v)->time_since_epoch().count()
        == 1344815704000000000ll);

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

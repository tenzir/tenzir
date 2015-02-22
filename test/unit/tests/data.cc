#include "framework/unit.h"

#include "vast/data.h"
#include "vast/io/serialization.h"

using namespace vast;

SUITE("data")

TEST("time::point")
{
  auto t = time::point::utc(2012, 8, 12, 23, 55, 4);

  CHECK(t.delta() == t);
  CHECK(t.delta(30).time_since_epoch() == time::seconds(1344815734));
  CHECK(t.delta(56).time_since_epoch() == time::seconds(1344815760));
  CHECK(t.delta(60).time_since_epoch() == time::seconds(1344815764));
  CHECK(t.delta(68).time_since_epoch() == time::seconds(1344815772));
  CHECK(t.delta(123587).time_since_epoch() == time::seconds(1344939291));
  CHECK(t.delta(0, 0, 28).time_since_epoch() == time::seconds(1344916504));

  time::point u;

  // Positive offsets
  u = time::point::utc(2012, 9, 11, 23, 55, 4);
  CHECK(t.delta(0, 0, 0, 30) == u);
  u = time::point::utc(2012, 10, 11, 23, 55, 4);
  CHECK(t.delta(0, 0, 0, 60) == u);
  u = time::point::utc(2012, 11, 2, 23, 55, 4);
  CHECK(t.delta(0, 0, 0, 82) == u);
  u = time::point::utc(2012, 10, 12, 23, 55, 4);
  CHECK(t.delta(0, 0, 0, 0, 2) == u);
  u = time::point::utc(2012, 11, 4, 23, 55, 4);
  CHECK(t.delta(0, 0, 0, 84) == u);
  u = time::point::utc(2013, 1, 11, 23, 55, 4);
  CHECK(t.delta(0, 0, 0, 152) == u);
  u = time::point::utc(2012, 11, 12, 23, 55, 4);
  CHECK(t.delta(0, 0, 0, 0, 3) == u);
  u = time::point::utc(2013, 3, 12, 23, 55, 4);
  CHECK(t.delta(0, 0, 0, 0, 7) == u);
  u = time::point::utc(2018, 3, 12, 23, 55, 4);
  CHECK(t.delta(0, 0, 0, 0, 67) == u);
  u = time::point::utc(2024, 8, 12, 23, 55, 4);
  CHECK(t.delta(0, 0, 0, 0, 0, 12) == u);

  // Negative offsets
  u = time::point::utc(2012, 8, 12, 23, 55);
  CHECK(t.delta(-4) == u);
  u = time::point::utc(2012, 8, 12, 23, 54, 58);
  CHECK(t.delta(-6) == u);
  u = time::point::utc(2012, 8, 12, 23, 53, 59);
  CHECK(t.delta(-65) == u);
  u = time::point::utc(2012, 8, 12, 23, 0, 4);
  CHECK(t.delta(0, -55) == u);
  u = time::point::utc(2012, 8, 12, 21, 45, 4);
  CHECK(t.delta(0, -130) == u);
  u = time::point::utc(2012, 8, 12, 0, 55, 4);
  CHECK(t.delta(0, 0, -23) == u);
  u = time::point::utc(2012, 8, 11, 23, 55, 4);
  CHECK(t.delta(0, 0, -24) == u);
  u = time::point::utc(2012, 8, 9, 21, 55, 4);
  CHECK(t.delta(0, 0, -74) == u);
  u = time::point::utc(2012, 8, 4, 23, 55, 4);
  CHECK(t.delta(0, 0, 0, -8) == u);
  u = time::point::utc(2012, 8, 1, 23, 55, 4);
  CHECK(t.delta(0, 0, 0, -11) == u);
  u = time::point::utc(2012, 7, 31, 23, 55, 4);
  CHECK(t.delta(0, 0, 0, -12) == u);
  u = time::point::utc(2012, 7, 29, 23, 55, 4);
  CHECK(t.delta(0, 0, 0, -14) == u);
  u = time::point::utc(2012, 7, 1, 23, 55, 4);
  CHECK(t.delta(0, 0, 0, -42) == u);
  u = time::point::utc(2012, 6, 30, 23, 55, 4);
  CHECK(t.delta(0, 0, 0, -43) == u);
  u = time::point::utc(2011, 8, 12, 23, 55, 4);
  CHECK(t.delta(0, 0, 0, -366) == u);
  u = time::point::utc(2012, 5, 12, 23, 55, 4);
  CHECK(t.delta(0, 0, 0, 0, -3) == u);
  u = time::point::utc(2012, 1, 12, 23, 55, 4);
  CHECK(t.delta(0, 0, 0, 0, -7) == u);
  u = time::point::utc(2011, 8, 12, 23, 55, 4);
  CHECK(t.delta(0, 0, 0, 0, -12) == u);
  u = time::point::utc(2011, 7, 12, 23, 55, 4);
  CHECK(t.delta(0, 0, 0, 0, -13) == u);
  u = time::point::utc(2010, 12, 12, 23, 55, 4);
  CHECK(t.delta(0, 0, 0, 0, -20) == u);

  auto str = to<std::string>(u, "%Y-%m");
  REQUIRE(str);
  CHECK(*str == "2010-12");

  str = to<std::string>(u, "%H:%M:%S");
  REQUIRE(str);
  CHECK(*str == "23:55:04");

  auto d = to<data>("@1398933902.686337s");
  REQUIRE(d);
  auto tp = get<time::point>(*d);
  REQUIRE(tp);
  CHECK(*tp == time::fractional(1398933902.686337));
  CHECK(to_string(*tp) == "2014-05-01+08:45:02");
}

TEST("patterns")
{
  std::string str = "1";
  CHECK(pattern("[0-9]").match(str));
  CHECK(! pattern("[^1]").match(str));

  str = "foobarbaz";
  CHECK(pattern("bar").search(str));
  CHECK(! pattern("^bar$").search(str));
  CHECK(pattern("^\\w{3}\\w{3}\\w{3}$").match(str));

  CHECK(pattern::glob("foo*baz").match(str));
  CHECK(pattern::glob("foo???baz").match(str));

  str = "Holla die Waldfee!";
  pattern p{"\\w+ die Waldfe{2}."};
  CHECK(p.match(str));
  CHECK(p.search(str));

  p = pattern("(\\w+ )");
  CHECK(! p.match(str));
  CHECK(p.search(str));

  CHECK(to_string(p) == "/(\\w+ )/");
}

TEST("addresses (IPv4)")
{
  address x;
  address y;
  CHECK(x == y);
  CHECK(! x.is_v4());
  CHECK(x.is_v6());

  auto a = *to<address>("172.16.7.1");
  CHECK(to_string(a) == "172.16.7.1");
  CHECK(a.is_v4());
  CHECK(! a.is_v6());
  CHECK(! a.is_loopback());
  CHECK(! a.is_multicast());
  CHECK(! a.is_broadcast());

  auto localhost = *to<address>("127.0.0.1");
  CHECK(to_string(localhost) == "127.0.0.1");
  CHECK(localhost.is_v4());
  CHECK(localhost.is_loopback());
  CHECK(! localhost.is_multicast());
  CHECK(! localhost.is_broadcast());

  // Lexicalgraphical comparison.
  CHECK(localhost < a);

  // Bitwise operations
  address anded = a & localhost;
  address ored = a | localhost;
  address xored = a ^ localhost;
  CHECK(anded == *to<address>("44.0.0.1"));
  CHECK(ored == *to<address>("255.16.7.1"));
  CHECK(xored == *to<address>("211.16.7.0"));
  CHECK(anded.is_v4());
  CHECK(ored.is_v4());
  CHECK(xored.is_v4());

  auto broadcast = *to<address>("255.255.255.255");
  CHECK(broadcast.is_broadcast());

  uint32_t n = 3232235691;
  address b{&n, address::ipv4, address::host};
  CHECK(to_string(b) == "192.168.0.171");
}

TEST("addresses (IPv6)")
{
  CHECK(address() == *to<address>("::"));

  auto a = *to<address>("2001:db8:0000:0000:0202:b3ff:fe1e:8329");
  auto b = *to<address>("2001:db8:0:0:202:b3ff:fe1e:8329");
  auto c = *to<address>("2001:db8::202:b3ff:fe1e:8329");
  CHECK(a.is_v6() && b.is_v6() && c.is_v6());
  CHECK(! (a.is_v4() || b.is_v4() || c.is_v4()));
  CHECK(a == b && b == c);

  auto d = *to<address>("ff01::1");
  CHECK(d.is_multicast());

  CHECK((a ^ b) == *to<address>("::"));
  CHECK((a & b) == a);
  CHECK((a | b) == a);
  CHECK((a & d) == *to<address>("2001::1"));
  CHECK((a | d) == *to<address>("ff01:db8::202:b3ff:fe1e:8329"));
  CHECK((a ^ d) == *to<address>("df00:db8::202:b3ff:fe1e:8328"));

  uint8_t raw8[16] = { 0xdf, 0x00, 0x0d, 0xb8,
  0x00, 0x00, 0x00, 0x00,
  0x02, 0x02, 0xb3, 0xff,
  0xfe, 0x1e, 0x83, 0x28 };
  auto p = reinterpret_cast<uint32_t const*>(raw8);
  address e(p, address::ipv6, address::network);
  CHECK(e == (a ^ d));

  uint32_t raw32[4] = { 0xdf000db8, 0x00000000, 0x0202b3ff, 0xfe1e8328 };
  p = reinterpret_cast<uint32_t const*>(raw32);
  address f(p, address::ipv6, address::host);
  CHECK(f == (a ^ d));
  CHECK(f == e);

  CHECK(! a.mask(129));
  CHECK(a.mask(128)); // No modification
  CHECK(a == *to<address>("2001:db8:0000:0000:0202:b3ff:fe1e:8329"));
  CHECK(a.mask(112));
  CHECK(a == *to<address>("2001:db8::202:b3ff:fe1e:0"));
  CHECK(a.mask(100));
  CHECK(a == *to<address>("2001:db8::202:b3ff:f000:0"));
  CHECK(a.mask(64));
  CHECK(a == *to<address>("2001:db8::"));
  CHECK(a.mask(3));
  CHECK(a == *to<address>("2000::"));
  CHECK(a.mask(0));
  CHECK(a == *to<address>("::"));
}

TEST("subnets")
{
  subnet p;
  CHECK(p.network() == *to<address>("::"));
  CHECK(p.length() == 0);
  CHECK(to_string(p) == "::/0");

  auto a = *to<address>("192.168.0.1");
  subnet q{a, 24};
  CHECK(q.network() == *to<address>("192.168.0.0"));
  CHECK(q.length() == 24);
  CHECK(to_string(q) == "192.168.0.0/24");
  CHECK(q.contains(*to<address>("192.168.0.73")));
  CHECK(! q.contains(*to<address>("192.168.244.73")));

  auto b = *to<address>("2001:db8:0000:0000:0202:b3ff:fe1e:8329");
  subnet r{b, 64};
  CHECK(r.length() == 64);
  CHECK(r.network() == *to<address>("2001:db8::"));
  CHECK(to_string(r) == "2001:db8::/64");
  CHECK(r.contains(*to<address>("2001:db8::cafe:babe")));
  CHECK(! r.contains(*to<address>("ff00::")));
}

TEST("ports")
{
  port p;
  CHECK(p.number() == 0u);
  CHECK(p.type() == port::unknown);

  p = port(22u, port::tcp);
  CHECK(p.number() == 22u);
  CHECK(p.type() == port::tcp);

  port q(53u, port::udp);
  CHECK(q.number() == 53u);
  CHECK(q.type() == port::udp);

  CHECK(p != q);
  CHECK(p < q);
}

TEST("tables")
{
  table ports{{"ssh", 22u}, {"http", 80u}, {"https", 443u}, {"imaps", 993u}};
  CHECK(ports.size() == 4);

  auto i = ports.find("ssh");
  REQUIRE(i != ports.end());
  CHECK(i->second == 22u);
  i = ports.find("imaps");
  REQUIRE(i != ports.end());
  CHECK(i->second == 993u);

  CHECK(ports.emplace("telnet", 23u).second);
  CHECK(! ports.emplace("http", 8080u).second);
}

TEST("records")
{
  record r{"foo", -42, 1001u, "x", port{443, port::tcp}};
  record s{100, "bar", r};
  CHECK(r.size() == 5);

  CHECK(*s.at(offset{0}) == 100);
  CHECK(*s.at(offset{1}) == "bar");
  CHECK(*s.at(offset{2}) == r);
  CHECK(*s.at(offset{2, 3}) == data{"x"});

  auto structured =
    record{"foo", record{-42, record{1001u}}, "x", port{443, port::tcp}};

  auto t = type::record{{
        {"foo", type::string{}},
        {"r0", type::record{{
          {"i", type::integer{}},
          {"r1", type::record{{
            {"c", type::count{}}}}}}}},
        {"bar", type::string{}},
        {"baz", type::port{}}
      }};

  auto attempt = r.unflatten(t);
  REQUIRE(attempt);
  CHECK(*attempt == structured);
  CHECK(congruent(t, type::derive(structured)));

  std::vector<data> each;
  auto flat = record{"foo", -42, 1001u, "x", port{443, port::tcp}};
  for (auto& i : record::each{structured})
    each.push_back(*i);
  CHECK(each == flat);
}

// An *invalid* value has neither a type nor data.
// This is the default-constructed state.
TEST("invalid data")
{
  data d;
  CHECK(is<none>(d));
}

TEST("data construction")
{
  CHECK(is<none>(data{}));
  CHECK(is<boolean>(data{true}));
  CHECK(is<boolean>(data{false}));
  CHECK(is<integer>(data{0}));
  CHECK(is<integer>(data{42}));
  CHECK(is<integer>(data{-42}));
  CHECK(is<count>(data{42u}));
  CHECK(is<real>(data{4.2}));
  CHECK(is<std::string>(data{"foo"}));
  CHECK(is<std::string>(data{std::string{"foo"}}));
  CHECK(is<pattern>(data{pattern{"foo"}}));
  CHECK(is<address>(data{address{}}));
  CHECK(is<subnet>(data{subnet{}}));
  CHECK(is<port>(data{port{53, port::udp}}));
  CHECK(is<vector>(data{vector{}}));
  CHECK(is<set>(data{set{}}));
  CHECK(is<table>(data{table{}}));
  CHECK(is<record>(data{record{}}));
}

TEST("relational operators")
{
  data d1;
  data d2;
  CHECK(d1 == d2);
  CHECK(! (d1 < d2));
  CHECK(! (d1 <= d2));
  CHECK(! (d1 >= d2));
  CHECK(! (d1 > d2));

  d2 = 42;
  CHECK(d1 != d2);
  CHECK(! (d1 < d2));
  CHECK(! (d1 <= d2));
  CHECK(! (d1 >= d2));
  CHECK(! (d1 > d2));

  d1 = 42;
  d2 = nil;
  CHECK(d1 != d2);
  CHECK(! (d1 < d2));
  CHECK(! (d1 <= d2));
  CHECK(! (d1 >= d2));
  CHECK(! (d1 > d2));

  d2 = 1377;
  CHECK(d1 != d2);
  CHECK(d1 < d2);
  CHECK(d1 <= d2);
  CHECK(! (d1 >= d2));
  CHECK(! (d1 > d2));
}

TEST("predicate evaluation")
{
  data lhs{"foo"};
  data rhs{"foobar"};
  CHECK(data::evaluate(lhs, in, rhs));
  CHECK(data::evaluate(rhs, not_in, lhs));
  CHECK(data::evaluate(rhs, ni, lhs));
  CHECK(data::evaluate(rhs, not_in, lhs));

  lhs = count{42};
  rhs = count{1337};
  CHECK(data::evaluate(lhs, less_equal, rhs));
  CHECK(data::evaluate(lhs, less, rhs));
  CHECK(data::evaluate(lhs, not_equal, rhs));
  CHECK(! data::evaluate(lhs, equal, rhs));

  lhs = *to<data>("10.0.0.1");
  rhs = *to<data>("10.0.0.0/8");
  CHECK(data::evaluate(lhs, in, rhs));

  rhs = real{4.2};
  CHECK(! data::evaluate(lhs, equal, rhs));
  CHECK(data::evaluate(lhs, not_equal, rhs));
}

TEST("serialization")
{
  set s;
  s.emplace(port{80, port::tcp});
  s.emplace(port{53, port::udp});
  s.emplace(port{8, port::icmp});

  data d0{s};
  data d1;
  std::vector<uint8_t> buf;
  io::archive(buf, d0);
  io::unarchive(buf, d1);

  CHECK(d0 == d1);
  CHECK(to_string(d1) == "{8/icmp, 53/udp, 80/tcp}");
}

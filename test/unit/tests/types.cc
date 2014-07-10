#include "framework/unit.h"

#include "vast/file_system.h"
#include "vast/value.h"

SUITE("value types")

using namespace vast;

TEST("time_point")
{
  time_point t(2012, 8, 12, 23, 55, 4);

  CHECK(t.delta() == t);
  CHECK(t.delta(30) == time_range::seconds(1344815734));
  CHECK(t.delta(56) == time_range::seconds(1344815760));
  CHECK(t.delta(60) == time_range::seconds(1344815764));
  CHECK(t.delta(68) == time_range::seconds(1344815772));
  CHECK(t.delta(123587) == time_range::seconds(1344939291));
  CHECK(t.delta(0, 0, 28) == time_range::seconds(1344916504));

  time_point u;

  // Positive offsets
  u = time_point(2012, 9, 11, 23, 55, 4);
  CHECK(t.delta(0, 0, 0, 30) == u);
  u = time_point(2012, 10, 11, 23, 55, 4);
  CHECK(t.delta(0, 0, 0, 60) == u);
  u = time_point(2012, 11, 2, 23, 55, 4);
  CHECK(t.delta(0, 0, 0, 82) == u);
  u = time_point(2012, 10, 12, 23, 55, 4);
  CHECK(t.delta(0, 0, 0, 0, 2) == u);
  u = time_point(2012, 11, 4, 23, 55, 4);
  CHECK(t.delta(0, 0, 0, 84) == u);
  u = time_point(2013, 1, 11, 23, 55, 4);
  CHECK(t.delta(0, 0, 0, 152) == u);
  u = time_point(2012, 11, 12, 23, 55, 4);
  CHECK(t.delta(0, 0, 0, 0, 3) == u);
  u = time_point(2013, 3, 12, 23, 55, 4);
  CHECK(t.delta(0, 0, 0, 0, 7) == u);
  u = time_point(2018, 3, 12, 23, 55, 4);
  CHECK(t.delta(0, 0, 0, 0, 67) == u);
  u = time_point(2024, 8, 12, 23, 55, 4);
  CHECK(t.delta(0, 0, 0, 0, 0, 12) == u);

  // Negative offsets
  u = time_point(2012, 8, 12, 23, 55);
  CHECK(t.delta(-4) == u);
  u = time_point(2012, 8, 12, 23, 54, 58);
  CHECK(t.delta(-6) == u);
  u = time_point(2012, 8, 12, 23, 53, 59);
  CHECK(t.delta(-65) == u);
  u = time_point(2012, 8, 12, 23, 0, 4);
  CHECK(t.delta(0, -55) == u);
  u = time_point(2012, 8, 12, 21, 45, 4);
  CHECK(t.delta(0, -130) == u);
  u = time_point(2012, 8, 12, 0, 55, 4);
  CHECK(t.delta(0, 0, -23) == u);
  u = time_point(2012, 8, 11, 23, 55, 4);
  CHECK(t.delta(0, 0, -24) == u);
  u = time_point(2012, 8, 9, 21, 55, 4);
  CHECK(t.delta(0, 0, -74) == u);
  u = time_point(2012, 8, 4, 23, 55, 4);
  CHECK(t.delta(0, 0, 0, -8) == u);
  u = time_point(2012, 8, 1, 23, 55, 4);
  CHECK(t.delta(0, 0, 0, -11) == u);
  u = time_point(2012, 7, 31, 23, 55, 4);
  CHECK(t.delta(0, 0, 0, -12) == u);
  u = time_point(2012, 7, 29, 23, 55, 4);
  CHECK(t.delta(0, 0, 0, -14) == u);
  u = time_point(2012, 7, 1, 23, 55, 4);
  CHECK(t.delta(0, 0, 0, -42) == u);
  u = time_point(2012, 6, 30, 23, 55, 4);
  CHECK(t.delta(0, 0, 0, -43) == u);
  u = time_point(2011, 8, 12, 23, 55, 4);
  CHECK(t.delta(0, 0, 0, -366) == u);
  u = time_point(2012, 5, 12, 23, 55, 4);
  CHECK(t.delta(0, 0, 0, 0, -3) == u);
  u = time_point(2012, 1, 12, 23, 55, 4);
  CHECK(t.delta(0, 0, 0, 0, -7) == u);
  u = time_point(2011, 8, 12, 23, 55, 4);
  CHECK(t.delta(0, 0, 0, 0, -12) == u);
  u = time_point(2011, 7, 12, 23, 55, 4);
  CHECK(t.delta(0, 0, 0, 0, -13) == u);
  u = time_point(2010, 12, 12, 23, 55, 4);
  CHECK(t.delta(0, 0, 0, 0, -20) == u);

  auto str = to<std::string>(u, "%Y-%m");
  REQUIRE(str);
  CHECK(*str == "2010-12");

  str = to<std::string>(u, "%H:%M:%S");
  REQUIRE(str);
  CHECK(*str == "23:55:04");
}

TEST("strings")
{
  string c('c');
  CHECK(c == "c");

  string a("foo");
  string b("bar");
  string ab = a + b;
  CHECK(ab == "foobar");
  CHECK(ab == (a + "bar"));
  CHECK(ab == ("foo" + b));

  auto str = string("foo\tbar\rbaz ");
  auto escaped = str.escape();
  CHECK(escaped == "foo\\x09bar\\x0dbaz ");
  CHECK(str == escaped.unescape());
  CHECK(str == str.escape(true).unescape());
  str = "\\x2a";
  CHECK(str.is_escape_seq(str.begin()));
  CHECK(str.escape() == "\\x5cx2a");
  CHECK(str.escape().unescape() == str);
  CHECK(str.escape(true).unescape() == str);

  str = "ai caramba";
  CHECK(str.substr(0, 2) == "ai");
  CHECK(str.substr(3, 100) == "caramba");
  CHECK(str.substr(3, 7) == "caramba");
  CHECK(str.substr(3) == "caramba");
  CHECK(str.substr(20, 7) == "");

  CHECK(str.sub("a", "o") == "oi caramba");
  CHECK(str.sub("car", "dog") == "ai dogamba");
  CHECK(str.gsub("ai", "mai") == "mai caramba");
  CHECK(str.gsub("a", "o") == "oi coromba");
  auto z = string("foo bar foo baz foo qux");
  CHECK(z.gsub("foo", "quux") == "quux bar quux baz quux qux");

  str = "yo,my,bud";
  auto pos = str.split(",");
  REQUIRE(pos.size() == 3);
  string s0(pos[0].first, pos[0].second);
  string s1(pos[1].first, pos[1].second);
  string s2(pos[2].first, pos[2].second);
  CHECK(s0 == "yo");
  CHECK(s1 == "my");
  CHECK(s2 == "bud");

  str = "foo, bar|, baz, qux";
  pos = str.split(", ", "|");
  REQUIRE(pos.size() == 3);
  s0 = {pos[0].first, pos[0].second};
  s1 = {pos[1].first, pos[1].second};
  s2 = {pos[2].first, pos[2].second};
  CHECK(s0 == "foo");
  CHECK(s1, "bar| == baz");
  CHECK(s2 == "qux");

  str = "foo--bar||--baz--qux--corge";
  pos = str.split("--", "||", 3, true);
  REQUIRE(pos.size() == 5);
  s0 = {pos[0].first, pos[0].second};
  s1 = {pos[1].first, pos[1].second};
  s2 = {pos[2].first, pos[2].second};
  string s3{pos[3].first, pos[3].second};
  string s4{pos[4].first, pos[4].second};
  CHECK(s0 == "foo");
  CHECK(s1 == "--");
  CHECK(s2 == "bar||--baz");
  CHECK(s3 == s1);
  CHECK(s4 == "qux--corge");

  str = "  x  ";
  CHECK(str.trim() == "x");
  CHECK(str.trim("  ") == "x");
  CHECK(str.trim("   ") == "  x  ");

  str = "the needle in the haystack";
  CHECK(str.find("needle") == 4);
  CHECK(str.find("the", 3) == 14);
  CHECK(str.find("t") == 0);
  CHECK(str.find("k") == str.size() - 1);
  CHECK(str.find("fox") == string::npos);

  CHECK(str.rfind("", 4) == string::npos);
  CHECK(str.rfind("t", 0) == string::npos);
  CHECK(str.rfind("t") == 22);
  CHECK(str.rfind("needle") == 4);
  CHECK(str.rfind("the") == 14);
  CHECK(str.rfind("the needle") == 0);

  CHECK(str.starts_with("the needle"));
  CHECK(str.ends_with("the haystack"));
  CHECK(! str.ends_with("the yarn"));
  CHECK(! str.ends_with("a haystack"));

  str = "XXXaaa--XXXbbb---XXXX";
  CHECK(str.thin("XXX") == "aaa--bbb---X");
  str = "/http:\\/\\/www.bro-ids.org/";
  CHECK(str.thin("/") == "http:\\\\www.bro-ids.org");
  CHECK(str.thin("/", "\\") == "http://www.bro-ids.org");

  //str = "123456";
  //CHECK(*to<int>(str) == 123456);
  //CHECK(*to<long>(str) == 123456);
  //CHECK(*to<unsigned long>(str) == 123456);

  //str = "0x2a";
  //CHECK(*to<unsigned int>(str) == 42);

  //str = "0.0042";
  //CHECK(*to<double>(str) == 0.0042);
}

TEST("regexes")
{
  {
    std::string str = "1";
    CHECK(regex("[0-9]").match(str));
    CHECK(! regex("[^1]").match(str));

    str = "foobarbaz";
    CHECK(regex("bar").search(str));
    CHECK(! regex("^bar$").search(str));
    CHECK(regex("^\\w{3}\\w{3}\\w{3}$").match(str));

    std::string built;
    regex(std::string{"\\w+"}).match(
        str,
        [&built](std::string const& s) { built += s; });

    CHECK(str == built);

    CHECK(regex::glob("foo*baz").match(str));
    CHECK(regex::glob("foo???baz").match(str));
  }

  {
    string str("Holla die Waldfee!");
    regex rx("\\w+ die Waldfe{2}.");
    CHECK(rx.match(str));
    CHECK(rx.search(str));

    rx = regex("(\\w+ )");
    CHECK(! rx.match(str));
    CHECK(rx.search(str));

    CHECK(to_string(rx) == "/(\\w+ )/");
  }
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
  record r{"foo", -42, 1001u, 'x', port{443, port::tcp}};
  record s{100, "bar", r};
  CHECK(r.size() == 5);

  CHECK(*s.at({0}) == 100);
  CHECK(*s.at({1}) == "bar");
  CHECK(*s.at({2}) == value(r));
  CHECK(*s.at({2, 3}) == 'x');

  CHECK(s.flat_size() == r.size() + 2);
  CHECK(*s.flat_at(0) == 100);
  CHECK(*s.flat_at(1) == "bar");
  CHECK(*s.flat_at(2) == "foo");
  CHECK(s.flat_at(6)->which() == port_value);
  CHECK(s.flat_at(7) == nullptr);

  std::vector<offset> expected =
  {
    {0},
    {1},
    {2, 0},
    {2, 1},
    {2, 2},
    {2, 3},
    {2, 4}
  };

  std::vector<offset> offsets;
  s.each_offset(
      [&](value const&, offset const& o)
      {
        offsets.push_back(o);
      });

  CHECK(offsets == expected);
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

  a.mask(112);
  CHECK(a == *to<address>("2001:db8::202:b3ff:fe1e:0"));
  a.mask(100);
  CHECK(a == *to<address>("2001:db8::202:b3ff:f000:0"));
  a.mask(3);
  CHECK(a == *to<address>("2000::"));
}

TEST("prefixes")
{
  prefix p;
  CHECK(p.network() == *to<address>("::"));
  CHECK(p.length() == 0);
  CHECK(to_string(p) == "::/0");

  auto a = *to<address>("192.168.0.1");
  prefix q{a, 24};
  CHECK(q.network() == *to<address>("192.168.0.0"));
  CHECK(q.length() == 24);
  CHECK(to_string(q) == "192.168.0.0/24");
  CHECK(q.contains(*to<address>("192.168.0.73")));
  CHECK(! q.contains(*to<address>("192.168.244.73")));

  auto b = *to<address>("2001:db8:0000:0000:0202:b3ff:fe1e:8329");
  prefix r{b, 64};
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

TEST("paths")
{
  path p(".");
  CHECK(p.basename() == ".");
  CHECK(p.extension() == ".");
  CHECK(p.parent() == "");
  p = "..";
  CHECK(p.basename() == "..");
  CHECK(p.extension() == ".");
  CHECK(p.parent() == "");
  p = "/";
  CHECK(p.basename() == "/");
  CHECK(p.extension() == "");
  CHECK(p.parent() == "");
  p = "foo";
  CHECK(p.basename() == "foo");
  CHECK(p.extension() == "");
  CHECK(p.parent() == "");
  p = "/foo";
  CHECK(p.basename() == "foo");
  CHECK(p.extension() == "");
  CHECK(p.parent() == "/");
  p = "foo/";
  CHECK(p.basename() == ".");
  CHECK(p.extension() == "");
  CHECK(p.parent() == "foo");
  p = "/foo/";
  CHECK(p.basename() == ".");
  CHECK(p.extension() == "");
  CHECK(p.parent() == "/foo");
  p = "foo/bar";
  CHECK(p.basename() == "bar");
  CHECK(p.extension() == "");
  CHECK(p.parent() == "foo");
  p = "/foo/bar";
  CHECK(p.basename() == "bar");
  CHECK(p.extension() == "");
  CHECK(p.parent() == "/foo");
  p = "/.";
  CHECK(p.basename() == ".");
  CHECK(p.extension() == ".");
  CHECK(p.parent() == "/");
  p = "./";
  CHECK(p.basename() == ".");
  CHECK(p.extension() == "");
  CHECK(p.parent() == ".");
  p = "/..";
  CHECK(p.basename() == "..");
  CHECK(p.extension() == ".");
  CHECK(p.parent() == "/");
  p = "../";
  CHECK(p.basename() == ".");
  CHECK(p.extension() == "");
  CHECK(p.parent() == "..");
  p = "foo/.";
  CHECK(p.basename() == ".");
  CHECK(p.extension() == ".");
  CHECK(p.parent() == "foo");
  p = "foo/..";
  CHECK(p.basename() == "..");
  CHECK(p.extension() == ".");
  CHECK(p.parent() == "foo");
  p = "foo/./";
  CHECK(p.basename() == ".");
  CHECK(p.extension() == "");
  CHECK(p.parent() == "foo/.");
  p = "foo/../";
  CHECK(p.basename() == ".");
  CHECK(p.extension() == "");
  CHECK(p.parent() == "foo/..");
  p = "foo/./bar";
  CHECK(p.basename() == "bar");
  CHECK(p.extension() == "");
  CHECK(p.parent() == "foo/.");
}

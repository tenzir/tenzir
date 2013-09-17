#include "test.h"

#include "vast/container.h"
#include "vast/file_system.h"
#include "vast/port.h"
#include "vast/prefix.h"
#include "vast/time.h"
#include "vast/string.h"
#include "vast/regex.h"

using namespace vast;

BOOST_AUTO_TEST_SUITE(type_test_suite)

BOOST_AUTO_TEST_CASE(time_points)
{
  time_point t(2012, 8, 12, 23, 55, 4);

  BOOST_CHECK_EQUAL(t.delta(), t);
  BOOST_CHECK_EQUAL(t.delta(30), time_range::seconds(1344815734));
  BOOST_CHECK_EQUAL(t.delta(56), time_range::seconds(1344815760));
  BOOST_CHECK_EQUAL(t.delta(60), time_range::seconds(1344815764));
  BOOST_CHECK_EQUAL(t.delta(68), time_range::seconds(1344815772));
  BOOST_CHECK_EQUAL(t.delta(123587), time_range::seconds(1344939291));
  BOOST_CHECK_EQUAL(t.delta(0, 0, 28), time_range::seconds(1344916504));

  time_point u;

  // Positive offsets
  u = time_point(2012, 9, 11, 23, 55, 4);
  BOOST_CHECK_EQUAL(t.delta(0, 0, 0, 30), u);
  u = time_point(2012, 10, 11, 23, 55, 4);
  BOOST_CHECK_EQUAL(t.delta(0, 0, 0, 60), u);
  u = time_point(2012, 11, 2, 23, 55, 4);
  BOOST_CHECK_EQUAL(t.delta(0, 0, 0, 82), u);
  u = time_point(2012, 10, 12, 23, 55, 4);
  BOOST_CHECK_EQUAL(t.delta(0, 0, 0, 0, 2), u);
  u = time_point(2012, 11, 4, 23, 55, 4);
  BOOST_CHECK_EQUAL(t.delta(0, 0, 0, 84), u);
  u = time_point(2013, 1, 11, 23, 55, 4);
  BOOST_CHECK_EQUAL(t.delta(0, 0, 0, 152), u);
  u = time_point(2012, 11, 12, 23, 55, 4);
  BOOST_CHECK_EQUAL(t.delta(0, 0, 0, 0, 3), u);
  u = time_point(2013, 3, 12, 23, 55, 4);
  BOOST_CHECK_EQUAL(t.delta(0, 0, 0, 0, 7), u);
  u = time_point(2018, 3, 12, 23, 55, 4);
  BOOST_CHECK_EQUAL(t.delta(0, 0, 0, 0, 67), u);
  u = time_point(2024, 8, 12, 23, 55, 4);
  BOOST_CHECK_EQUAL(t.delta(0, 0, 0, 0, 0, 12), u);

  // Negative offsets
  u = time_point(2012, 8, 12, 23, 55);
  BOOST_CHECK_EQUAL(t.delta(-4), u);
  u = time_point(2012, 8, 12, 23, 54, 58);
  BOOST_CHECK_EQUAL(t.delta(-6), u);
  u = time_point(2012, 8, 12, 23, 53, 59);
  BOOST_CHECK_EQUAL(t.delta(-65), u);
  u = time_point(2012, 8, 12, 23, 0, 4);
  BOOST_CHECK_EQUAL(t.delta(0, -55), u);
  u = time_point(2012, 8, 12, 21, 45, 4);
  BOOST_CHECK_EQUAL(t.delta(0, -130), u);
  u = time_point(2012, 8, 12, 0, 55, 4);
  BOOST_CHECK_EQUAL(t.delta(0, 0, -23), u);
  u = time_point(2012, 8, 11, 23, 55, 4);
  BOOST_CHECK_EQUAL(t.delta(0, 0, -24), u);
  u = time_point(2012, 8, 9, 21, 55, 4);
  BOOST_CHECK_EQUAL(t.delta(0, 0, -74), u);
  u = time_point(2012, 8, 4, 23, 55, 4);
  BOOST_CHECK_EQUAL(t.delta(0, 0, 0, -8), u);
  u = time_point(2012, 8, 1, 23, 55, 4);
  BOOST_CHECK_EQUAL(t.delta(0, 0, 0, -11), u);
  u = time_point(2012, 7, 31, 23, 55, 4);
  BOOST_CHECK_EQUAL(t.delta(0, 0, 0, -12), u);
  u = time_point(2012, 7, 29, 23, 55, 4);
  BOOST_CHECK_EQUAL(t.delta(0, 0, 0, -14), u);
  u = time_point(2012, 7, 1, 23, 55, 4);
  BOOST_CHECK_EQUAL(t.delta(0, 0, 0, -42), u);
  u = time_point(2012, 6, 30, 23, 55, 4);
  BOOST_CHECK_EQUAL(t.delta(0, 0, 0, -43), u);
  u = time_point(2011, 8, 12, 23, 55, 4);
  BOOST_CHECK_EQUAL(t.delta(0, 0, 0, -366), u);
  u = time_point(2012, 5, 12, 23, 55, 4);
  BOOST_CHECK_EQUAL(t.delta(0, 0, 0, 0, -3), u);
  u = time_point(2012, 1, 12, 23, 55, 4);
  BOOST_CHECK_EQUAL(t.delta(0, 0, 0, 0, -7), u);
  u = time_point(2011, 8, 12, 23, 55, 4);
  BOOST_CHECK_EQUAL(t.delta(0, 0, 0, 0, -12), u);
  u = time_point(2011, 7, 12, 23, 55, 4);
  BOOST_CHECK_EQUAL(t.delta(0, 0, 0, 0, -13), u);
  u = time_point(2010, 12, 12, 23, 55, 4);
  BOOST_CHECK_EQUAL(t.delta(0, 0, 0, 0, -20), u);
}

BOOST_AUTO_TEST_CASE(strings)
{
  string c('c');
  BOOST_CHECK_EQUAL(c, "c");

  string a("foo");
  string b("bar");
  string ab = a + b;
  BOOST_CHECK_EQUAL(ab, "foobar");
  BOOST_CHECK_EQUAL(ab, a + "bar");
  BOOST_CHECK_EQUAL(ab, "foo" + b);

  auto str = string("foo\tbar\rbaz ");
  auto escaped = str.escape();
  BOOST_CHECK_EQUAL(escaped, "foo\\x09bar\\x0dbaz ");
  BOOST_CHECK_EQUAL(str, escaped.unescape());
  BOOST_CHECK_EQUAL(str, str.escape(true).unescape());
  str = "\\x2a";
  BOOST_CHECK(str.is_escape_seq(str.begin()));
  BOOST_CHECK_EQUAL(str.escape(), "\\x5cx2a");
  BOOST_CHECK_EQUAL(str.escape().unescape(), str);
  BOOST_CHECK_EQUAL(str.escape(true).unescape(), str);

  str = "ai caramba";
  BOOST_CHECK_EQUAL(str.substr(0, 2), "ai");
  BOOST_CHECK_EQUAL(str.substr(3, 100), "caramba");
  BOOST_CHECK_EQUAL(str.substr(3, 7), "caramba");
  BOOST_CHECK_EQUAL(str.substr(3), "caramba");
  BOOST_CHECK_EQUAL(str.substr(20, 7), "");

  str = "yo,my,bud";
  auto pos = str.split(",");
  BOOST_REQUIRE_EQUAL(pos.size(), 3);
  string s0(pos[0].first, pos[0].second);
  string s1(pos[1].first, pos[1].second);
  string s2(pos[2].first, pos[2].second);
  BOOST_CHECK_EQUAL(s0, "yo");
  BOOST_CHECK_EQUAL(s1, "my");
  BOOST_CHECK_EQUAL(s2, "bud");

  str = "foo, bar|, baz, qux";
  pos = str.split(", ", "|");
  BOOST_REQUIRE_EQUAL(pos.size(), 3);
  s0 = {pos[0].first, pos[0].second};
  s1 = {pos[1].first, pos[1].second};
  s2 = {pos[2].first, pos[2].second};
  BOOST_CHECK_EQUAL(s0, "foo");
  BOOST_CHECK_EQUAL(s1, "bar|, baz");
  BOOST_CHECK_EQUAL(s2, "qux");

  str = "foo--bar||--baz--qux--corge";
  pos = str.split("--", "||", 3, true);
  BOOST_REQUIRE_EQUAL(pos.size(), 5);
  s0 = {pos[0].first, pos[0].second};
  s1 = {pos[1].first, pos[1].second};
  s2 = {pos[2].first, pos[2].second};
  string s3{pos[3].first, pos[3].second};
  string s4{pos[4].first, pos[4].second};
  BOOST_CHECK_EQUAL(s0, "foo");
  BOOST_CHECK_EQUAL(s1, "--");
  BOOST_CHECK_EQUAL(s2, "bar||--baz");
  BOOST_CHECK_EQUAL(s3, s1);
  BOOST_CHECK_EQUAL(s4, "qux--corge");

  str = "  x  ";
  BOOST_CHECK_EQUAL(str.trim(), "x");
  BOOST_CHECK_EQUAL(str.trim("  "), "x");
  BOOST_CHECK_EQUAL(str.trim("   "), "  x  ");

  str = "the needle in the haystack";
  BOOST_CHECK_EQUAL(str.find("needle"), 4);
  BOOST_CHECK_EQUAL(str.find("the", 3), 14);
  BOOST_CHECK_EQUAL(str.find("t"), 0);
  BOOST_CHECK_EQUAL(str.find("k"), str.size() - 1);
  BOOST_CHECK_EQUAL(str.find("fox"), string::npos);

  BOOST_CHECK_EQUAL(str.rfind("", 4), string::npos);
  BOOST_CHECK_EQUAL(str.rfind("t", 0), string::npos);
  BOOST_CHECK_EQUAL(str.rfind("t"), 22);
  BOOST_CHECK_EQUAL(str.rfind("needle"), 4);
  BOOST_CHECK_EQUAL(str.rfind("the"), 14);
  BOOST_CHECK_EQUAL(str.rfind("the needle"), 0);

  BOOST_CHECK(str.starts_with("the needle"));
  BOOST_CHECK(str.ends_with("the haystack"));
  BOOST_CHECK(! str.ends_with("the yarn"));
  BOOST_CHECK(! str.ends_with("a haystack"));

  str = "XXXaaa--XXXbbb---XXXX";
  BOOST_CHECK_EQUAL(str.thin("XXX"), "aaa--bbb---X");
  str = "/http:\\/\\/www.bro-ids.org/";
  BOOST_CHECK_EQUAL(str.thin("/"), "http:\\\\www.bro-ids.org");
  BOOST_CHECK_EQUAL(str.thin("/", "\\"), "http://www.bro-ids.org");

  str = "123456";
  BOOST_CHECK_EQUAL(to<int>(str), 123456);
  BOOST_CHECK_EQUAL(to<long>(str), 123456);
  BOOST_CHECK_EQUAL(to<unsigned long>(str), 123456);

  str = "0x2a";
  BOOST_CHECK_EQUAL(to<unsigned int>(str), 42);

  str = "0.0042";
  BOOST_CHECK_EQUAL(to<double>(str), 0.0042);
}

BOOST_AUTO_TEST_CASE(regexes)
{
  {
    std::string str = "1";
    BOOST_CHECK(regex("[0-9]").match(str));
    BOOST_CHECK(! regex("[^1]").match(str));

    str = "foobarbaz";
    BOOST_CHECK(regex("bar").search(str));
    BOOST_CHECK(! regex("^bar$").search(str));
    BOOST_CHECK(regex("^\\w{3}\\w{3}\\w{3}$").match(str));

    std::string built;
    regex("\\w+").match(
        str,
        [&built](std::string const& s) { built += s; });

    BOOST_CHECK_EQUAL(str, built);

    BOOST_CHECK(regex::glob("foo*baz").match(str));
    BOOST_CHECK(regex::glob("foo???baz").match(str));
  }

  {
    string str("Holla die Waldfee!");
    regex rx("\\w+ die Waldfe{2}.");
    BOOST_CHECK(rx.match(str));
    BOOST_CHECK(rx.search(str));

    rx = regex("(\\w+ )");
    BOOST_CHECK(! rx.match(str));
    BOOST_CHECK(rx.search(str));

    BOOST_CHECK_EQUAL(to_string(rx), "/(\\w+ )/");
  }
}

BOOST_AUTO_TEST_CASE(tables)
{
  table ports{{"ssh", 22u}, {"http", 80u}, {"https", 443u}, {"imaps", 993u}};
  BOOST_CHECK(ports.size() == 4);

  auto i = ports.find("ssh");
  BOOST_REQUIRE(i != ports.end());
  BOOST_CHECK(i->second == 22u);
  i = ports.find("imaps");
  BOOST_REQUIRE(i != ports.end());
  BOOST_CHECK(i->second == 993u);

  BOOST_CHECK(ports.emplace("telnet", 23u).second);
  BOOST_CHECK(! ports.emplace("http", 8080u).second);
}

BOOST_AUTO_TEST_CASE(records)
{
  record r{"foo", -42, 1001u, 'x', port{443, port::tcp}};
  record s{100, "bar", r};
  BOOST_CHECK_EQUAL(r.size(), 5);

  BOOST_CHECK_EQUAL(*s.at({0}), 100);
  BOOST_CHECK_EQUAL(*s.at({1}), "bar");
  BOOST_CHECK_EQUAL(*s.at({2}), value(r));
  BOOST_CHECK_EQUAL(*s.at({2, 3}), 'x');

  BOOST_CHECK_EQUAL(s.flat_size(), r.size() + 2);
  BOOST_CHECK_EQUAL(*s.flat_at(0), 100);
  BOOST_CHECK_EQUAL(*s.flat_at(1), "bar");
  BOOST_CHECK_EQUAL(*s.flat_at(2), "foo");
  BOOST_CHECK_EQUAL(s.flat_at(6)->which(), port_type);
  BOOST_CHECK(s.flat_at(7) == nullptr);
}

BOOST_AUTO_TEST_CASE(addresses_v4)
{
  address x;
  address y;
  BOOST_CHECK(x == y);
  BOOST_CHECK(! x.is_v4());
  BOOST_CHECK(x.is_v6());

  address a("172.16.7.1");
  BOOST_CHECK(to_string(a) == "172.16.7.1");
  BOOST_CHECK(a.is_v4());
  BOOST_CHECK(! a.is_v6());
  BOOST_CHECK(! a.is_loopback());
  BOOST_CHECK(! a.is_multicast());
  BOOST_CHECK(! a.is_broadcast());

  address localhost("127.0.0.1");
  BOOST_CHECK(to_string(localhost) == "127.0.0.1");
  BOOST_CHECK(localhost.is_v4());
  BOOST_CHECK(localhost.is_loopback());
  BOOST_CHECK(! localhost.is_multicast());
  BOOST_CHECK(! localhost.is_broadcast());

  // Lexicalgraphical comparison.
  BOOST_CHECK(localhost < a);

  // Bitwise operations
  address anded = a & localhost;
  address ored = a | localhost;
  address xored = a ^ localhost;
  BOOST_CHECK(anded == address("44.0.0.1"));
  BOOST_CHECK(ored == address("255.16.7.1"));
  BOOST_CHECK(xored == address("211.16.7.0"));
  BOOST_CHECK(anded.is_v4());
  BOOST_CHECK(ored.is_v4());
  BOOST_CHECK(xored.is_v4());

  auto broadcast = address("255.255.255.255");
  BOOST_CHECK(broadcast.is_broadcast());

  uint32_t n = 3232235691;
  address b(&n, address::ipv4, address::host);
  BOOST_CHECK(to_string(b) == "192.168.0.171");
}

BOOST_AUTO_TEST_CASE(addresses_v6)
{
  BOOST_CHECK(address() == address("::"));

  address a("2001:db8:0000:0000:0202:b3ff:fe1e:8329");
  address b("2001:db8:0:0:202:b3ff:fe1e:8329");
  address c("2001:db8::202:b3ff:fe1e:8329");
  BOOST_CHECK(a.is_v6() && b.is_v6() && c.is_v6());
  BOOST_CHECK(! (a.is_v4() || b.is_v4() || c.is_v4()));
  BOOST_CHECK(a == b && b == c);

  address d("ff01::1");
  BOOST_CHECK(d.is_multicast());

  BOOST_CHECK((a ^ b) == address("::"));
  BOOST_CHECK((a & b) == a);
  BOOST_CHECK((a | b) == a);
  BOOST_CHECK((a & d) == address("2001::1"));
  BOOST_CHECK((a | d) == address("ff01:db8::202:b3ff:fe1e:8329"));
  BOOST_CHECK((a ^ d) == address("df00:db8::202:b3ff:fe1e:8328"));

  uint8_t raw8[16] = { 0xdf, 0x00, 0x0d, 0xb8,
  0x00, 0x00, 0x00, 0x00,
  0x02, 0x02, 0xb3, 0xff,
  0xfe, 0x1e, 0x83, 0x28 };
  auto p = reinterpret_cast<uint32_t const*>(raw8);
  address e(p, address::ipv6, address::network);
  BOOST_CHECK(e == (a ^ d));

  uint32_t raw32[4] = { 0xdf000db8, 0x00000000, 0x0202b3ff, 0xfe1e8328 };
  p = reinterpret_cast<uint32_t const*>(raw32);
  address f(p, address::ipv6, address::host);
  BOOST_CHECK(f == (a ^ d));
  BOOST_CHECK(f == e);

  a.mask(112);
  BOOST_CHECK(a == address("2001:db8::202:b3ff:fe1e:0"));
  a.mask(100);
  BOOST_CHECK(a == address("2001:db8::202:b3ff:f000:0"));
  a.mask(3);
  BOOST_CHECK(a == address("2000::"));
}

BOOST_AUTO_TEST_CASE(prefixes)
{
  prefix p;
  BOOST_CHECK_EQUAL(p.network(), address{"::"});
  BOOST_CHECK_EQUAL(p.length(), 0);
  BOOST_CHECK_EQUAL(to_string(p), "::/0");

  address a{"192.168.0.1"};
  prefix q{a, 24};
  BOOST_CHECK_EQUAL(q.network(), address("192.168.0.0"));
  BOOST_CHECK_EQUAL(q.length(), 24);
  BOOST_CHECK_EQUAL(to_string(q), "192.168.0.0/24");
  BOOST_CHECK(q.contains(address("192.168.0.73")));
  BOOST_CHECK(! q.contains(address("192.168.244.73")));

  address b{"2001:db8:0000:0000:0202:b3ff:fe1e:8329"};
  prefix r{b, 64};
  BOOST_CHECK_EQUAL(r.length(), 64);
  BOOST_CHECK_EQUAL(r.network(), address{"2001:db8::"});
  BOOST_CHECK_EQUAL(to_string(r), "2001:db8::/64");
  BOOST_CHECK(r.contains(address("2001:db8::cafe:babe")));
  BOOST_CHECK(! r.contains(address("ff00::")));
}

BOOST_AUTO_TEST_CASE(ports)
{
  port p;
  BOOST_CHECK(p.number() == 0u);
  BOOST_CHECK(p.type() == port::unknown);

  p = port(22u, port::tcp);
  BOOST_CHECK(p.number() == 22u);
  BOOST_CHECK(p.type() == port::tcp);

  port q(53u, port::udp);
  BOOST_CHECK(q.number() == 53u);
  BOOST_CHECK(q.type() == port::udp);

  BOOST_CHECK(p != q);
  BOOST_CHECK(p < q);
}

BOOST_AUTO_TEST_CASE(paths)
{
  path p(".");
  BOOST_CHECK_EQUAL(p.basename(), ".");
  BOOST_CHECK_EQUAL(p.extension(), ".");
  BOOST_CHECK_EQUAL(p.parent(), "");
  p = "..";
  BOOST_CHECK_EQUAL(p.basename(), "..");
  BOOST_CHECK_EQUAL(p.extension(), ".");
  BOOST_CHECK_EQUAL(p.parent(), "");
  p = "/";
  BOOST_CHECK_EQUAL(p.basename(), "/");
  BOOST_CHECK_EQUAL(p.extension(), "");
  BOOST_CHECK_EQUAL(p.parent(), "");
  p = "foo";
  BOOST_CHECK_EQUAL(p.basename(), "foo");
  BOOST_CHECK_EQUAL(p.extension(), "");
  BOOST_CHECK_EQUAL(p.parent(), "");
  p = "/foo";
  BOOST_CHECK_EQUAL(p.basename(), "foo");
  BOOST_CHECK_EQUAL(p.extension(), "");
  BOOST_CHECK_EQUAL(p.parent(), "/");
  p = "foo/";
  BOOST_CHECK_EQUAL(p.basename(), ".");
  BOOST_CHECK_EQUAL(p.extension(), "");
  BOOST_CHECK_EQUAL(p.parent(), "foo");
  p = "/foo/";
  BOOST_CHECK_EQUAL(p.basename(), ".");
  BOOST_CHECK_EQUAL(p.extension(), "");
  BOOST_CHECK_EQUAL(p.parent(), "/foo");
  p = "foo/bar";
  BOOST_CHECK_EQUAL(p.basename(), "bar");
  BOOST_CHECK_EQUAL(p.extension(), "");
  BOOST_CHECK_EQUAL(p.parent(), "foo");
  p = "/foo/bar";
  BOOST_CHECK_EQUAL(p.basename(), "bar");
  BOOST_CHECK_EQUAL(p.extension(), "");
  BOOST_CHECK_EQUAL(p.parent(), "/foo");
  p = "/.";
  BOOST_CHECK_EQUAL(p.basename(), ".");
  BOOST_CHECK_EQUAL(p.extension(), ".");
  BOOST_CHECK_EQUAL(p.parent(), "/");
  p = "./";
  BOOST_CHECK_EQUAL(p.basename(), ".");
  BOOST_CHECK_EQUAL(p.extension(), "");
  BOOST_CHECK_EQUAL(p.parent(), ".");
  p = "/..";
  BOOST_CHECK_EQUAL(p.basename(), "..");
  BOOST_CHECK_EQUAL(p.extension(), ".");
  BOOST_CHECK_EQUAL(p.parent(), "/");
  p = "../";
  BOOST_CHECK_EQUAL(p.basename(), ".");
  BOOST_CHECK_EQUAL(p.extension(), "");
  BOOST_CHECK_EQUAL(p.parent(), "..");
  p = "foo/.";
  BOOST_CHECK_EQUAL(p.basename(), ".");
  BOOST_CHECK_EQUAL(p.extension(), ".");
  BOOST_CHECK_EQUAL(p.parent(), "foo");
  p = "foo/..";
  BOOST_CHECK_EQUAL(p.basename(), "..");
  BOOST_CHECK_EQUAL(p.extension(), ".");
  BOOST_CHECK_EQUAL(p.parent(), "foo");
  p = "foo/./";
  BOOST_CHECK_EQUAL(p.basename(), ".");
  BOOST_CHECK_EQUAL(p.extension(), "");
  BOOST_CHECK_EQUAL(p.parent(), "foo/.");
  p = "foo/../";
  BOOST_CHECK_EQUAL(p.basename(), ".");
  BOOST_CHECK_EQUAL(p.extension(), "");
  BOOST_CHECK_EQUAL(p.parent(), "foo/..");
  p = "foo/./bar";
  BOOST_CHECK_EQUAL(p.basename(), "bar");
  BOOST_CHECK_EQUAL(p.extension(), "");
  BOOST_CHECK_EQUAL(p.parent(), "foo/.");
}

BOOST_AUTO_TEST_SUITE_END()

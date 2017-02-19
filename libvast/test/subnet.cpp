#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/subnet.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/subnet.hpp"
#include "vast/subnet.hpp"

#define SUITE subnet
#include "test.hpp"

using namespace vast;
using namespace std::string_literals;

TEST(subnets) {
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
  CHECK(!q.contains(*to<address>("192.168.244.73")));

  auto b = *to<address>("2001:db8:0000:0000:0202:b3ff:fe1e:8329");
  subnet r{b, 64};
  CHECK(r.length() == 64);
  CHECK(r.network() == *to<address>("2001:db8::"));
  CHECK(to_string(r) == "2001:db8::/64");
  CHECK(r.contains(*to<address>("2001:db8::cafe:babe")));
  CHECK(!r.contains(*to<address>("ff00::")));
}

TEST(printable) {
  auto sn = subnet{*to<address>("10.0.0.0"), 8};
  CHECK_EQUAL(to_string(sn), "10.0.0.0/8");
}

TEST(subnet) {
  auto p = make_parser<subnet>{};
  MESSAGE("IPv4");
  auto str = "192.168.0.0/24"s;
  auto f = str.begin();
  auto l = str.end();
  subnet s;
  CHECK(p(f, l, s));
  CHECK(f == l);
  CHECK(s == subnet{*to<address>("192.168.0.0"), 24});
  CHECK(s.network().is_v4());
  MESSAGE("IPv6");
  str = "beef::cafe/40";
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, s));
  CHECK(f == l);
  CHECK(s == subnet{*to<address>("beef::cafe"), 40});
  CHECK(s.network().is_v6());
}


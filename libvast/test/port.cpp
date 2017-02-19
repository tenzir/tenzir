#include "vast/concept/parseable/from_string.hpp"
#include "vast/concept/parseable/vast/port.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/port.hpp"
#include "vast/subnet.hpp"

#define SUITE port
#include "test.hpp"

using namespace vast;
using namespace std::string_literals;

TEST(ports) {
  port p;
  CHECK(p.number() == 0u);
  CHECK(p.type() == port::unknown);
  MESSAGE("tcp");
  p = port(22u, port::tcp);
  CHECK(p.number() == 22u);
  CHECK(p.type() == port::tcp);
  MESSAGE("udp");
  port q(53u, port::udp);
  CHECK(q.number() == 53u);
  CHECK(q.type() == port::udp);
  MESSAGE("operators");
  CHECK(p != q);
  CHECK(p < q);
}

TEST(printable) {
  auto p = port{53, port::udp};
  CHECK_EQUAL(to_string(p), "53/udp");
}

TEST(parseable) {
  auto p = make_parser<port>();
  MESSAGE("tcp");
  auto str = "22/tcp"s;
  auto f = str.begin();
  auto l = str.end();
  port prt;
  CHECK(p(f, l, prt));
  CHECK(f == l);
  CHECK(prt == port{22, port::tcp});
  MESSAGE("udp");
  str = "53/udp"s;
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, prt));
  CHECK(f == l);
  CHECK(prt == port{53, port::udp});
  MESSAGE("icmp");
  str = "7/icmp"s;
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, prt));
  CHECK(f == l);
  CHECK(prt == port{7, port::icmp});
  MESSAGE("unknown");
  str = "42/?"s;
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, prt));
  CHECK(f == l);
  CHECK(prt == port{42, port::unknown});
}

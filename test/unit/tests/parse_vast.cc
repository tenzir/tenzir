#include "vast/concept/parseable/to.h"
#include "vast/concept/parseable/vast/address.h"
#include "vast/concept/parseable/vast/key.h"
#include "vast/concept/parseable/vast/offset.h"
#include "vast/concept/parseable/vast/pattern.h"
#include "vast/concept/parseable/vast/port.h"
#include "vast/concept/parseable/vast/subnet.h"
#include "vast/concept/parseable/vast/time.h"
#include "vast/concept/printable/to_string.h"
#include "vast/concept/printable/vast/pattern.h"
#include "vast/concept/printable/vast/address.h"
#include "vast/concept/parseable/vast/http.h"
#include "vast/concept/parseable/vast/uri.h"


#define SUITE parseable
#include "test.h"

using namespace vast;
using namespace std::string_literals;

TEST(time::duration) {
  time::duration d;

  MESSAGE("nanoseconds");
  CHECK(parsers::time_duration("42 nsecs", d));
  CHECK(d == time::nanoseconds(42));
  CHECK(parsers::time_duration("43nsecs", d));
  CHECK(d == time::nanoseconds(43));
  CHECK(parsers::time_duration("44ns", d));
  CHECK(d == time::nanoseconds(44));

  MESSAGE("microseconds");
  CHECK(parsers::time_duration("42 usecs", d));
  CHECK(d == time::microseconds(42));
  CHECK(parsers::time_duration("43usecs", d));
  CHECK(d == time::microseconds(43));
  CHECK(parsers::time_duration("44us", d));
  CHECK(d == time::microseconds(44));

  MESSAGE("milliseconds");
  CHECK(parsers::time_duration("42 msecs", d));
  CHECK(d == time::milliseconds(42));
  CHECK(parsers::time_duration("43msecs", d));
  CHECK(d == time::milliseconds(43));
  CHECK(parsers::time_duration("44ms", d));
  CHECK(d == time::milliseconds(44));

  MESSAGE("seconds");
  CHECK(parsers::time_duration("-42 secs", d));
  CHECK(d == time::seconds(-42));
  CHECK(parsers::time_duration("-43secs", d));
  CHECK(d == time::seconds(-43));
  CHECK(parsers::time_duration("-44s", d));
  CHECK(d == time::seconds(-44));

  MESSAGE("minutes");
  CHECK(parsers::time_duration("-42 mins", d));
  CHECK(d == time::minutes(-42));
  CHECK(parsers::time_duration("-43min", d));
  CHECK(d == time::minutes(-43));
  CHECK(parsers::time_duration("44m", d));
  CHECK(d == time::minutes(44));

  MESSAGE("hours");
  CHECK(parsers::time_duration("42 hours", d));
  CHECK(d == time::hours(42));
  CHECK(parsers::time_duration("-43hrs", d));
  CHECK(d == time::hours(-43));
  CHECK(parsers::time_duration("44h", d));
  CHECK(d == time::hours(44));

  // TODO
  // MESSAGE("compound");
  // CHECK(parsers::time_duration("5m99s", d));
  // CHECK(d.count() == 399000000000ll);
}

TEST(time::point) {
  MESSAGE("YYY-MM-DD+HH:MM:SS");
  auto p = make_parser<time::point>{};
  auto str = "2012-08-12+23:55:04"s;
  auto f = str.begin();
  auto l = str.end();
  time::point tp;
  CHECK(p.parse(f, l, tp));
  CHECK(f == l);
  CHECK(tp == time::point::utc(2012, 8, 12, 23, 55, 4));

  // TODO
  // MESSAGE("UNIX epoch");
  // CHECK(p("@1398933902", tp));
  // CHECK(p == time::seconds{1398933902});
  // CHECK(p("@1398933902.686337", tp));
  // CHECK(p == time::double_seconds{1398933902.686337});
}

TEST(pattern) {
  auto p = make_parser<pattern>{};
  auto str = "/^\\w{3}\\w{3}\\w{3}$/"s;
  auto f = str.begin();
  auto l = str.end();
  pattern pat;
  CHECK(p.parse(f, l, pat));
  CHECK(f == l);
  CHECK(to_string(pat) == str);

  str = "/foo\\+(bar){2}|\"baz\"*/";
  pat = {};
  f = str.begin();
  l = str.end();
  CHECK(p.parse(f, l, pat));
  CHECK(f == l);
  CHECK(to_string(pat) == str);
}

TEST(address) {
  auto p = make_parser<address>{};

  MESSAGE("IPv4");
  auto str = "192.168.0.1"s;
  auto f = str.begin();
  auto l = str.end();
  address a;
  CHECK(p.parse(f, l, a));
  CHECK(f == l);
  CHECK(a.is_v4());
  CHECK(to_string(a) == str);

  MESSAGE("IPv6");
  str = "::";
  f = str.begin();
  l = str.end();
  CHECK(p.parse(f, l, a));
  CHECK(f == l);
  CHECK(a.is_v6());
  CHECK(to_string(a) == str);
  str = "beef::cafe";
  f = str.begin();
  l = str.end();
  CHECK(p.parse(f, l, a));
  CHECK(f == l);
  CHECK(a.is_v6());
  CHECK(to_string(a) == str);
  str = "f00::cafe";
  f = str.begin();
  l = str.end();
  CHECK(p.parse(f, l, a));
  CHECK(f == l);
  CHECK(a.is_v6());
  CHECK(to_string(a) == str);
}

TEST(subnet) {
  auto p = make_parser<subnet>{};

  MESSAGE("IPv4");
  auto str = "192.168.0.0/24"s;
  auto f = str.begin();
  auto l = str.end();
  subnet s;
  CHECK(p.parse(f, l, s));
  CHECK(f == l);
  CHECK(s == subnet{*to<address>("192.168.0.0"), 24});
  CHECK(s.network().is_v4());

  MESSAGE("IPv6");
  str = "beef::cafe/40";
  f = str.begin();
  l = str.end();
  CHECK(p.parse(f, l, s));
  CHECK(f == l);
  CHECK(s == subnet{*to<address>("beef::cafe"), 40});
  CHECK(s.network().is_v6());
}

TEST(port) {
  auto p = make_parser<port>();

  MESSAGE("tcp");
  auto str = "22/tcp"s;
  auto f = str.begin();
  auto l = str.end();
  port prt;
  CHECK(p.parse(f, l, prt));
  CHECK(f == l);
  CHECK(prt == port{22, port::tcp});

  MESSAGE("udp");
  str = "53/udp"s;
  f = str.begin();
  l = str.end();
  CHECK(p.parse(f, l, prt));
  CHECK(f == l);
  CHECK(prt == port{53, port::udp});

  MESSAGE("icmp");
  str = "7/icmp"s;
  f = str.begin();
  l = str.end();
  CHECK(p.parse(f, l, prt));
  CHECK(f == l);
  CHECK(prt == port{7, port::icmp});

  MESSAGE("unknown");
  str = "42/?"s;
  f = str.begin();
  l = str.end();
  CHECK(p.parse(f, l, prt));
  CHECK(f == l);
  CHECK(prt == port{42, port::unknown});
}

TEST(key) {
  key k;
  CHECK(parsers::key("foo.bar_baz.qux", k));
  CHECK(k == key{"foo", "bar_baz", "qux"});
}

TEST(offset) {
  offset o;
  CHECK(parsers::offset("1,2,3", o));
  CHECK(o == offset{1, 2, 3});
}

TEST(http_header_parser)
{
  auto p = make_parser<http::header>();
  auto str = "foo: bar"s;
  auto f = str.begin();
  auto l = str.end();
  http::header hdr;
  CHECK(p.parse(f, l, hdr));
  CHECK(hdr.name == "FOO");
  CHECK(hdr.value == "bar");
  CHECK(f == l);

  str = "Content-Type:application/pdf";
  f = str.begin();
  l = str.end();
  CHECK(p.parse(f, l, hdr));
  CHECK(hdr.name == "CONTENT-TYPE");
  CHECK(hdr.value == "application/pdf");
  CHECK(f == l);
}

TEST(http_request_parser)
{
  auto p = make_parser<http::request>();
  auto str = "GET /foo/bar%20baz/ HTTP/1.1\r\nContent-Type:text/html\r\nContent-Length:1234\r\n\r\nBody "s;
  auto f = str.begin();
  auto l = str.end();
  http::request req;
  CHECK(p.parse(f, l, req));
  CHECK(req.method == "GET");
  CHECK(req.uri.path[0] == "foo");
  CHECK(req.uri.path[1] == "bar baz");
  CHECK(req.protocol == "HTTP");
  CHECK(req.version == 1.1);
  auto hdr = req.header("content-type");
  REQUIRE(hdr);
  CHECK(hdr->name == "CONTENT-TYPE");
  CHECK(hdr->value == "text/html");
  hdr = req.header("content-length");
  REQUIRE(hdr);
  CHECK(hdr->name == "CONTENT-LENGTH");
  CHECK(hdr->value == "1234");
  CHECK(f == l);
}

TEST(URI_parser_URL)
{
  auto p = make_parser<uri>();
  auto str = "http://foo.bar:80/foo/bar?opt1=val1&opt2=val2#frag1"s;
  auto f = str.begin();
  auto l = str.end();
  uri u;
  CHECK(p.parse(f, l, u));
  CHECK(u.protocol == "http");
  CHECK(u.hostname == "foo.bar");
  CHECK(u.port == 80);
  CHECK(u.path[0] == "foo");
  CHECK(u.path[1] == "bar");
  CHECK(u.options["opt1"] == "val1");
  CHECK(u.options["opt2"] == "val2");
  CHECK(u.fragment == "frag1");
  CHECK(f == l);
}

TEST(URI_parser_http)
{
  auto p = make_parser<uri>();
  auto str = "/foo/bar?opt1=val1&opt2=val2"s;
  auto f = str.begin();
  auto l = str.end();
  uri u;
  CHECK(p.parse(f, l, u));
  CHECK(u.protocol == "");
  CHECK(u.hostname == "");
  CHECK(u.port == 0);
  CHECK(u.path[0] == "foo");
  CHECK(u.path[1] == "bar");
  CHECK(u.options["opt1"] == "val1");
  CHECK(u.options["opt2"] == "val2");
  CHECK(u.fragment == "");
  CHECK(f == l);
}

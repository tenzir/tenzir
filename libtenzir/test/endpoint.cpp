//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/concept/parseable/tenzir/endpoint.hpp"

#include "tenzir/endpoint.hpp"
#include "tenzir/test/test.hpp"

using namespace tenzir;
using namespace std::string_literals;

namespace {

struct fixture {
  endpoint x;
};

} // namespace

WITH_FIXTURE(fixture) {
  TEST("parseable - host only") {
    CHECK(parsers::endpoint("localhost", x));
    CHECK_EQUAL(x.host, "localhost");
    CHECK(not x.port);
    CHECK(parsers::endpoint("foo-bar_baz.test", x));
    CHECK_EQUAL(x.host, "foo-bar_baz.test");
    CHECK(not x.port);
  }

  TEST("parseable - port only") {
    x.host = "foo";
    CHECK(parsers::endpoint(":5158", x));
    CHECK_EQUAL(x.host, "");
    CHECK_EQUAL(*x.port, 5158);
    CHECK(parsers::endpoint(":12345/tcp", x));
    CHECK_EQUAL(x.host, "");
    CHECK_EQUAL(*x.port, (tenzir::port{12345, port_type::tcp}));
  }

  TEST("parseable - host and port") {
    CHECK(parsers::endpoint("10.0.0.1:80", x));
    CHECK_EQUAL(x.host, "10.0.0.1");
    CHECK_EQUAL(*x.port, 80);
    CHECK_EQUAL(x.port->type(), port_type::unknown);
    CHECK(parsers::endpoint("10.0.0.1:9995/udp", x));
    CHECK_EQUAL(x.host, "10.0.0.1");
    CHECK_EQUAL(x.port->number(), 9995);
    CHECK_EQUAL(x.port->type(), port_type::udp);
  }

  TEST("parseable - IPv6") {
    CHECK(parsers::endpoint("::1", x));
    CHECK_EQUAL(x.host, "::1");
    CHECK(not x.port);
    CHECK(parsers::endpoint("[::1]", x));
    CHECK_EQUAL(x.host, "::1");
    CHECK(not x.port);
    CHECK(parsers::endpoint("[::1]:443", x));
    CHECK_EQUAL(x.host, "::1");
    CHECK_EQUAL(*x.port, 443);
    CHECK(parsers::endpoint("::1:443", x));
    CHECK_EQUAL(x.host, "::1:443");
    CHECK(not x.port);
    CHECK(parsers::endpoint("[2001:db8::1]:443/tcp", x));
    CHECK_EQUAL(x.host, "2001:db8::1");
    CHECK_EQUAL(*x.port, (tenzir::port{443, port_type::tcp}));
  }

  TEST("parseable - malformed endpoints") {
    CHECK(not parsers::endpoint("[::1"));
    CHECK(not parsers::endpoint("[::1]443"));
    CHECK(not parsers::endpoint("[localhost]:443"));
    CHECK(not parsers::endpoint("localhost:"));
    CHECK(not parsers::endpoint("localhost:http"));
  }

  TEST("printable - IPv6 with port") {
    x.host = "::1";
    x.port = 443;
    CHECK_EQUAL(fmt::to_string(x), "[::1]:443");
  }
}

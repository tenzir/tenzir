//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
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
    CHECK_EQUAL(x.port, std::nullopt);
    MESSAGE("keep defaults");
    x.port = 42;
    CHECK(parsers::endpoint("foo-bar_baz.test", x));
    CHECK_EQUAL(x.host, "foo-bar_baz.test");
    CHECK_EQUAL(*x.port, 42);
  }

  TEST("parseable - port only") {
    x.host = "foo";
    CHECK(parsers::endpoint(":5158", x));
    CHECK_EQUAL(x.host, "foo");
    CHECK_EQUAL(*x.port, 5158);
    CHECK(parsers::endpoint(":12345/tcp", x));
    CHECK_EQUAL(x.host, "foo");
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
}

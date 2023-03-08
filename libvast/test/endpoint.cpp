//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/concept/parseable/vast/endpoint.hpp"

#include "vast/endpoint.hpp"
#include "vast/test/test.hpp"

using namespace vast;
using namespace std::string_literals;

namespace {

struct fixture {
  endpoint x;
};

} // namespace

FIXTURE_SCOPE(endpoint_tests, fixture)

TEST(parseable - host only) {
  CHECK(parsers::endpoint("localhost", x));
  CHECK_EQUAL(x.host, "localhost");
  CHECK_EQUAL(x.port, std::nullopt);
  MESSAGE("keep defaults");
  x.port = 42;
  CHECK(parsers::endpoint("foo-bar_baz.test", x));
  CHECK_EQUAL(x.host, "foo-bar_baz.test");
  CHECK_EQUAL(*x.port, 42);
}

TEST(parseable - port only) {
  x.host = "foo";
  CHECK(parsers::endpoint(":42000", x));
  CHECK_EQUAL(x.host, "foo");
  CHECK_EQUAL(*x.port, 42000);
  CHECK(parsers::endpoint(":12345/tcp", x));
  CHECK_EQUAL(x.host, "foo");
  CHECK_EQUAL(*x.port, (vast::port{12345, port_type::tcp}));
}

TEST(parseable - host and port) {
  CHECK(parsers::endpoint("10.0.0.1:80", x));
  CHECK_EQUAL(x.host, "10.0.0.1");
  CHECK_EQUAL(*x.port, 80);
  CHECK_EQUAL(x.port->type(), port_type::unknown);
  CHECK(parsers::endpoint("10.0.0.1:9995/udp", x));
  CHECK_EQUAL(x.host, "10.0.0.1");
  CHECK_EQUAL(x.port->number(), 9995);
  CHECK_EQUAL(x.port->type(), port_type::udp);
}

FIXTURE_SCOPE_END()

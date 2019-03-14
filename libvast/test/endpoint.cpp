/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#define SUITE endpoint

#include "vast/test/test.hpp"

#include "vast/concept/parseable/vast/endpoint.hpp"
#include "vast/endpoint.hpp"

using namespace vast;
using namespace std::string_literals;

namespace {

struct fixture {
  endpoint x;
};

} // namespace <anonymous>

FIXTURE_SCOPE(endpoint_tests, fixture)

TEST(parseable - host only) {
  CHECK(parsers::endpoint("localhost", x));
  CHECK_EQUAL(x.host, "localhost");
  CHECK_EQUAL(x.port, 0);
  MESSAGE("keep defaults");
  x.port = 42;
  CHECK(parsers::endpoint("foo-bar_baz.test", x));
  CHECK_EQUAL(x.host, "foo-bar_baz.test");
  CHECK_EQUAL(x.port, 42);
}

TEST(parseable - port only) {
  x.host = "foo";
  CHECK(parsers::endpoint(":42000", x));
  CHECK_EQUAL(x.host, "foo");
  CHECK_EQUAL(x.port, 42000);
  CHECK(parsers::endpoint(":12345/tcp", x));
  CHECK_EQUAL(x.host, "foo");
  CHECK_EQUAL(x.port, (vast::port{12345, port::tcp}));
}

TEST(parseable - host and port) {
  CHECK(parsers::endpoint("10.0.0.1:80", x));
  CHECK_EQUAL(x.host, "10.0.0.1");
  CHECK_EQUAL(x.port, 80);
  CHECK_EQUAL(x.port.type(), port::unknown);
  CHECK(parsers::endpoint("10.0.0.1:9995/udp", x));
  CHECK_EQUAL(x.host, "10.0.0.1");
  CHECK_EQUAL(x.port.number(), 9995);
  CHECK_EQUAL(x.port.type(), port::udp);
}

FIXTURE_SCOPE_END()

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

#include "vast/concept/parseable/vast/endpoint.hpp"
#include "vast/endpoint.hpp"

#define SUITE endpoint
#include "vast/test/test.hpp"

using namespace vast;
using namespace std::string_literals;

TEST(parseable) {
  endpoint e;
  CHECK(parsers::endpoint(":42000", e));
  CHECK(e.host == "");
  CHECK(e.port == 42000);
  CHECK(parsers::endpoint("localhost", e));
  CHECK(e.host == "localhost");
  CHECK(e.port == 0);
  CHECK(parsers::endpoint("10.0.0.1:80", e));
  CHECK(e.host == "10.0.0.1");
  CHECK(e.port == 80);
  CHECK(parsers::endpoint("foo-bar_baz.test", e));
  CHECK(e.host == "foo-bar_baz.test");
  CHECK(e.port == 0);
}


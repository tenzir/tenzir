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

// NOTE: This file contains an example for using the CAF testing framework, and
// does not contain any meaningful tests for the example plugin. It merely
// exists to show how to setup unit tests.

#define CAF_SUITE example
#include <caf/test/unit_test.hpp>

CAF_TEST(multiply) {
  CAF_REQUIRE(0 * 1 == 0);
  CAF_CHECK(42 + 42 == 84);
}

struct fixture {
  fixture() {
    CAF_MESSAGE("entering test");
  }

  ~fixture() {
    CAF_MESSAGE("leaving test");
  }
};

CAF_TEST_FIXTURE_SCOPE(tracing_scope, fixture)

CAF_TEST(divide) {
  CAF_CHECK_EQUAL(0 / 1, 0);
  CAF_CHECK_NOT_EQUAL(1 / 1, 0);
}

CAF_TEST_FIXTURE_SCOPE_END()

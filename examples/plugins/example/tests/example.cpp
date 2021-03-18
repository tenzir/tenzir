// SPDX-FileCopyrightText: (c) 2021 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

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

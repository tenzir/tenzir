//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

// NOTE: This file contains an example for using the CAF testing framework, and
// does not contain any meaningful tests for the example plugin. It merely
// exists to show how to setup unit tests.

#include <vast/test/test.hpp>

TEST(multiply) {
  CAF_REQUIRE(0 * 1 == 0);
  CAF_CHECK(42 + 42 == 84);
}

namespace {

struct fixture {
  fixture() {
    MESSAGE("entering test");
  }

  ~fixture() {
    MESSAGE("leaving test");
  }
};

} // namespace

FIXTURE_SCOPE(tracing_scope, fixture)

TEST(divide) {
  CAF_CHECK_EQUAL(0 / 1, 0);
  CAF_CHECK_NOT_EQUAL(1 / 1, 0);
}

FIXTURE_SCOPE_END()

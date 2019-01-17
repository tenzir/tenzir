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

#pragma once

#ifdef SUITE
#define CAF_SUITE SUITE
#endif

#include <set>
#include <string>

#include <caf/test/unit_test.hpp>
#include <caf/test/dsl.hpp>

// -- logging macros -----------------------------------------------------------

#define ERROR CAF_TEST_PRINT_ERROR
#define INFO CAF_TEST_PRINT_INFO
#define VERBOSE CAF_TEST_PRINT_VERBOSE
#define MESSAGE CAF_MESSAGE

// -- test setup macros --------------------------------------------------------

#define TEST CAF_TEST
#define TEST_DISABLED CAF_TEST_DISABLED
#define FIXTURE_SCOPE CAF_TEST_FIXTURE_SCOPE
#define FIXTURE_SCOPE_END CAF_TEST_FIXTURE_SCOPE_END

// -- macros for checking results ----------------------------------------------

#define REQUIRE CAF_REQUIRE
#define REQUIRE_EQUAL CAF_REQUIRE_EQUAL
#define REQUIRE_NOT_EQUAL CAF_REQUIRE_NOT_EQUAL
#define REQUIRE_LESS CAF_REQUIRE_LESS
#define REQUIRE_LESS_EQUAL CAF_REQUIRE_LESS_EQUAL
#define REQUIRE_GREATER CAF_REQUIRE_GREATER
#define REQUIRE_GREATER_EQUAL CAF_REQUIRE_GREATER_EQUAL
#define CHECK CAF_CHECK
#define CHECK_EQUAL CAF_CHECK_EQUAL
#define CHECK_NOT_EQUAL CAF_CHECK_NOT_EQUAL
#define CHECK_LESS CAF_CHECK_LESS
#define CHECK_LESS_EQUAL CAF_CHECK_LESS_OR_EQUAL
#define CHECK_GREATER CAF_CHECK_GREATER
#define CHECK_GREATER_EQUAL CAF_CHECK_GREATER_OR_EQUAL
#define CHECK_FAIL CAF_CHECK_FAIL
#define FAIL CAF_FAIL

// -- convenience macros for common check categories ---------------------------

// Checks whether a value initialized from `expr` compares equal to itself
// after a cycle of serializing and deserializing it. Requires the
// `deterministic_actor_system` fixture.
#define CHECK_ROUNDTRIP(expr)                                                  \
  {                                                                            \
    auto x = expr;                                                             \
    CHECK_EQUAL(roundtrip(x), x);                                              \
  }

// Like `CHECK_ROUNDTRIP`, but compares the objects by dereferencing them via
// `operator*` first.
#define CHECK_ROUNDTRIP_DEREF(expr)                                            \
  {                                                                            \
    auto x = expr;                                                             \
    auto y = roundtrip(x);                                                     \
    REQUIRE_NOT_EQUAL(x, nullptr);                                             \
    REQUIRE_NOT_EQUAL(y, nullptr);                                             \
    CHECK_EQUAL(*y, *x);                                                       \
  }

// -- global state -------------------------------------------------------------

namespace vast::test {

// Holds global configuration options passed on the command line after the
// special -- delimiter.
extern std::set<std::string> config;

} // namespace vast::test


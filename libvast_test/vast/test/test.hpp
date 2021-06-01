//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#ifdef SUITE
#  define CAF_SUITE SUITE
#endif

#include <caf/test/unit_test.hpp>

#include <set>
#include <string>

// Work around missing namespace qualification in CAF header.
using ::caf::term;

namespace vast::test::detail {

struct equality_compare {
  template <class T1, class T2>
  bool operator()(const T1& t1, const T2& t2) {
    return t1 == t2;
  }
};

struct inequality_compare {
  template <class T1, class T2>
  bool operator()(const T1& t1, const T2& t2) {
    return t1 != t2;
  }
};

struct greater_compare {
  template <class T1, class T2>
  bool operator()(const T1& t1, const T2& t2) {
    return t1 > t2;
  }
};

struct greater_equal_compare {
  template <class T1, class T2>
  bool operator()(const T1& t1, const T2& t2) {
    return t1 >= t2;
  }
};

struct less_compare {
  template <class T1, class T2>
  bool operator()(const T1& t1, const T2& t2) {
    return t1 < t2;
  }
};

struct less_equal_compare {
  template <class T1, class T2>
  bool operator()(const T1& t1, const T2& t2) {
    return t1 <= t2;
  }
};

} // end namespace vast::test::detail

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

// Checks that abort the current test on failure
#define REQUIRE CAF_REQUIRE
#define REQUIRE_EQUAL(x, y)                                                    \
  CAF_REQUIRE_FUNC(vast::test::detail::equality_compare, (x), (y))
#define REQUIRE_NOT_EQUAL(x, y)                                                \
  CAF_REQUIRE_FUNC(vast::test::detail::inequality_compare, (x), (y))
#define REQUIRE_LESS(x, y)                                                     \
  CAF_REQUIRE_FUNC(vast::test::detail::less_compare, (x), (y))
#define REQUIRE_LESS_EQUAL(x, y)                                               \
  CAF_REQUIRE_FUNC(vast::test::detail::less_equal_compare, (x), (y))
#define REQUIRE_GREATER(x, y)                                                  \
  CAF_REQUIRE_FUNC(vast::test::detail::greater_compare, (x), (y))
#define REQUIRE_GREATER_EQUAL(x, y)                                            \
  CAF_REQUIRE_FUNC(vast::test::detail::greater_equal_compare, (x), (y))
#define REQUIRE_NOERROR(x)                                                     \
  do {                                                                         \
    if (!(x)) {                                                                \
      FAIL((x).error());                                                       \
    } else {                                                                   \
      CAF_CHECK_PASSED(#x);                                                    \
    }                                                                          \
  } while (false)
#define FAIL CAF_FAIL
// Checks that continue with the current test on failure
#define CHECK CAF_CHECK
#define CHECK_EQUAL(x, y)                                                      \
  CAF_CHECK_FUNC(vast::test::detail::equality_compare, (x), (y))
#define CHECK_NOT_EQUAL(x, y)                                                  \
  CAF_CHECK_FUNC(vast::test::detail::inequality_compare, (x), (y))
#define CHECK_LESS(x, y)                                                       \
  CAF_CHECK_FUNC(vast::test::detail::less_compare, (x), (y))
#define CHECK_LESS_EQUAL(x, y)                                                 \
  CAF_CHECK_FUNC(vast::test::detail::less_equal_compare, (x), (y))
#define CHECK_GREATER(x, y)                                                    \
  CAF_CHECK_FUNC(vast::test::detail::greater_compare, (x), (y))
#define CHECK_GREATER_EQUAL(x, y)                                              \
  CAF_CHECK_FUNC(vast::test::detail::greater_equal_compare, (x), (y))
#define CHECK_NOERROR(x)                                                       \
  do {                                                                         \
    if (!(x)) {                                                                \
      CAF_CHECK_FAILED((x).error());                                           \
    } else {                                                                   \
      CAF_CHECK_PASSED(#x)                                                     \
    }                                                                          \
  } while (false)
#define CHECK_FAIL CAF_CHECK_FAIL
// Checks that automagically handle caf::variant types.
#define CHECK_VARIANT_EQUAL CAF_CHECK_EQUAL
#define CHECK_VARIANT_NOT_EQUAL CAF_CHECK_NOT_EQUAL
#define CHECK_VARIANT_LESS CAF_CHECK_LESS
#define CHECK_VARIANT_LESS_EQUAL CAF_CHECK_LESS_OR_EQUAL
#define CHECK_VARIANT_GREATER CAF_CHECK_GREATER
#define CHECK_VARIANT_GREATER_EQUAL CAF_CHECK_GREATER_OR_EQUAL

// -- convenience macros for common check categories ---------------------------

// Checks whether a value initialized from `expr` compares equal to itself
// after a cycle of serializing and deserializing it. Requires the
// `deterministic_actor_system` fixture.
#define CHECK_ROUNDTRIP(expr)                                                  \
  {                                                                            \
    auto __x = expr;                                                           \
    CHECK_EQUAL(roundtrip(__x), __x);                                          \
  }

// Like `CHECK_ROUNDTRIP`, but compares the objects by dereferencing them via
// `operator*` first.
#define CHECK_ROUNDTRIP_DEREF(expr)                                            \
  {                                                                            \
    auto __x = expr;                                                           \
    auto __y = roundtrip(__x);                                                 \
    REQUIRE_NOT_EQUAL(__x, nullptr);                                           \
    REQUIRE_NOT_EQUAL(__y, nullptr);                                           \
    CHECK_EQUAL(*__y, *__x);                                                   \
  }

// -- global state -------------------------------------------------------------

namespace vast::test {

template <class T>
T unbox(std::optional<T>&& x) {
  if (!x)
    CAF_FAIL("x == none");
  return std::move(*x);
}

template <class T>
T unbox(std::optional<T>& x) {
  if (!x)
    CAF_FAIL("x == none");
  return std::move(*x);
}

template <class T>
T unbox(const std::optional<T>& x) {
  if (!x)
    CAF_FAIL("x == none");
  return *x;
}

// Holds global configuration options passed on the command line after the
// special -- delimiter.
extern std::set<std::string> config;

} // namespace vast::test

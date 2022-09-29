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

#include "vast/span.hpp"
#include "vast/test/type_ids.hpp"

#include <caf/allowed_unsafe_message_type.hpp>
#include <caf/detail/stringification_inspector.hpp>
#include <caf/inspector_access.hpp>
#include <caf/test/unit_test.hpp>
#include <fmt/format.h>

#include <optional>
#include <set>
#include <string>
#include <string_view>

// Work around missing namespace qualification in CAF header.
using ::caf::term;

namespace caf {

template <>
struct inspector_access<std::string_view> {
  static auto apply(detail::stringification_inspector& f, std::string_view& x) {
    auto str = std::string{x};
    return f.apply(str);
  }

  static bool
  save_field(detail::stringification_inspector&, string_view, auto&, auto&) {
    return true;
  }
};

} // namespace caf

namespace vast::test::detail {

struct equality_compare {
  template <class T1, class T2>
  bool operator()(const T1& t1, const T2& t2) {
    using vast::operator==;
    return t1 == t2;
  }
};

struct inequality_compare {
  template <class T1, class T2>
  bool operator()(const T1& t1, const T2& t2) {
    using vast::operator!=;
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
// fmt::format("{}", (x).error()
// Checks that abort the current test on failure
#define REQUIRE CAF_REQUIRE
#define REQUIRE_EQUAL(x, y) CAF_REQUIRE_EQUAL((x), (y))
#define REQUIRE_NOT_EQUAL(x, y) CAF_REQUIRE_NOT_EQUAL((x), (y))
#define REQUIRE_LESS(x, y) CAF_REQUIRE_LESS((x), (y))
#define REQUIRE_LESS_EQUAL(x, y) CAF_REQUIRE_LESS_OR_EQUAL((x), (y))
#define REQUIRE_GREATER(x, y) CAF_REQUIRE_GREATER((x), (y))
#define REQUIRE_GREATER_EQUAL(x, y) CAF_REQUIRE_GREATER_OR_EQUAL((x), (y))
#define REQUIRE_NOERROR(x)                                                     \
  do {                                                                         \
    if (!(x)) {                                                                \
      CAF_FAIL(__FILE__, __LINE__);                                            \
    } else {                                                                   \
      MESSAGE("Successful check " #x);                                         \
    }                                                                          \
  } while (false)
#define REQUIRE_ERROR(x) REQUIRE_EQUAL(!(x), true)
#define REQUIRE_SUCCESS(x) REQUIRE_EQUAL((x), caf::none)
#define REQUIRE_FAILURE(x) REQUIRE_NOT_EQUAL((x), caf::none)
#define FAIL CAF_FAIL
// Checks that continue with the current test on failure
#define CHECK CAF_CHECK
#define CHECK_EQUAL(x, y)                                                      \
  do {                                                                         \
    using namespace vast;                                                      \
    CAF_CHECK_EQUAL((x), (y));                                                 \
  } while (false)
// CAF_CHECK_EQUAL((x), (y))
#define CHECK_NOT_EQUAL(x, y) CAF_CHECK_NOT_EQUAL((x), (y))
#define CHECK_LESS(x, y) CAF_CHECK_LESS((x), (y))
#define CHECK_LESS_EQUAL(x, y) CAF_CHECK_LESS_OR_EQUAL((x), (y))
#define CHECK_GREATER(x, y) CAF_CHECK_GREATER((x), (y))
#define CHECK_GREATER_EQUAL(x, y) CAF_CHECK_GREATER_OR_EQUAL((x), (y))
#define CHECK_ERROR(x) CHECK_EQUAL(!(x), true)
#define CHECK_SUCCESS(x) CHECK_EQUAL((x), caf::none)
#define CHECK_FAILURE(x) CHECK_NOT_EQUAL((x), caf::none)
// Checks that automagically handle caf::variant types.
#define CHECK_VARIANT_EQUAL(x, y)                                              \
  do {                                                                         \
    auto v1 = x;                                                               \
    decltype(v1) y_as_variant = y;                                             \
    CHECK_EQUAL(v1, y_as_variant);                                             \
  } while (false)
#define CHECK_VARIANT_NOT_EQUAL CAF_CHECK_NOT_EQUAL
#define CHECK_VARIANT_LESS CAF_CHECK_LESS
#define CHECK_VARIANT_LESS_EQUAL CAF_CHECK_LESS_OR_EQUAL
#define CHECK_VARIANT_GREATER CAF_CHECK_GREATER
#define CHECK_VARIANT_GREATER_EQUAL CAF_CHECK_GREATER_OR_EQUAL

// -- convenience macros for common check categories ---------------------------

// Checks whether a value initialized from `expr` compares equal to itself
// after a cycle of serializing and deserializing it. Requires the
// `deterministic_actor_system` fixture.
#define CHECK_ROUNDTRIP(...)                                                   \
  {                                                                            \
    auto __x = (__VA_ARGS__);                                                  \
    CHECK_EQUAL(roundtrip(__x), __x);                                          \
  }

// Like `CHECK_ROUNDTRIP`, but compares the objects by dereferencing them via
// `operator*` first.
#define CHECK_ROUNDTRIP_DEREF(...)                                             \
  {                                                                            \
    auto __x = (__VA_ARGS__);                                                  \
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

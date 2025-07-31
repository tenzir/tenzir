//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#ifdef SUITE
#  define CAF_SUITE SUITE
#endif

#include "tenzir/span.hpp"

#include <caf/allowed_unsafe_message_type.hpp>
#include <caf/detail/stringification_inspector.hpp>
#include <caf/inspector_access.hpp>
#include <caf/test/test.hpp>
#include <fmt/format.h>

#include <iostream>
#include <optional>
#include <set>
#include <string>

// Work around missing namespace qualification in CAF header.
// using ::caf::term;

namespace tenzir::test::detail {

struct equality_compare {
  template <class T1, class T2>
  bool operator()(const T1& t1, const T2& t2) {
    using tenzir::operator==;
    return t1 == t2;
  }
};

struct inequality_compare {
  template <class T1, class T2>
  bool operator()(const T1& t1, const T2& t2) {
    using tenzir::operator!=;
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

template <class T>
auto stringify(const T& value) {
  if constexpr (std::is_same_v<T, std::nullptr_t>) {
    return std::string{"null"};
  } else if constexpr (std::is_convertible_v<T, std::string>) {
    return std::string{value};
  } else {
    return caf::deep_to_string(value);
  }
}

template <class T0, class T1>
bool check_eq(const T0& lhs, const T1& rhs,
              caf::detail::source_location location
              = caf::detail::source_location::current()) {
  // Adapted from CAF, but without safety checks.
  if (lhs == rhs) {
    caf::test::reporter::instance().pass(location);
    return true;
  }
  caf::test::reporter::instance().fail(
    caf::test::binary_predicate::eq, stringify(lhs), stringify(rhs), location);
  return false;
}

} // end namespace tenzir::test::detail

// -- logging macros -----------------------------------------------------------

#define ERROR CAF_TEST_PRINT_ERROR
#define INFO CAF_TEST_PRINT_INFO
#define VERBOSE CAF_TEST_PRINT_VERBOSE
// The new testing framework does not have `CAF_MESSAGE` anymore.
#define MESSAGE(...) fmt::println(__VA_ARGS__)

// -- test setup macros --------------------------------------------------------

#define TEST_DISABLED CAF_TEST_DISABLED
#define FIXTURE_SCOPE CAF_TEST_FIXTURE_SCOPE
#define FIXTURE_SCOPE_END CAF_TEST_FIXTURE_SCOPE_END

// -- macros for checking results ----------------------------------------------
// Checks that abort the current test on failure
#define REQUIRE(x)                                                             \
  ::caf::test::runnable::current().require(static_cast<bool>(x))
#define REQUIRE_EQUAL(x, y)                                                    \
  ::caf::test::runnable::current().require_eq((x), (y))
#define REQUIRE_NOT_EQUAL(x, y)                                                \
  ::caf::test::runnable::current().require_ne((x), (y))
#define REQUIRE_LESS(x, y) ::caf::test::runnable::current().require_lt((x), (y))
#define REQUIRE_LESS_EQUAL(x, y)                                               \
  ::caf::test::runnable::current().require_le((x), (y))
#define REQUIRE_GREATER(x, y)                                                  \
  ::caf::test::runnable::current().require_gt((x), (y))
#define REQUIRE_GREATER_EQUAL(x, y)                                            \
  ::caf::test::runnable::current().require_ge((x), (y))
#define REQUIRE_NOERROR(x)                                                     \
  do {                                                                         \
    if (! (x)) {                                                               \
      ::caf::test::runnable::current().fail("Unexpected error {} in: {}",      \
                                            (x).error(), __FILE__);            \
    } else {                                                                   \
      MESSAGE("Successful check " #x);                                         \
    }                                                                          \
  } while (false)
#define REQUIRE_ERROR(x) REQUIRE_EQUAL(! (x), true)
#define REQUIRE_SUCCESS(x) REQUIRE_EQUAL((x), caf::none)
#define REQUIRE_FAILURE(x) REQUIRE_NOT_EQUAL((x), caf::none)
#define FAIL ::caf::test::runnable::current().fail
// Checks that continue with the current test on failure
#define CHECK(x) ::caf::test::runnable::current().check(static_cast<bool>(x))
#define CHECK_EQUAL(x, y) ::tenzir::test::detail::check_eq((x), (y))
#define CHECK_NOT_EQUAL(x, y)                                                  \
  ::caf::test::runnable::current().check_ne((x), (y))
#define CHECK_LESS(x, y) ::caf::test::runnable::current().check_lt((x), (y))
#define CHECK_LESS_EQUAL(x, y)                                                 \
  ::caf::test::runnable::current().check_le((x), (y))
#define CHECK_GREATER(x, y) ::caf::test::runnable::current().check_gt((x), (y))
#define CHECK_GREATER_EQUAL(x, y)                                              \
  ::caf::test::runnable::current().check_ge((x), (y))
#define CHECK_ERROR(x) CHECK_EQUAL(! (x), true)
#define CHECK_SUCCESS(x) CHECK_EQUAL((x), caf::none)
#define CHECK_FAILURE(x) CHECK_NOT_EQUAL((x), caf::none)
#define CHECK_VARIANT_EQUAL(x, y)                                              \
  do {                                                                         \
    auto v1 = (x);                                                             \
    decltype(v1) y_as_variant = (y);                                           \
    CHECK_EQUAL(v1, y_as_variant);                                             \
  } while (false)
#define CHECK_VARIANT_NOT_EQUAL ::caf::test::runnable::current().check_ne
#define CHECK_VARIANT_LESS ::caf::test::runnable::current().check_lt
#define CHECK_VARIANT_LESS_EQUAL ::caf::test::runnable::current().check_le
#define CHECK_VARIANT_GREATER ::caf::test::runnable::current().check_gt
#define CHECK_VARIANT_GREATER_EQUAL ::caf::test::runnable::current().check_ge

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

namespace tenzir::test {

template <class T>
T unbox(std::optional<T> x) {
  if (! x) {
    FAIL("x == none");
  }
  return std::move(*x);
}

template <class T>
T unbox(caf::expected<T> x) {
  if (! x) {
    FAIL("expected<T> contains an error: {}", x.error());
  }
  return std::move(*x);
}

template <class T>
T unbox(T* x) {
  if (! x) {
    FAIL("T* contains nullptr");
  }
  return std::move(*x);
}

// Holds global configuration options passed on the command line after the
// special -- delimiter.
extern std::set<std::string> config;

} // namespace tenzir::test

namespace tenzir {

using test::unbox;

} // namespace tenzir

//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/arc.hpp"

#include "tenzir/test/test.hpp"

#include <functional>
#include <string>

namespace tenzir {

// -- static assertions --------------------------------------------------------

static_assert(std::is_copy_constructible_v<Arc<int>>);
static_assert(std::is_move_constructible_v<Arc<int>>);
static_assert(std::is_default_constructible_v<Arc<int>>);
static_assert(std::is_copy_constructible_v<Arc<std::string>>);

// -- construction -------------------------------------------------------------

TEST("value construction") {
  auto a = Arc<int>{42};
  CHECK_EQUAL(*a, 42);
}

TEST("in-place construction") {
  auto a = Arc<std::string>{std::in_place, 5, 'x'};
  CHECK_EQUAL(*a, "xxxxx");
}

TEST("from_non_null") {
  auto ptr = std::make_shared<int>(42);
  auto a = Arc<int>::from_non_null(std::move(ptr));
  CHECK_EQUAL(*a, 42);
}

TEST("CTAD deduces value type") {
  auto a = Arc{42};
  static_assert(std::same_as<decltype(a), Arc<int>>);
  CHECK_EQUAL(*a, 42);
}

// -- copy shares ownership ----------------------------------------------------

TEST("copy increments refcount") {
  auto a = Arc<int>{42};
  CHECK_EQUAL(a.strong_count(), 1);
  auto b = a;
  CHECK_EQUAL(a.strong_count(), 2);
  CHECK_EQUAL(b.strong_count(), 2);
  CHECK_EQUAL(*a, *b);
}

TEST("copy assignment shares ownership") {
  auto a = Arc<int>{1};
  auto b = Arc<int>{2};
  CHECK_EQUAL(a.strong_count(), 1);
  b = a;
  CHECK_EQUAL(a.strong_count(), 2);
  CHECK_EQUAL(*b, 1);
}

TEST("move leaves source empty") {
  auto a = Arc<int>{42};
  auto b = std::move(a);
  CHECK_EQUAL(*b, 42);
  CHECK_EQUAL(b.strong_count(), 1);
}

// -- polymorphic sharing ------------------------------------------------------

namespace {

struct Base {
  virtual ~Base() = default;
  virtual auto value() const -> int {
    return 0;
  }
};

struct Derived : Base {
  auto value() const -> int override {
    return 42;
  }
};

} // namespace

TEST("polymorphic type sharing") {
  auto derived = std::make_shared<Derived>();
  auto a = Arc<Base>::from_non_null(derived);
  auto b = a;
  CHECK_EQUAL(a.strong_count(), 3); // derived + a + b
  CHECK_EQUAL(a->value(), 42);
  CHECK_EQUAL(b->value(), 42);
}

// -- access -------------------------------------------------------------------

TEST("converting construction") {
  auto a = Arc<std::string>{"hello"};
  CHECK_EQUAL(*a, std::string{"hello"});
}

TEST("operator-> provides member access") {
  auto a = Arc<std::string>{"hello"};
  CHECK_EQUAL(a->size(), size_t{5});
}

TEST("operator* dereferences") {
  auto a = Arc<int>{42};
  CHECK_EQUAL(*a, 42);
}

TEST("implicit conversion to reference") {
  auto a = Arc<int>{42};
  int const& ref = a;
  CHECK_EQUAL(ref, 42);
}

TEST("mutable access through operator->") {
  auto a = Arc<std::string>{"hello"};
  a->append(" world");
  CHECK_EQUAL(*a, std::string{"hello world"});
}

// -- callable -----------------------------------------------------------------

TEST("callable arc") {
  auto fn = std::function<int(int)>{[](int x) {
    return x * 2;
  }};
  auto f = Arc{std::move(fn)};
  CHECK_EQUAL(f(21), 42);
}

// -- equality -----------------------------------------------------------------

TEST("arc-arc equality") {
  CHECK(Arc<int>{42} == Arc<int>{42});
  CHECK(not(Arc<int>{1} == Arc<int>{2}));
}

TEST("arc-value equality") {
  CHECK(Arc<int>{42} == 42);
  CHECK(not(Arc<int>{42} == 99));
}

TEST("reversed comparison") {
  CHECK(42 == Arc<int>{42});
}

} // namespace tenzir

//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/any.hpp"

#include "tenzir/test/test.hpp"

#include <memory>
#include <string>

using namespace tenzir;

static_assert(sizeof(Any) == sizeof(void*));
static_assert(not std::is_copy_constructible_v<Any>);
static_assert(not std::is_copy_assignable_v<Any>);
static_assert(std::is_nothrow_move_constructible_v<Any>);
static_assert(std::is_nothrow_move_assignable_v<Any>);

TEST("default construction") {
  Any a;
  CHECK_EQUAL(a.try_as<int>(), nullptr);
}

TEST("construction from value") {
  Any a{42};
  REQUIRE(a.try_as<int>() != nullptr);
  CHECK_EQUAL(a.as<int>(), 42);
}

TEST("in-place construction") {
  Any a{std::in_place_type<std::string>, "hello"};
  REQUIRE(a.try_as<std::string>() != nullptr);
  CHECK_EQUAL(a.as<std::string>(), "hello");
}

TEST("move construction") {
  Any a{42};
  Any b{std::move(a)};
  CHECK_EQUAL(b.as<int>(), 42);
  // a is now empty (moved-from)
  CHECK_EQUAL(a.try_as<int>(), nullptr);
}

TEST("move assignment") {
  Any a{42};
  Any b{std::string{"hello"}};
  b = std::move(a);
  CHECK_EQUAL(b.as<int>(), 42);
  CHECK_EQUAL(a.try_as<int>(), nullptr);
}

TEST("self-move assignment") {
  Any a{42};
  auto* ptr = &a;
  *ptr = std::move(a);
  // unique_ptr self-move is well-defined: target.reset(source.release())
  CHECK_EQUAL(a.as<int>(), 42);
}

TEST("reset via move assignment from empty") {
  Any a{42};
  a = Any{};
  CHECK_EQUAL(a.try_as<int>(), nullptr);
}

TEST("type mismatch with try_as") {
  Any a{42};
  CHECK_EQUAL(a.try_as<double>(), nullptr);
  CHECK_EQUAL(a.try_as<std::string>(), nullptr);
}

TEST("empty state with try_as") {
  Any a;
  CHECK_EQUAL(a.try_as<int>(), nullptr);
  CHECK_EQUAL(a.try_as<std::string>(), nullptr);
}

TEST("const access") {
  Any a{42};
  auto const& ca = a;
  CHECK_EQUAL(ca.as<int>(), 42);
  REQUIRE(ca.try_as<int>() != nullptr);
  CHECK_EQUAL(*ca.try_as<int>(), 42);
}

TEST("modification through as") {
  Any a{42};
  a.as<int>() = 100;
  CHECK_EQUAL(a.as<int>(), 100);
}

TEST("modification through try_as") {
  Any a{42};
  auto* ptr = a.try_as<int>();
  REQUIRE(ptr != nullptr);
  *ptr = 100;
  CHECK_EQUAL(a.as<int>(), 100);
}

TEST("move-only type") {
  auto ptr = std::make_unique<int>(42);
  Any a{std::move(ptr)};
  REQUIRE(a.try_as<std::unique_ptr<int>>() != nullptr);
  CHECK_EQUAL(*a.as<std::unique_ptr<int>>(), 42);
}

TEST("move value out via rvalue as") {
  Any a{std::string{"hello"}};
  std::string s = std::move(a).as<std::string>();
  CHECK_EQUAL(s, "hello");
  // a still contains the moved-from string
  CHECK(a.try_as<std::string>() != nullptr);
}

TEST("destruction tracking") {
  int count = 0;
  struct Tracker {
    int* count;
    ~Tracker() {
      ++(*count);
    }
  };
  {
    Any a{Tracker{&count}};
    // One destruction from the temporary
  }
  // One destruction when Any goes out of scope
  CHECK_EQUAL(count, 2);
}

TEST("destruction on reassignment") {
  int count = 0;
  struct Tracker {
    int* count;
    ~Tracker() {
      ++(*count);
    }
  };
  Any a{Tracker{&count}};
  auto count_after_construction = count;
  a = Any{42};
  CHECK_EQUAL(count, count_after_construction + 1);
}

//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/operators.hpp"

#include "tenzir/test/test.hpp"

using namespace tenzir::detail;

namespace {

struct foo : addable<foo>,
             addable<foo, int> {
  explicit foo(int x) : value{x} {
    // nop
  }

  foo& operator+=(const foo& other) {
    value += other.value;
    return *this;
  }

  foo& operator+=(int x) {
    value += x;
    return *this;
  }

  int value;
};

} // namespace

TEST("commutative operators") {
  auto x = foo{42};
  auto y = foo{-3};
  auto result = 1 + x + 1 + y + 1;
  CHECK_EQUAL(result.value, 42);
}

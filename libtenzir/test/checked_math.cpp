//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/checked_math.hpp"

#include "tenzir/test/test.hpp"

namespace tenzir {

namespace {

using S = int64_t;
using U = uint64_t;

// These are macros to preserve line numbers.
#define GOOD(x, y)                                                             \
  static_assert(                                                               \
    std::same_as<decltype(x),                                                  \
                 std::optional<std::remove_cvref_t<decltype(y)>>>);            \
  CHECK_EQUAL(x, y);
#define BAD(x) CHECK_EQUAL(x, std::nullopt);

TEST(checked_add) {
  // unsigned
  GOOD(checked_add(U(0), U(0)), U(0));
  GOOD(checked_add(max<U>, U(0)), max<U>);
  BAD(checked_add(max<U>, U(1)));
  BAD(checked_add(max<U>, max<U>));
  // signed
  GOOD(checked_add(S(0), S(0)), S(0));
  GOOD(checked_add(min<S>, S(0)), min<S>);
  GOOD(checked_add(S(0), max<S>), max<S>);
  GOOD(checked_add(min<S>, max<S>), S(-1));
  BAD(checked_add(min<S>, S(-1)));
  BAD(checked_add(min<S>, min<S>));
  BAD(checked_add(max<S>, S(1)));
  BAD(checked_add(max<S>, max<S>));
  // mixed
  GOOD(checked_add(U(0), S(0)), U(0));
  GOOD(checked_add(U(0), max<S>), U(max<S>));
  GOOD(checked_add(U(max<S>), max<S>), U(2) * U(max<S>));
  GOOD(checked_add(U(max<S>) + 1, min<S>), U(0));
  BAD(checked_add(max<U> - U(max<S>) + U(1), max<S>));
  GOOD(checked_add(max<U> - U(max<S>), max<S>), max<U>);
}

TEST(checked_sub) {
  // unsigned - unsigned
  GOOD(checked_sub(U(0), U(0)), U(0));
  BAD(checked_sub(U(0), U(1)));
  GOOD(checked_sub(max<U>, U(0)), max<U>);
  GOOD(checked_sub(max<U>, max<U>), U(0));
  // signed - signed
  GOOD(checked_sub(S(0), S(0)), S(0));
  GOOD(checked_sub(max<S>, S(0)), max<S>);
  GOOD(checked_sub(max<S>, max<S>), S(0));
  BAD(checked_sub(max<S>, S(-1)));
  GOOD(checked_sub(min<S>, S(0)), min<S>);
  GOOD(checked_sub(min<S>, min<S>), S(0));
  GOOD(checked_sub(min<S>, S(-1)), min<S> + S(1));
  BAD(checked_sub(min<S>, S(1)));
  BAD(checked_sub(min<S>, max<S>));
  // signed - unsigned
  GOOD(checked_sub(S(0), U(0)), S(0));
  GOOD(checked_sub(max<S>, U(0)), max<S>);
  GOOD(checked_sub(max<S>, U(max<S>)), S(0));
  GOOD(checked_sub(min<S>, U(0)), min<S>);
  BAD(checked_sub(min<S>, U(1)));
  BAD(checked_sub(min<S>, U(max<S>)));
  GOOD(checked_sub(max<S>, U(max<S>) + 1), S(-1));
  GOOD(checked_sub(max<S>, max<U>), min<S>);
  // unsigned - signed
  GOOD(checked_sub(U(0), S(0)), U(0));
  GOOD(checked_sub(U(0), S(-1)), U(1));
  GOOD(checked_sub(U(0), min<S>), U(max<S>) + 1);
  BAD(checked_sub(U(0), S(1)));
  BAD(checked_sub(U(0), max<S>));
  GOOD(checked_sub(U(max<S>), max<S>), U(0));
  BAD(checked_sub(U(max<S> - 1), max<S>));
  GOOD(checked_sub(U(max<S>), S(0)), U(max<S>));
  GOOD(checked_sub(U(max<S>), min<S>), max<U>);
}

TEST(checked_mul) {
  // unsigned * unsigned
  GOOD(checked_mul(U(0), U(0)), U(0));
  GOOD(checked_mul(U(1), U(0)), U(0));
  GOOD(checked_mul(U(1), U(1)), U(1));
  GOOD(checked_mul(U(0), U(1)), U(0));
  GOOD(checked_mul(max<U> / 2, U(2)), max<U> - 1);
  GOOD(checked_mul(max<U> / 4, U(4)), max<U> - 3);
  GOOD(checked_mul(max<U> / 6, U(6)), max<U> - 3);
  GOOD(checked_mul(max<U> / 8, U(8)), max<U> - 7);
  GOOD(checked_mul(max<U>, U(1)), max<U>);
  GOOD(checked_mul(max<U> / 3, U(3)), max<U>);
  GOOD(checked_mul(max<U> / 5, U(5)), max<U>);
  GOOD(checked_mul(max<U> / 7, U(7)), max<U> - 1);
  BAD(checked_mul(max<U> / 2 + 1, U(2)));
  BAD(checked_mul(U(2), max<U> / 2 + 1));
  // signed * unsigned
  GOOD(checked_mul(S(0), U(max<S>)), S(0));
  GOOD(checked_mul(S(0), max<U>), S(0));
  GOOD(checked_mul(S(1), U(max<S>)), max<S>);
  GOOD(checked_mul(max<S>, U(1)), max<S>);
  GOOD(checked_mul(min<S>, U(1)), min<S>);
  BAD(checked_mul(min<S>, U(2)));
  BAD(checked_mul(S(1), U(max<S>) + 1));
  BAD(checked_mul(S(2), U(max<S>)));
  GOOD(checked_mul(S(-1), U(max<S>) + 1), min<S>);
  BAD(checked_mul(S(-1), U(max<S>) + 2));
  // signed * signed
  GOOD(checked_mul(S(2), S(3)), S(6));
  GOOD(checked_mul(S(3), S(2)), S(6));
  GOOD(checked_mul(S(-2), S(3)), S(-6));
  GOOD(checked_mul(S(3), S(-2)), S(-6));
  GOOD(checked_mul(S(-2), S(-3)), S(6));
  GOOD(checked_mul(S(-3), S(-2)), S(6));
  GOOD(checked_mul(S(2), S(-3)), S(-6));
  GOOD(checked_mul(S(-3), S(2)), S(-6));
  GOOD(checked_mul(S(1), max<S>), max<S>);
  GOOD(checked_mul(max<S>, S(1)), max<S>);
  GOOD(checked_mul(S(-1), max<S>), -max<S>);
  GOOD(checked_mul(max<S>, S(-1)), -max<S>);
  GOOD(checked_mul(S(1), min<S>), min<S>);
  GOOD(checked_mul(min<S>, S(1)), min<S>);
  BAD(checked_mul(S(-1), min<S>));
  BAD(checked_mul(min<S>, S(-1)));
  BAD(checked_mul(min<S>, max<S>));
}

} // namespace

} // namespace tenzir

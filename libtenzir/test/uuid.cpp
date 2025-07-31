//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/uuid.hpp"

#include "tenzir/concept/parseable/tenzir/uuid.hpp"
#include "tenzir/concept/parseable/to.hpp"
#include "tenzir/concept/printable/tenzir/uuid.hpp"
#include "tenzir/concept/printable/to_string.hpp"
#include "tenzir/test/test.hpp"

#include <span>

using namespace tenzir;

TEST("pod size") {
  CHECK(sizeof(uuid) == size_t{16});
}

TEST("parseable and printable") {
  auto x = to<uuid>("01234567-89ab-cdef-0123-456789abcdef");
  REQUIRE(x);
  CHECK_EQUAL(to_string(*x), "01234567-89ab-cdef-0123-456789abcdef");
}

TEST("construction from span") {
  std::array<char, 16> bytes{0, 1, 2,  3,  4,  5,  6,  7,
                             8, 9, 10, 12, 12, 13, 14, 15};
  auto bytes_view = as_bytes(std::span<char, 16>{bytes});
  CHECK(bytes_view == as_bytes(uuid{bytes_view}));
}

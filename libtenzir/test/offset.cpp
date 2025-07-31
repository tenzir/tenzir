//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/offset.hpp"

#include "tenzir/concept/parseable/tenzir/offset.hpp"
#include "tenzir/concept/parseable/to.hpp"
#include "tenzir/concept/printable/tenzir/offset.hpp"
#include "tenzir/concept/printable/to_string.hpp"
#include "tenzir/test/test.hpp"

using namespace tenzir;

TEST("offset printing") {
  auto o = offset{0, 10, 8};
  CHECK(to_string(o) == "0,10,8");
}

TEST("offset parsing") {
  auto o = to<offset>("0,4,8,12");
  CHECK(o);
  CHECK(*o == offset({0, 4, 8, 12}));
}

TEST("offset sorting") {
  // clang-format off
  const auto os = std::vector<offset>{
    {},
    {0, 0},
    {0, 0, 1}, 
    {0, 1, 0, 0}, 
    {0, 2, 1, 0}, 
    {1, 0},
  };
  // clang-format on
  CHECK(std::is_sorted(os.begin(), os.end()));
}

//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/offset.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/offset.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/offset.hpp"
#include "vast/test/test.hpp"

using namespace vast;

TEST(offset printing) {
  auto o = offset{0, 10, 8};
  CHECK(to_string(o) == "0,10,8");
}

TEST(offset parsing) {
  auto o = to<offset>("0,4,8,12");
  CHECK(o);
  CHECK(*o == offset({0, 4, 8, 12}));
}

TEST(offset sorting) {
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

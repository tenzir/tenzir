//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/span.hpp"

#include "vast/detail/narrow.hpp"

#include <array>
#include <cstddef>
#include <string>
#include <vector>

#define SUITE span
#include "vast/test/test.hpp"

using namespace std::string_literals;
using namespace vast;

TEST(string) {
  auto foo = "foo"s;
  auto x = span<char>{foo};
  CHECK_EQUAL(x.size(), 3u);
}

TEST(byte) {
  auto b = std::byte{0b0000'1100};
  auto x = span<std::byte>{&b, 1};
  CHECK_EQUAL(x.size(), 1u);
  auto foo = "foo"s;
  x = span<std::byte>(reinterpret_cast<std::byte*>(foo.data()), foo.size());
  CHECK_EQUAL(x.size(), 3u);
  CHECK_EQUAL(x[0], std::byte{'f'});
}

TEST(subspan) {
  auto xs = std::vector<int>{1, 2, 3, 4, 5, 6, 7};
  auto ys = span<int>{xs};
  auto zs = ys.subspan(2, 3);
  REQUIRE_EQUAL(zs.size(), 3u);
  CHECK_EQUAL(zs[0], 3);
  CHECK_EQUAL(zs[1], 4);
  CHECK_EQUAL(zs[2], 5);
}

TEST(construct from empty array) {
  std::array<int, 42> xs;
  CHECK_EQUAL(span<int>{xs}.size(), 42u);
}

TEST(byte span utility) {
  std::array<int8_t, 42> xs;
  auto ys = as_writeable_bytes(span{xs.data(), xs.size()});
  ys[0] = std::byte{0xff};
  CHECK_EQUAL(ys[0], std::byte{0xff});
  auto zs = as_bytes(span{xs.data(), xs.size()});
  CHECK_EQUAL(zs[0], std::byte{0xff});
}

//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/concept/hashable/hash_append.hpp"

#include "vast/detail/bit.hpp"

#define SUITE hashable
#include "vast/test/test.hpp"

#include <cstdint>
#include <tuple>

using namespace vast;

namespace {

struct fake_hasher {
  static constexpr detail::endian endian = detail::endian::native;

  void operator()(const void*, size_t n) {
    num_bytes += n;
  }

  std::uint64_t num_bytes{};
};

} // namespace

TEST(lvalue tuple) {
  fake_hasher h{};
  const auto t = std::tuple{42, 'A'};
  hash_append(h, t);
  CHECK(h.num_bytes == sizeof(int) + sizeof(char));
}

TEST(rvalue tuple) {
  fake_hasher h{};
  hash_append(h, std::tuple{42, 'A'});
  CHECK(h.num_bytes == sizeof(int) + sizeof(char));
}

//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/hash/hash_append.hpp"

#include "vast/detail/bit.hpp"
#include "vast/hash/default_hash.hpp"
#include "vast/hash/uhash.hpp"

#define SUITE hashable
#include "vast/test/test.hpp"

#include <cstdint>
#include <tuple>

using namespace vast;

namespace {

struct fake_hasher {
  static constexpr detail::endian endian = detail::endian::native;

  using result_type = size_t;

  void add(std::span<const std::byte> bytes) noexcept {
    num_bytes += bytes.size();
  }

  result_type finish() const noexcept {
    return num_bytes;
  }

  size_t num_bytes = 0;
};

struct foo {
  int a = 42;
  int b = 1337;
};

template <class Inspector>
auto inspect(Inspector& f, foo& x) {
  return f(x.a, x.b);
}

} // namespace

TEST(lvalue tuple) {
  fake_hasher h{};
  const auto t = std::tuple{42, 'A'};
  hash_append(h, t);
  CHECK(h.finish() == sizeof(int) + sizeof(char));
}

TEST(rvalue tuple) {
  fake_hasher h{};
  hash_append(h, std::tuple{42, 'A'});
  CHECK(h.finish() == sizeof(int) + sizeof(char));
}

TEST(hashing an inspectable type) {
  // Manual hashing two values...
  auto a = 42;
  auto b = 1337;
  default_hash h;
  hash_append(h, a);
  hash_append(h, b);
  auto manual_digest = h.finish();
  // ...and hashing them through the inspection API...
  auto inspect_digest = uhash<default_hash>{}(foo{});
  // ...must yield the same value.
  CHECK_EQUAL(manual_digest, inspect_digest);
}

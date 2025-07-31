//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/hash/hash.hpp"

#include "tenzir/test/test.hpp"

#include <bit>
#include <cstdint>
#include <tuple>

using namespace tenzir;

namespace {

struct algorithm_base {
  static constexpr std::endian endian = std::endian::native;
  using result_type = size_t;
};

// A hash algorithm that only operates in a one-shot fasion.
struct oneshot : algorithm_base {
  static result_type make(std::span<const std::byte> bytes) noexcept {
    return bytes.size();
  }
};

// A hash algorithm that only operates incrementally.
struct incremental : algorithm_base {
  void add(std::span<const std::byte>) noexcept {
  }

  result_type finish() noexcept {
    return 0;
  }
};

// A hash algorithm that is both oneshot and incremental.
struct oneshot_and_incremental : algorithm_base {
  static result_type make(std::span<const std::byte> bytes) noexcept {
    return bytes.size();
  }

  void add(std::span<const std::byte>) noexcept {
  }

  result_type finish() noexcept {
    return 0;
  }
};

// A type that models a fixed-size byte sequence by exposing an as_bytes
// function with a non-dynamic extent.
struct fixed {
  friend std::span<const std::byte, 1> as_bytes(const fixed& x) {
    return std::span<const std::byte, 1>{x.bytes, 1};
  }

  std::byte bytes[64];
} __attribute__((__packed__));

// A type that can be hashed by either (1) taking its memory address and size,
// or (2) accessing it as fixed byte sequence.
struct fixed_and_unique : fixed {
} __attribute__((__packed__));

} // namespace

namespace tenzir {

template <>
struct is_uniquely_represented<fixed_and_unique> : std::true_type {};

} // namespace tenzir

TEST("hash via oneshot hashing") {
  uint16_t u16 = 0;
  static_assert(uniquely_hashable<decltype(u16), oneshot>);
  static_assert(uniquely_hashable<decltype(u16), incremental>);
  CHECK_EQUAL(hash<oneshot>(u16), sizeof(u16));
  CHECK_EQUAL(hash<incremental>(u16), 0u);
}

TEST("prefer fast path when both is available") {
  auto u16 = uint16_t{0};
  auto f64 = double{4.2};
  static_assert(uniquely_hashable<decltype(u16), oneshot_and_incremental>);
  static_assert(!uniquely_hashable<decltype(f64), oneshot_and_incremental>);
  CHECK_EQUAL(hash<oneshot_and_incremental>(u16), sizeof(u16)); // oneshot
  CHECK_EQUAL(hash<oneshot_and_incremental>(f64), 0u);          // incremental
}

TEST("hash fixed byte sequences in one shot") {
  CHECK_EQUAL(as_bytes(fixed{}).size(), 1u);
  CHECK_EQUAL(hash<oneshot_and_incremental>(fixed{}), 1u);
}

TEST("hash byte sequence that is fixed and unique") {
  // Make sure we're not going via as_bytes when we can take the address.
  static_assert(sizeof(fixed_and_unique) == 64u);
  CHECK_EQUAL(hash<oneshot_and_incremental>(fixed_and_unique{}), 64u);
}

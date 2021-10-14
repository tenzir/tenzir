//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/concept/hashable/hash.hpp"

#include "vast/detail/bit.hpp"

#define SUITE hashable
#include "vast/test/test.hpp"

#include <cstdint>
#include <tuple>

using namespace vast;

namespace {

struct algorithm_base {
  static constexpr detail::endian endian = detail::endian::native;
  using result_type = bool;
};

struct oneshot : algorithm_base {
  static result_type make(const void*, size_t) noexcept {
    return true;
  }
};

struct incremental : algorithm_base {
  void operator()(const void*, size_t) noexcept {
  }

  explicit operator result_type() noexcept {
    return false;
  }
};

struct oneshot_and_incremental : algorithm_base {
  static result_type make(const void*, size_t) noexcept {
    return true;
  }

  void operator()(const void*, size_t) noexcept {
  }

  explicit operator result_type() noexcept {
    return false;
  }
};

} // namespace

TEST(check the fast path) {
  CHECK(hash<oneshot>(0));
  CHECK(!hash<incremental>(std::byte{42}));
  CHECK(hash<oneshot_and_incremental>(std::byte{42}));
  CHECK(!hash<oneshot_and_incremental>(double{1.0}));
}

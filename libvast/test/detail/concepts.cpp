//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE concepts

#include "vast/detail/concepts.hpp"

#include "vast/test/test.hpp"

#include <array>
#include <type_traits>

TEST(transparent) {
  struct with {
    using is_transparent = std::true_type;
  };
  struct without {};
  static_assert(vast::detail::transparent<with>);
  static_assert(!vast::detail::transparent<without>);
}

TEST(container) {
  static_assert(vast::detail::container<std::array<int, 1>>);
  struct empty {};
  static_assert(!vast::detail::container<empty>);
  struct user_defined_type {
    auto data() const {
      return nullptr;
    }
    auto size() const {
      return 0;
    }
  };
  static_assert(vast::detail::container<user_defined_type>);
}

TEST(byte_container) {
  using fake_byte_container_t = std::array<std::uint8_t, 2>;
  static_assert(vast::detail::byte_container<fake_byte_container_t>);
  struct not_byte_container {};
  static_assert(!vast::detail::byte_container<not_byte_container>);
}

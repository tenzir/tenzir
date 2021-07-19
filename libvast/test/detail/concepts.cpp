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

#include <type_traits>

TEST(transparent) {
  struct with {
    using is_transparent = std::true_type;
  };
  struct without {};
  static_assert(vast::detail::transparent<with>);
  static_assert(!vast::detail::transparent<without>);
}

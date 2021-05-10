//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE type_traits

#include "vast/detail/type_traits.hpp"

#include "vast/test/test.hpp"

TEST(sum) {
  static_assert(vast::detail::sum<> == 0);
  static_assert(vast::detail::sum<1, 2, 3> == 6);
  static_assert(vast::detail::sum<42, 58> == 100);
}

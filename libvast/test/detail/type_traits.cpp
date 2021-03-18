// SPDX-FileCopyrightText: (c) 2021 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE type_traits

#include "vast/test/test.hpp"

#include "vast/detail/type_traits.hpp"

TEST(sum) {
  static_assert(vast::detail::sum<> == 0);
  static_assert(vast::detail::sum<1, 2, 3> == 6);
  static_assert(vast::detail::sum<42, 58> == 100);
}


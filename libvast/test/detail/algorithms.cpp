// SPDX-FileCopyrightText: (c) 2018 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE algorithms

#include "vast/detail/algorithms.hpp"

#include "vast/test/test.hpp"

#include <map>
#include <vector>

using vast::detail::unique_values;

using imap = std::map<int, int>;

using ivec = std::vector<int>;

TEST(empty collection values) {
  CHECK_EQUAL(unique_values(imap()), ivec());
}

TEST(unique collection values) {
  CHECK_EQUAL(unique_values(imap({{1, 10}, {2, 30}, {3, 30}})), ivec({10, 30}));
}

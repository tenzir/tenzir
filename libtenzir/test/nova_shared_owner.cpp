//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/shared_owner.hpp"
#include "tenzir/test/test.hpp"

#include <ranges>
#include <string>

using namespace tenzir::nova::storage;

TEST("bulk append handles transformed values without dangling") {
  auto values = std::views::iota(0, 5) | std::views::transform([](int value) {
                  return 42 + value;
                });
  auto builder = DataOwner<int[]>::make_uninitialized(6);
  builder.emplace_back(123);
  builder.move_append(values.begin(), values.end());
  auto result = builder.finish();
  CHECK_EQUAL(result.length(), 6);
  CHECK_EQUAL(result[0], 123);
  for (auto i = Index{0}; i < 5; ++i) {
    CHECK_EQUAL(result[i + 1], 42 + i);
  }
}

TEST("bulk append handles nontrivial transformed values") {
  auto values = std::views::iota(0, 5) | std::views::transform([](int value) {
                  return std::string(100, 'x') + std::to_string(value);
                });
  auto builder = DataOwner<std::string[]>::Builder{};
  builder.move_append(values.begin(), values.end());
  auto result = builder.finish();
  CHECK_EQUAL(result.length(), 5);
  for (auto i = Index{0}; i < 5; ++i) {
    CHECK_EQUAL(result[i], std::string(100, 'x') + std::to_string(i));
  }
}

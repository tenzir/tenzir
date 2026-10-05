//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/shared_owner.hpp"
#include "tenzir/test/test.hpp"

#include <limits>
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

TEST("uninitialized owners may start empty") {
  auto empty = DataOwner<int[]>::make_uninitialized(0);
  CHECK_EQUAL(empty.capacity(), 0);
  CHECK_EQUAL(empty.finish().length(), 0);
  auto builder = DataOwner<int[]>::make_uninitialized(0);
  builder.emplace_back(42);
  auto result = builder.finish();
  CHECK_EQUAL(result.length(), 1);
  CHECK_EQUAL(result[0], 42);
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

TEST("reserve preserves elements and satisfies its capacity contract") {
  auto exercise = []<class T>(T value) {
    auto builder = typename DataOwner<T[]>::Builder{};
    builder.reserve_exact(0);
    CHECK_EQUAL(builder.capacity(), 0);
    builder.reserve_exact(3);
    CHECK_EQUAL(builder.capacity(), 3);
    builder.emplace_back(value);
    builder.reserve_exact(7);
    CHECK_EQUAL(builder.capacity(), 7);
    CHECK_EQUAL(builder.size(), 1);
    CHECK_EQUAL(builder[0], value);
    builder.reserve_exact(2);
    CHECK_EQUAL(builder.capacity(), 7);
    builder.reserve_at_least(20);
    CHECK(builder.capacity() >= 20);
    CHECK_EQUAL(builder.size(), 1);
    CHECK_EQUAL(builder[0], value);
    auto const capacity = builder.capacity();
    builder.reserve_at_least(0);
    CHECK_EQUAL(builder.capacity(), capacity);
    CHECK_EQUAL(builder.finish()[0], value);
  };
  exercise(42);
  exercise(std::string(100, 'x'));
}

TEST("growth beyond a third of the index range stays within it") {
  // Growing by half used to overflow for targets beyond a third of the range
  // of `Index`, which left the builder without the requested capacity.
  CHECK_EQUAL(_::grow_capacity(Index{10}), Index{16});
  CHECK_EQUAL(_::grow_capacity(Index{800'000'000}), Index{1'200'000'001});
  CHECK_EQUAL(_::grow_capacity(Index{1'500'000'000}),
              std::numeric_limits<Index>::max());
  CHECK_EQUAL(_::grow_capacity(std::numeric_limits<Index>::max()),
              std::numeric_limits<Index>::max());
}

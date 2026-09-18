//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/bitmap_iteration.hpp"
#include "tenzir/test/test.hpp"

#include <ranges>
#include <vector>

using namespace tenzir;
using namespace tenzir::nova::storage;

static_assert(std::ranges::input_range<BitMapIteration>);
static_assert(std::ranges::input_range<TrueBits>);

namespace {

auto check_iteration(BitMap const& bitmap,
                     std::vector<Index> const& set_indices) -> void {
  auto expected = set_indices.begin();
  auto count = Index{0};
  for (auto index : bitmap_iteration(bitmap)) {
    if (expected != set_indices.end() and *expected == count) {
      REQUIRE(index);
      CHECK_EQUAL(*index, count);
      ++expected;
    } else {
      CHECK(not index);
    }
    ++count;
  }
  CHECK_EQUAL(count, bitmap.length());
  CHECK(expected == set_indices.end());
}

auto check_true_bits(BitMap const& bitmap,
                     std::vector<Index> const& set_indices) -> void {
  auto expected = set_indices.begin();
  for (auto index : true_bits(bitmap)) {
    REQUIRE(expected != set_indices.end());
    CHECK_EQUAL(index, *expected);
    ++expected;
  }
  CHECK(expected == set_indices.end());
}

} // namespace

TEST("bitmap iteration handles empty and constant bitmaps") {
  check_iteration(BitMap{0, false}, {});
  check_iteration(BitMap{5, false}, {});
  check_iteration(BitMap{5, true}, {0, 1, 2, 3, 4});
  check_true_bits(BitMap{0, false}, {});
  check_true_bits(BitMap{5, false}, {});
  check_true_bits(BitMap{5, true}, {0, 1, 2, 3, 4});
}

TEST("true bit iteration handles materialized constant values") {
  auto all_false = BitMap::Mutable{5};
  check_true_bits(std::move(all_false).finish(), {});
  auto all_true = BitMap::Mutable{5};
  for (auto index = Index{0}; index < all_true.length(); ++index) {
    all_true.set(index, true);
  }
  check_true_bits(std::move(all_true).finish(), {0, 1, 2, 3, 4});
}

TEST("bitmap iteration crosses word boundaries") {
  auto const set_indices
    = std::vector<Index>{0, 1, 126, 127, 128, 129, 255, 256};
  auto builder = BitMap::Builder{};
  auto expected = set_indices.begin();
  for (auto index = Index{0}; index < 257; ++index) {
    auto const is_set = expected != set_indices.end() and *expected == index;
    builder.emplace_back(is_set);
    if (is_set) {
      ++expected;
    }
  }
  auto bitmap = builder.finish();
  check_iteration(bitmap, set_indices);
  check_true_bits(bitmap, set_indices);
}

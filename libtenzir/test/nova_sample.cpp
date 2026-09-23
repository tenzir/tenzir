//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/sample.hpp"
#include "tenzir/test/test.hpp"

using namespace tenzir::nova;

TEST("sampling stride follows active rows across batch boundaries") {
  auto first = sample_mask(storage::BitMap{2, true}, 0, 3, 10);
  CHECK(first.get(0));
  CHECK(not first.get(1));
  auto input = storage::BitMap::Mutable{7};
  for (auto row : {0, 2, 3, 5, 6}) {
    input.set(row, true);
  }
  auto mask = std::move(input).finish();
  auto second = sample_mask(mask, 2, 3, 10);
  for (auto row = storage::Index{0}; row < mask.length(); ++row) {
    CHECK_EQUAL(second.get(row), row == 2 or row == 6);
  }
  auto capped = sample_mask(mask, 2, 3, 1);
  for (auto row = storage::Index{0}; row < mask.length(); ++row) {
    CHECK_EQUAL(capped.get(row), row == 2);
  }
  CHECK(not sample_mask(mask, 2, 3, 0).any());
}

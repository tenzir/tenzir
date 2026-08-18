//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/series_builder.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/test/test.hpp"

namespace tenzir {

namespace {

auto make_test_slice() -> table_slice {
  auto b = series_builder{};
  for (auto x : {10, 20, 30, 40, 50}) {
    b.record().field("x").data(int64_t{x});
  }
  auto slices = b.finish_as_table_slice("test");
  REQUIRE_EQUAL(slices.size(), size_t{1});
  return std::move(slices[0]);
}

TEST("subslice shifts a valid offset") {
  auto slice = make_test_slice();
  slice.offset(100);
  auto sub = subslice(slice, 1, 3);
  CHECK_EQUAL(sub.rows(), size_t{2});
  CHECK_EQUAL(sub.offset(), id{101});
}

TEST("subslice keeps an invalid offset invalid") {
  auto slice = make_test_slice();
  slice.offset(invalid_id);
  auto sub = subslice(slice, 1, 3);
  CHECK_EQUAL(sub.rows(), size_t{2});
  CHECK_EQUAL(sub.offset(), invalid_id);
}

TEST("subslice of a non-contiguous take has no offset") {
  auto slice = make_test_slice();
  slice.offset(100);
  // A non-contiguous take does not occupy a dense ID range, so `take_rows`
  // drops the offset. Sub-slicing the result must not invent one.
  auto taken = take_rows(slice, std::array<int64_t, 3>{4, 0, 2});
  REQUIRE_EQUAL(taken.rows(), size_t{3});
  REQUIRE_EQUAL(taken.offset(), invalid_id);
  auto sub = subslice(taken, 1, 3);
  CHECK_EQUAL(sub.rows(), size_t{2});
  CHECK_EQUAL(sub.offset(), invalid_id);
}

} // namespace

} // namespace tenzir

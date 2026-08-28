//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/arrow_table_slice.hpp"
#include "tenzir/arrow_utils.hpp"
#include "tenzir/option.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/test/test.hpp"

#include <arrow/api.h>

#include <algorithm>

namespace tenzir {

namespace {

auto make_mask(std::vector<bool> bits) -> std::shared_ptr<arrow::BooleanArray> {
  auto builder = arrow::BooleanBuilder{};
  for (auto bit : bits) {
    check(builder.Append(bit));
  }
  return finish(builder);
}

auto make_nullable_mask(std::vector<Option<bool>> bits)
  -> std::shared_ptr<arrow::BooleanArray> {
  auto builder = arrow::BooleanBuilder{};
  for (auto bit : bits) {
    if (bit.is_none()) {
      check(builder.AppendNull());
    } else {
      check(builder.Append(*bit));
    }
  }
  return finish(builder);
}

auto make_test_slice() -> table_slice {
  auto b = series_builder{};
  b.record().field("x").data(int64_t{10});
  b.record().field("x").data(int64_t{20});
  b.record().field("x").data(int64_t{30});
  b.record().field("x").data(int64_t{40});
  b.record().field("x").data(int64_t{50});
  auto slices = b.finish_as_table_slice("test");
  REQUIRE_EQUAL(slices.size(), size_t{1});
  return std::move(slices[0]);
}

} // namespace

TEST("filter table slice - keep all") {
  auto slice = make_test_slice();
  auto mask = make_mask({true, true, true, true, true});
  auto result = filter(slice, *mask);
  REQUIRE_EQUAL(result.rows(), uint64_t{5});
  CHECK_EQUAL(materialize(result.at(0, 0)), int64_t{10});
  CHECK_EQUAL(materialize(result.at(4, 0)), int64_t{50});
}

TEST("filter table slice - drop all") {
  auto slice = make_test_slice();
  auto mask = make_mask({false, false, false, false, false});
  auto result = filter(slice, *mask);
  CHECK_EQUAL(result.rows(), uint64_t{0});
}

TEST("filter table slice - alternating") {
  auto slice = make_test_slice();
  auto mask = make_mask({true, false, true, false, true});
  auto result = filter(slice, *mask);
  REQUIRE_EQUAL(result.rows(), uint64_t{3});
  CHECK_EQUAL(materialize(result.at(0, 0)), int64_t{10});
  CHECK_EQUAL(materialize(result.at(1, 0)), int64_t{30});
  CHECK_EQUAL(materialize(result.at(2, 0)), int64_t{50});
}

TEST("filter table slice - keep first and last") {
  auto slice = make_test_slice();
  auto mask = make_mask({true, false, false, false, true});
  auto result = filter(slice, *mask);
  REQUIRE_EQUAL(result.rows(), uint64_t{2});
  CHECK_EQUAL(materialize(result.at(0, 0)), int64_t{10});
  CHECK_EQUAL(materialize(result.at(1, 0)), int64_t{50});
}

TEST("filter table slice - keep middle") {
  auto slice = make_test_slice();
  auto mask = make_mask({false, true, true, true, false});
  auto result = filter(slice, *mask);
  REQUIRE_EQUAL(result.rows(), uint64_t{3});
  CHECK_EQUAL(materialize(result.at(0, 0)), int64_t{20});
  CHECK_EQUAL(materialize(result.at(1, 0)), int64_t{30});
  CHECK_EQUAL(materialize(result.at(2, 0)), int64_t{40});
}

TEST("filter table slice - nulls count as false") {
  auto slice = make_test_slice();
  auto mask = make_nullable_mask({true, None{}, true, None{}, false});
  auto result = filter(slice, *mask);
  REQUIRE_EQUAL(result.rows(), uint64_t{2});
  CHECK_EQUAL(materialize(result.at(0, 0)), int64_t{10});
  CHECK_EQUAL(materialize(result.at(1, 0)), int64_t{30});
}

TEST("filter table slice - an all-null mask keeps nothing") {
  auto slice = make_test_slice();
  auto mask = make_nullable_mask({None{}, None{}, None{}, None{}, None{}});
  CHECK_EQUAL(filter(slice, *mask).rows(), uint64_t{0});
}

TEST("filter table slice - nested record") {
  auto b = series_builder{};
  b.record().field("a").data(int64_t{1});
  b.record().field("a").data(int64_t{2});
  b.record().field("a").data(int64_t{3});
  // Add a second field to the first record retroactively.
  auto slices = b.finish_as_table_slice("nested");
  REQUIRE_EQUAL(slices.size(), size_t{1});
  auto mask = make_mask({true, false, true});
  auto result = filter(slices[0], *mask);
  REQUIRE_EQUAL(result.rows(), uint64_t{2});
  CHECK_EQUAL(materialize(result.at(0, 0)), int64_t{1});
  CHECK_EQUAL(materialize(result.at(1, 0)), int64_t{3});
}

TEST("filter table slice - multiple fields") {
  auto b = series_builder{};
  auto r0 = b.record();
  r0.field("a").data(int64_t{1});
  r0.field("b").data("hello");
  auto r1 = b.record();
  r1.field("a").data(int64_t{2});
  r1.field("b").data("world");
  auto r2 = b.record();
  r2.field("a").data(int64_t{3});
  r2.field("b").data("!");
  auto slices = b.finish_as_table_slice("multi");
  REQUIRE_EQUAL(slices.size(), size_t{1});
  auto mask = make_mask({false, true, true});
  auto result = filter(slices[0], *mask);
  REQUIRE_EQUAL(result.rows(), uint64_t{2});
  CHECK_EQUAL(materialize(result.at(0, 0)), int64_t{2});
  CHECK_EQUAL(materialize(result.at(0, 1)), "world");
  CHECK_EQUAL(materialize(result.at(1, 0)), int64_t{3});
  CHECK_EQUAL(materialize(result.at(1, 1)), "!");
}

TEST("filter table slice - with list field") {
  auto b = series_builder{};
  // Row 0: multi-element list.
  auto r0 = b.record();
  r0.field("a").data(int64_t{1});
  auto l0 = r0.field("b").list();
  l0.data(int64_t{10});
  l0.data(int64_t{11});
  // Row 1: null list.
  auto r1 = b.record();
  r1.field("a").data(int64_t{2});
  r1.field("b").data(caf::none);
  // Row 2: empty list.
  auto r2 = b.record();
  r2.field("a").data(int64_t{3});
  r2.field("b").list();
  // Row 3: single-element list.
  auto r3 = b.record();
  r3.field("a").data(int64_t{4});
  r3.field("b").list().data(int64_t{40});
  // Row 4: multi-element list.
  auto r4 = b.record();
  r4.field("a").data(int64_t{5});
  auto l4 = r4.field("b").list();
  l4.data(int64_t{50});
  l4.data(int64_t{51});
  l4.data(int64_t{52});
  // Row 5: null list.
  auto r5 = b.record();
  r5.field("a").data(int64_t{6});
  r5.field("b").data(caf::none);
  auto slices = b.finish_as_table_slice("lists");
  REQUIRE_EQUAL(slices.size(), size_t{1});
  // Keep: multi(2), null, empty(0), drop single(1), keep multi(3), drop null.
  auto mask = make_mask({true, true, true, false, true, false});
  auto result = filter(slices[0], *mask);
  REQUIRE_EQUAL(result.rows(), uint64_t{4});
  CHECK_EQUAL(materialize(result.at(0, 0)), int64_t{1});
  CHECK_EQUAL(materialize(result.at(0, 1)), (list{int64_t{10}, int64_t{11}}));
  CHECK_EQUAL(materialize(result.at(1, 0)), int64_t{2});
  CHECK_EQUAL(materialize(result.at(1, 1)), caf::none);
  CHECK_EQUAL(materialize(result.at(2, 0)), int64_t{3});
  CHECK_EQUAL(materialize(result.at(2, 1)), (list{}));
  CHECK_EQUAL(materialize(result.at(3, 0)), int64_t{5});
  CHECK_EQUAL(materialize(result.at(3, 1)),
              (list{int64_t{50}, int64_t{51}, int64_t{52}}));
}

TEST("filter table slice - with ip field") {
  auto b = series_builder{};
  b.record().field("addr").data(ip::v4(0x01020304));
  b.record().field("addr").data(ip::v4(0x05060708));
  b.record().field("addr").data(ip::v4(0x090A0B0C));
  auto slices = b.finish_as_table_slice("ips");
  REQUIRE_EQUAL(slices.size(), size_t{1});
  auto mask = make_mask({false, true, false});
  auto result = filter(slices[0], *mask);
  REQUIRE_EQUAL(result.rows(), uint64_t{1});
  CHECK_EQUAL(materialize(result.at(0, 0)), ip::v4(0x05060708));
}

TEST("filter table slice - with nulls") {
  auto b = series_builder{};
  auto r0 = b.record();
  r0.field("a").data(int64_t{1});
  r0.field("b").data("hello");
  auto r1 = b.record();
  r1.field("a").data(caf::none);
  r1.field("b").data("world");
  auto r2 = b.record();
  r2.field("a").data(int64_t{3});
  r2.field("b").data(caf::none);
  auto slices = b.finish_as_table_slice("nulls");
  REQUIRE_EQUAL(slices.size(), size_t{1});
  auto mask = make_mask({true, true, true});
  auto result = filter(slices[0], *mask);
  REQUIRE_EQUAL(result.rows(), uint64_t{3});
  CHECK_EQUAL(materialize(result.at(0, 0)), int64_t{1});
  CHECK_EQUAL(materialize(result.at(1, 0)), caf::none);
  CHECK_EQUAL(materialize(result.at(2, 1)), caf::none);
  // Now filter keeping only rows with nulls.
  auto mask2 = make_mask({false, true, true});
  auto result2 = filter(slices[0], *mask2);
  REQUIRE_EQUAL(result2.rows(), uint64_t{2});
  CHECK_EQUAL(materialize(result2.at(0, 0)), caf::none);
  CHECK_EQUAL(materialize(result2.at(0, 1)), "world");
  CHECK_EQUAL(materialize(result2.at(1, 0)), int64_t{3});
  CHECK_EQUAL(materialize(result2.at(1, 1)), caf::none);
}

TEST("filter table slice - empty input") {
  auto b = series_builder{};
  auto slices = b.finish_as_table_slice("empty");
  CHECK(slices.empty());
}

TEST("filter table slice - single row kept") {
  auto slice = make_test_slice();
  auto mask = make_mask({false, false, true, false, false});
  auto result = filter(slice, *mask);
  REQUIRE_EQUAL(result.rows(), uint64_t{1});
  CHECK_EQUAL(materialize(result.at(0, 0)), int64_t{30});
}

TEST("filter table slice - sliced list array (non-zero arrow offset)") {
  // Build a larger slice, then subslice it to create arrays with non-zero
  // Arrow offsets. This exercises the list value_offset handling.
  auto b = series_builder{};
  // Row 0: multi-element list (will be sliced off).
  auto r0 = b.record();
  r0.field("a").data(int64_t{1});
  auto l0 = r0.field("b").list();
  l0.data(int64_t{10});
  l0.data(int64_t{11});
  // Row 1: null list.
  auto r1 = b.record();
  r1.field("a").data(int64_t{2});
  r1.field("b").data(caf::none);
  // Row 2: empty list.
  auto r2 = b.record();
  r2.field("a").data(int64_t{3});
  r2.field("b").list();
  // Row 3: single-element list.
  auto r3 = b.record();
  r3.field("a").data(int64_t{4});
  r3.field("b").list().data(int64_t{40});
  // Row 4: multi-element list.
  auto r4 = b.record();
  r4.field("a").data(int64_t{5});
  auto l4 = r4.field("b").list();
  l4.data(int64_t{50});
  l4.data(int64_t{51});
  l4.data(int64_t{52});
  // Row 5: null list (will be sliced off).
  auto r5 = b.record();
  r5.field("a").data(int64_t{6});
  r5.field("b").data(caf::none);
  auto slices = b.finish_as_table_slice("offset_test");
  REQUIRE_EQUAL(slices.size(), size_t{1});
  // Subslice rows [1, 5) so the underlying arrays have offset=1.
  auto sliced = subslice(slices[0], 1, 5);
  REQUIRE_EQUAL(sliced.rows(), uint64_t{4});
  // Verify that the underlying arrays actually have a non-zero offset.
  auto batch = to_record_batch(sliced);
  CHECK_NOT_EQUAL(batch->column(0)->offset(), 0);
  // Keep: null, drop empty, keep single(1), keep multi(3).
  auto mask = make_mask({true, false, true, true});
  auto result = filter(sliced, *mask);
  REQUIRE_EQUAL(result.rows(), uint64_t{3});
  CHECK_EQUAL(materialize(result.at(0, 0)), int64_t{2});
  CHECK_EQUAL(materialize(result.at(0, 1)), caf::none);
  CHECK_EQUAL(materialize(result.at(1, 0)), int64_t{4});
  CHECK_EQUAL(materialize(result.at(1, 1)), (list{int64_t{40}}));
  CHECK_EQUAL(materialize(result.at(2, 0)), int64_t{5});
  CHECK_EQUAL(materialize(result.at(2, 1)),
              (list{int64_t{50}, int64_t{51}, int64_t{52}}));
}

TEST("partition table slice - alternating") {
  auto slice = make_test_slice();
  auto mask = make_mask({true, false, true, false, true});
  auto [lhs, rhs] = partition(slice, *mask);
  REQUIRE_EQUAL(lhs.rows(), uint64_t{3});
  CHECK_EQUAL(materialize(lhs.at(0, 0)), int64_t{10});
  CHECK_EQUAL(materialize(lhs.at(1, 0)), int64_t{30});
  CHECK_EQUAL(materialize(lhs.at(2, 0)), int64_t{50});
  REQUIRE_EQUAL(rhs.rows(), uint64_t{2});
  CHECK_EQUAL(materialize(rhs.at(0, 0)), int64_t{20});
  CHECK_EQUAL(materialize(rhs.at(1, 0)), int64_t{40});
}

TEST("partition table slice - all true") {
  auto slice = make_test_slice();
  auto mask = make_mask({true, true, true, true, true});
  auto [lhs, rhs] = partition(slice, *mask);
  CHECK_EQUAL(lhs.rows(), uint64_t{5});
  CHECK_EQUAL(rhs.rows(), uint64_t{0});
}

TEST("partition table slice - all false") {
  auto slice = make_test_slice();
  auto mask = make_mask({false, false, false, false, false});
  auto [lhs, rhs] = partition(slice, *mask);
  CHECK_EQUAL(lhs.rows(), uint64_t{0});
  CHECK_EQUAL(rhs.rows(), uint64_t{5});
}

TEST("partition table slice - multiple fields with lists") {
  auto b = series_builder{};
  auto r0 = b.record();
  r0.field("a").data(int64_t{1});
  auto l0 = r0.field("b").list();
  l0.data(int64_t{10});
  l0.data(int64_t{11});
  auto r1 = b.record();
  r1.field("a").data(int64_t{2});
  r1.field("b").data(caf::none);
  auto r2 = b.record();
  r2.field("a").data(int64_t{3});
  r2.field("b").list();
  auto r3 = b.record();
  r3.field("a").data(int64_t{4});
  r3.field("b").list().data(int64_t{40});
  auto slices = b.finish_as_table_slice("part");
  REQUIRE_EQUAL(slices.size(), size_t{1});
  auto mask = make_mask({true, false, false, true});
  auto [lhs, rhs] = partition(slices[0], *mask);
  REQUIRE_EQUAL(lhs.rows(), uint64_t{2});
  CHECK_EQUAL(materialize(lhs.at(0, 0)), int64_t{1});
  CHECK_EQUAL(materialize(lhs.at(0, 1)), (list{int64_t{10}, int64_t{11}}));
  CHECK_EQUAL(materialize(lhs.at(1, 0)), int64_t{4});
  CHECK_EQUAL(materialize(lhs.at(1, 1)), (list{int64_t{40}}));
  REQUIRE_EQUAL(rhs.rows(), uint64_t{2});
  CHECK_EQUAL(materialize(rhs.at(0, 0)), int64_t{2});
  CHECK_EQUAL(materialize(rhs.at(0, 1)), caf::none);
  CHECK_EQUAL(materialize(rhs.at(1, 0)), int64_t{3});
  CHECK_EQUAL(materialize(rhs.at(1, 1)), (list{}));
}

TEST("partition table slice - nulls count as false") {
  auto slice = make_test_slice();
  auto mask = make_nullable_mask({true, None{}, true, None{}, false});
  auto [lhs, rhs] = partition(slice, *mask);
  REQUIRE_EQUAL(lhs.rows(), uint64_t{2});
  CHECK_EQUAL(materialize(lhs.at(0, 0)), int64_t{10});
  CHECK_EQUAL(materialize(lhs.at(1, 0)), int64_t{30});
  REQUIRE_EQUAL(rhs.rows(), uint64_t{3});
  CHECK_EQUAL(materialize(rhs.at(0, 0)), int64_t{20});
  CHECK_EQUAL(materialize(rhs.at(1, 0)), int64_t{40});
  CHECK_EQUAL(materialize(rhs.at(2, 0)), int64_t{50});
}

TEST("partition table slice - offset mask") {
  // A sliced mask starts at a non-zero bit offset, which run detection must
  // honor instead of reading from the start of the bitmap.
  auto slice = subslice(make_test_slice(), 1, 4);
  auto mask = make_mask({false, true, false, true, false})->Slice(1, 3);
  auto [lhs, rhs] = partition(slice, as<arrow::BooleanArray>(*mask));
  REQUIRE_EQUAL(lhs.rows(), uint64_t{2});
  CHECK_EQUAL(materialize(lhs.at(0, 0)), int64_t{20});
  CHECK_EQUAL(materialize(lhs.at(1, 0)), int64_t{40});
  REQUIRE_EQUAL(rhs.rows(), uint64_t{1});
  CHECK_EQUAL(materialize(rhs.at(0, 0)), int64_t{30});
}

TEST("partition table slice - offset mask with nulls") {
  auto slice = subslice(make_test_slice(), 1, 4);
  auto mask
    = make_nullable_mask({false, true, None{}, true, false})->Slice(1, 3);
  auto [lhs, rhs] = partition(slice, as<arrow::BooleanArray>(*mask));
  REQUIRE_EQUAL(lhs.rows(), uint64_t{2});
  CHECK_EQUAL(materialize(lhs.at(0, 0)), int64_t{20});
  CHECK_EQUAL(materialize(lhs.at(1, 0)), int64_t{40});
  REQUIRE_EQUAL(rhs.rows(), uint64_t{1});
  CHECK_EQUAL(materialize(rhs.at(0, 0)), int64_t{30});
}

TEST("partition table slice - long runs") {
  // Runs longer than a machine word exercise the word-wise scan.
  auto b = series_builder{};
  constexpr auto rows = int64_t{200};
  for (auto row = int64_t{0}; row < rows; ++row) {
    b.record().field("x").data(row);
  }
  auto slices = b.finish_as_table_slice("runs");
  REQUIRE_EQUAL(slices.size(), size_t{1});
  auto bits = std::vector<bool>(rows, false);
  std::fill(bits.begin() + 70, bits.begin() + 150, true);
  auto [lhs, rhs] = partition(slices[0], *make_mask(bits));
  REQUIRE_EQUAL(lhs.rows(), uint64_t{80});
  CHECK_EQUAL(materialize(lhs.at(0, 0)), int64_t{70});
  CHECK_EQUAL(materialize(lhs.at(79, 0)), int64_t{149});
  REQUIRE_EQUAL(rhs.rows(), uint64_t{120});
  CHECK_EQUAL(materialize(rhs.at(0, 0)), int64_t{0});
  CHECK_EQUAL(materialize(rhs.at(69, 0)), int64_t{69});
  CHECK_EQUAL(materialize(rhs.at(70, 0)), int64_t{150});
  CHECK_EQUAL(materialize(rhs.at(119, 0)), int64_t{199});
}

TEST("partition table slice - row count invariant") {
  auto slice = make_test_slice();
  auto mask = make_mask({true, false, true, true, false});
  auto [lhs, rhs] = partition(slice, *mask);
  CHECK_EQUAL(lhs.rows() + rhs.rows(), slice.rows());
}

TEST("partition_runs splits a clustered mask without copying") {
  auto slice = make_test_slice();
  auto runs
    = partition_runs(slice, *make_mask({true, true, false, false, false}), 4);
  REQUIRE(runs);
  REQUIRE_EQUAL(runs->size(), size_t{2});
  CHECK_EQUAL((*runs)[0].selected, true);
  REQUIRE_EQUAL((*runs)[0].slice.rows(), uint64_t{2});
  CHECK_EQUAL(materialize((*runs)[0].slice.at(0, 0)), int64_t{10});
  CHECK_EQUAL((*runs)[1].selected, false);
  REQUIRE_EQUAL((*runs)[1].slice.rows(), uint64_t{3});
  CHECK_EQUAL(materialize((*runs)[1].slice.at(0, 0)), int64_t{30});
}

TEST("partition_runs covers every row exactly once") {
  auto slice = make_test_slice();
  auto runs
    = partition_runs(slice, *make_mask({false, true, true, false, false}), 4);
  REQUIRE(runs);
  auto rows = uint64_t{0};
  auto selected = uint64_t{0};
  for (const auto& run : *runs) {
    rows += run.slice.rows();
    selected += run.selected ? run.slice.rows() : 0;
  }
  CHECK_EQUAL(rows, slice.rows());
  CHECK_EQUAL(selected, uint64_t{2});
}

TEST("partition_runs gives up on an interleaved mask") {
  auto slice = make_test_slice();
  auto mask = make_mask({true, false, true, false, true});
  CHECK(not partition_runs(slice, *mask, 4));
  // With enough room for every run, the same mask still splits.
  auto runs = partition_runs(slice, *mask, 5);
  REQUIRE(runs);
  CHECK_EQUAL(runs->size(), size_t{5});
}

TEST("partition_runs treats nulls as not selected") {
  auto slice = make_test_slice();
  auto runs = partition_runs(
    slice, *make_nullable_mask({true, true, None{}, None{}, false}), 4);
  REQUIRE(runs);
  REQUIRE_EQUAL(runs->size(), size_t{2});
  CHECK_EQUAL((*runs)[0].selected, true);
  CHECK_EQUAL((*runs)[0].slice.rows(), uint64_t{2});
  CHECK_EQUAL((*runs)[1].selected, false);
  CHECK_EQUAL((*runs)[1].slice.rows(), uint64_t{3});
}

TEST("partition_runs on a uniform mask yields a single run") {
  auto slice = make_test_slice();
  auto all_true
    = partition_runs(slice, *make_mask({true, true, true, true, true}), 4);
  REQUIRE(all_true);
  REQUIRE_EQUAL(all_true->size(), size_t{1});
  CHECK_EQUAL((*all_true)[0].selected, true);
  CHECK_EQUAL((*all_true)[0].slice.rows(), slice.rows());
  auto all_false
    = partition_runs(slice, *make_mask({false, false, false, false, false}), 4);
  REQUIRE(all_false);
  REQUIRE_EQUAL(all_false->size(), size_t{1});
  CHECK_EQUAL((*all_false)[0].selected, false);
  CHECK_EQUAL((*all_false)[0].slice.rows(), slice.rows());
}

TEST("partition_runs preserves offsets") {
  auto slice = make_test_slice();
  slice.offset(100);
  auto runs
    = partition_runs(slice, *make_mask({true, true, false, false, false}), 4);
  REQUIRE(runs);
  REQUIRE_EQUAL(runs->size(), size_t{2});
  CHECK_EQUAL((*runs)[0].slice.offset(), id{100});
  CHECK_EQUAL((*runs)[1].slice.offset(), id{102});
}

TEST("take_rows selects rows in the given order") {
  auto slice = make_test_slice();
  auto rows = std::vector<int64_t>{3, 0, 4};
  auto result = take_rows(slice, rows);
  REQUIRE_EQUAL(result.rows(), uint64_t{3});
  CHECK_EQUAL(materialize(result.at(0, 0)), int64_t{40});
  CHECK_EQUAL(materialize(result.at(1, 0)), int64_t{10});
  CHECK_EQUAL(materialize(result.at(2, 0)), int64_t{50});
}

TEST("take_rows allows repeated indices") {
  auto slice = make_test_slice();
  auto rows = std::vector<int64_t>{2, 2, 2};
  auto result = take_rows(slice, rows);
  REQUIRE_EQUAL(result.rows(), uint64_t{3});
  for (auto row = uint64_t{0}; row < result.rows(); ++row) {
    CHECK_EQUAL(materialize(result.at(row, 0)), int64_t{30});
  }
}

TEST("take_rows on no indices yields an empty slice") {
  auto slice = make_test_slice();
  auto result = take_rows(slice, {});
  CHECK_EQUAL(result.rows(), uint64_t{0});
}

TEST("take_rows preserves schema and import time") {
  auto slice = make_test_slice();
  const auto import_time = tenzir::time{std::chrono::seconds{1234567890}};
  slice.import_time(import_time);
  slice.offset(42);
  auto rows = std::vector<int64_t>{1, 3};
  auto result = take_rows(slice, rows);
  CHECK_EQUAL(result.schema(), slice.schema());
  CHECK_EQUAL(result.import_time(), import_time);
}

TEST("take_rows invalidates the offset for non-contiguous rows") {
  auto slice = make_test_slice();
  slice.offset(42);
  CHECK_EQUAL(take_rows(slice, std::vector<int64_t>{1, 3}).offset(),
              tenzir::invalid_id);
  CHECK_EQUAL(take_rows(slice, std::vector<int64_t>{3, 0, 4}).offset(),
              tenzir::invalid_id);
  CHECK_EQUAL(take_rows(slice, std::vector<int64_t>{2, 2, 2}).offset(),
              tenzir::invalid_id);
}

TEST("take_rows shifts the offset for a contiguous run") {
  auto slice = make_test_slice();
  slice.offset(42);
  CHECK_EQUAL(take_rows(slice, std::vector<int64_t>{1, 2, 3}).offset(),
              tenzir::id{43});
  auto without_offset = make_test_slice();
  CHECK_EQUAL(take_rows(without_offset, std::vector<int64_t>{1, 2}).offset(),
              tenzir::invalid_id);
}

} // namespace tenzir

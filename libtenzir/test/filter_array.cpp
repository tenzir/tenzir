//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/arrow_table_slice.hpp"
#include "tenzir/arrow_utils.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/test/test.hpp"

#include <arrow/api.h>

namespace tenzir {

namespace {

auto make_mask(std::vector<bool> bits) -> std::shared_ptr<arrow::BooleanArray> {
  auto builder = arrow::BooleanBuilder{};
  for (auto bit : bits) {
    check(builder.Append(bit));
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

} // namespace tenzir

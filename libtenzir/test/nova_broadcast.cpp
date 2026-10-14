//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/array_builder.hpp"
#include "tenzir/nova/broadcast.hpp"
#include "tenzir/nova/data_array_builder.hpp"
#include "tenzir/nova/materialize.hpp"
#include "tenzir/test/test.hpp"

#include <span>
#include <vector>

using namespace tenzir;
using namespace tenzir::nova;

namespace {

// Four rows over seven elements: row 0 owns [0, 2), row 1 is empty, element 2
// is a gap, row 2 owns [3, 5), row 3 owns [5, 6), and element 6 is padding.
// Row 3 is not selected.
auto const spans = std::vector<storage::Span>{{0, 2}, {2, 2}, {3, 5}, {5, 6}};
auto const length = storage::Index{7};

auto selected_rows() -> storage::BitMap {
  auto rows = storage::BitMap::Mutable{4};
  rows.set(0, true);
  rows.set(1, true);
  rows.set(2, true);
  return std::move(rows).finish();
}

/// The row each element belongs to, or -1 for the elements outside the
/// selected spans.
auto const parents = std::vector<int>{0, 0, -1, 2, 2, -1, -1};

auto make(std::vector<nova::Data> const& values) -> Array<nova::Data> {
  auto builder = ArrayBuilder<nova::Data>{};
  for (auto const& value : values) {
    append_row(builder, RowView<nova::Data>{value});
  }
  return builder.finish();
}

/// Broadcasts `values` with the rows `present` selects as present, and checks
/// that exactly the elements of the selected and present rows are present and
/// hold their row's value.
auto check_broadcast(std::vector<nova::Data> const& values,
                     std::vector<bool> const& present
                     = {true, true, true, true}) -> void {
  auto const rows = selected_rows();
  auto mask = storage::BitMap::Mutable{4};
  for (auto row = 0; row < 4; ++row) {
    mask.set(row, present[row]);
  }
  auto const column
    = MaskedArray<Array<nova::Data>>{make(values), std::move(mask).finish()};
  auto const result = broadcast(column, spans, rows, length);
  REQUIRE_EQUAL(result.data.length(), length);
  REQUIRE_EQUAL(result.present.length(), length);
  for (auto element = 0; element < length; ++element) {
    auto const parent = parents[element];
    auto const valid = parent >= 0 and present[parent];
    CHECK_EQUAL(result.present.get(element), valid);
    if (valid) {
      CHECK_EQUAL(materialize_legacy(result.data.get(element)),
                  materialize_legacy(values[parent]));
    }
  }
}

} // namespace

TEST("broadcast spreads fundamental values over the selected spans") {
  check_broadcast({Int{1}, Int{2}, Int{3}, Int{4}});
  check_broadcast({Float{1.5}, Float{2.5}, Float{3.5}, Float{4.5}});
  check_broadcast({String{"a"}, String{"bb"}, String{"ccc"}, String{"d"}});
  check_broadcast({Null{}, Null{}, Null{}, Null{}});
}

TEST("broadcast spreads structured and mixed values") {
  check_broadcast({nova::List{Int{1}, Int{2}}, nova::List{}, nova::List{Null{}},
                   nova::List{String{"x"}}});
  check_broadcast({nova::Record{{"a", Int{1}}, {"b", String{"x"}}},
                   nova::Record{{"a", Int{2}}},
                   nova::Record{{"b", String{"y"}}, {"a", Int{3}}},
                   nova::Record{}});
  check_broadcast(
    {Int{1}, String{"a"}, nova::Record{{"a", Null{}}}, nova::List{Int{4}}});
  check_broadcast({Null{}, Int{2}, String{"c"}, Null{}});
}

TEST("broadcast leaves the elements of rows that are not present invalid") {
  check_broadcast({Int{1}, Int{2}, Int{3}, Int{4}}, {false, true, true, true});
  check_broadcast({String{"a"}, String{"b"}, String{"c"}, String{"d"}},
                  {true, true, false, true});
  check_broadcast({nova::Record{{"a", Int{1}}}, nova::Record{{"a", Int{2}}},
                   nova::Record{{"a", Int{3}}}, nova::Record{}},
                  {true, false, false, true});
  check_broadcast({Int{1}, String{"a"}, String{"c"}, nova::List{Int{4}}},
                  {false, true, true, true});
  // Rows that are not present are never read: the span of an unset string row
  // is invalid and must not end up in the result.
  auto strings = storage::DenseStringOffsetStorage::Mutable{3};
  REQUIRE(strings.set(0, "a"));
  REQUIRE(strings.set(2, "ccc"));
  auto present = storage::BitMap::Mutable{3};
  present.set(0, true);
  present.set(2, true);
  auto const column = MaskedArray<Array<nova::Data>>{
    Array<String>{std::move(strings).finish()}, std::move(present).finish()};
  auto const runs = std::vector<storage::Span>{{0, 2}, {2, 3}, {3, 5}};
  auto const result = broadcast(column, runs, storage::BitMap{3, true}, 5);
  auto const expanded = result.data.try_as<String>();
  REQUIRE(expanded);
  auto const& storage
    = as<storage::DenseStringOffsetStorage>(expanded->storage());
  CHECK(not result.present.get(2));
  CHECK_EQUAL(storage.span(2).begin, 0);
  CHECK_EQUAL(storage.span(2).end, 0);
  CHECK_EQUAL(*expanded->get(1), "a");
  CHECK_EQUAL(*expanded->get(4), "ccc");
}

TEST("broadcast keeps the payload of strings and lists") {
  auto const rows = selected_rows();
  auto const strings
    = make({String{"a"}, String{"bb"}, String{"ccc"}, String{"d"}});
  auto const source = strings.try_as<String>();
  REQUIRE(source);
  auto const result = broadcast(*source, spans, rows, length);
  auto const& before
    = as<storage::DenseStringOffsetStorage>(source->storage()).data();
  auto const& after = as<storage::DenseStringOffsetStorage>(result.storage());
  CHECK(after.data().begin() == before.begin());
  CHECK_EQUAL(*result.get(3), "ccc");
  auto const lists
    = make({nova::List{String{"a"}}, nova::List{},
            nova::List{String{"b"}, String{"c"}}, nova::List{String{"d"}}})
        .try_as<nova::List>();
  REQUIRE(lists);
  auto const expanded = broadcast(*lists, spans, rows, length);
  auto const values
    = as<storage::ListStorage>(lists->storage()).values().try_as<String>();
  auto const expanded_values
    = as<storage::ListStorage>(expanded.storage()).values().try_as<String>();
  REQUIRE(values);
  REQUIRE(expanded_values);
  CHECK(
    as<storage::DenseStringOffsetStorage>(expanded_values->storage())
      .data()
      .begin()
    == as<storage::DenseStringOffsetStorage>(values->storage()).data().begin());
}

TEST("broadcast keeps constants constant") {
  auto const rows = selected_rows();
  auto const column = Array<Int>{storage::ConstantStorage<Int>{4, 42}};
  auto const result = broadcast(column, spans, rows, length);
  REQUIRE(is<storage::ConstantStorage<Int>>(result.storage()));
  CHECK_EQUAL(result.length(), length);
  CHECK_EQUAL(*result.get(6), 42);
}

TEST("broadcast writes bitmap runs across word boundaries") {
  auto bits = storage::BitMap::Mutable{3};
  bits.set(0, true);
  bits.set(2, true);
  auto const runs
    = std::vector<storage::Span>{{0, 200}, {200, 400}, {400, 600}};
  auto const all = storage::BitMap{3, true};
  auto const bools
    = broadcast(Array<Bool>{std::move(bits).finish()}, runs, all, 601);
  auto const& result = as<storage::BitMap>(bools.storage());
  CHECK_EQUAL(result.length(), 601);
  CHECK_EQUAL(result.true_count(), 400);
  CHECK(result.get(199));
  CHECK(not result.get(200));
  CHECK(not result.get(399));
  CHECK(result.get(400));
  CHECK(not result.get(600));
  auto middle = storage::BitMap::Mutable{3};
  middle.set(1, true);
  auto const mask = broadcast_mask(runs, std::move(middle).finish(), 601);
  CHECK_EQUAL(mask.true_count(), 200);
  CHECK(not mask.get(199));
  CHECK(mask.get(200));
  CHECK(mask.get(399));
  CHECK(not mask.get(400));
}

TEST("broadcast handles empty results") {
  auto const column = MaskedArray<Array<nova::Data>>{
    make({String{"a"}, String{"b"}}), storage::BitMap{2, true}};
  auto const empty = std::vector<storage::Span>{{0, 0}, {0, 0}};
  auto const result
    = broadcast(column, empty, storage::BitMap{2, true}, storage::Index{0});
  CHECK_EQUAL(result.data.length(), 0);
  CHECK_EQUAL(result.present.length(), 0);
  auto const unselected
    = broadcast(column, std::vector<storage::Span>{{0, 1}, {1, 2}},
                storage::BitMap{2, false}, 2);
  CHECK_EQUAL(unselected.data.length(), 2);
  CHECK(not unselected.present.any());
}

TEST("broadcast accepts spans in any order") {
  auto const reversed = std::vector<storage::Span>{{4, 6}, {2, 4}, {0, 2}};
  auto const column = MaskedArray<Array<nova::Data>>{
    make({String{"a"}, String{"b"}, String{"c"}}), storage::BitMap{3, true}};
  auto const result
    = broadcast(column, reversed, storage::BitMap{3, true}, storage::Index{6});
  auto const expected = std::vector<std::string>{"c", "c", "b", "b", "a", "a"};
  for (auto element = 0; element < 6; ++element) {
    CHECK(result.present.get(element));
    CHECK_EQUAL(materialize_legacy(result.data.get(element)),
                tenzir::data{expected[element]});
  }
  // Repeated spans select their elements once.
  auto const repeated = std::vector<storage::Span>{{1, 3}, {0, 2}, {1, 3}};
  auto const mask = broadcast_mask(repeated, storage::BitMap{3, true}, 4);
  CHECK_EQUAL(mask.true_count(), 3);
  CHECK(not mask.get(3));
}

TEST("concatenating ranges repeats overlapping rows") {
  auto const values = std::vector<nova::Data>{
    String{"a"}, Int{1}, nova::Record{{"x", String{"b"}}}, nova::List{Int{2}}};
  auto const column = make(values);
  auto const ranges
    = std::vector<storage::Span>{{1, 3}, {0, 2}, {2, 2}, {1, 4}};
  auto const result = concatenate_ranges(column, ranges);
  auto const expected = std::vector<int>{1, 2, 0, 1, 1, 2, 3};
  REQUIRE_EQUAL(result.length(), static_cast<storage::Index>(expected.size()));
  for (auto element = 0; element < result.length(); ++element) {
    CHECK_EQUAL(materialize_legacy(result.get(element)),
                materialize_legacy(values[expected[element]]));
  }
  auto bits = storage::BitMap::Mutable{4};
  bits.set(1, true);
  bits.set(3, true);
  auto const copied = concatenate_ranges(std::move(bits).finish(), ranges);
  REQUIRE_EQUAL(copied.length(), 7);
  for (auto element = 0; element < copied.length(); ++element) {
    CHECK_EQUAL(copied.get(element), expected[element] % 2 == 1);
  }
  // Strings keep their buffer.
  auto const strings = make({String{"a"}, String{"bb"}}).try_as<String>();
  REQUIRE(strings);
  auto const repeated = concatenate_ranges(
    Array<nova::Data>{*strings}, std::vector<storage::Span>{{0, 2}, {0, 2}});
  auto const repeated_strings = repeated.try_as<String>();
  REQUIRE(repeated_strings);
  CHECK(
    as<storage::DenseStringOffsetStorage>(repeated_strings->storage())
      .data()
      .begin()
    == as<storage::DenseStringOffsetStorage>(strings->storage()).data().begin());
  CHECK_EQUAL(*repeated_strings->get(3), "bb");
}

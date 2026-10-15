//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/array_builder.hpp"
#include "tenzir/nova/arrow_export.hpp"
#include "tenzir/nova/censor.hpp"
#include "tenzir/nova/schema_discovery.hpp"
#include "tenzir/series.hpp"
#include "tenzir/test/test.hpp"

#include <arrow/record_batch.h>

#include <algorithm>
#include <chrono>
#include <string>
#include <utility>
#include <vector>

using namespace tenzir::nova;

namespace {

auto make_events(std::vector<Record> const& rows) -> Events {
  auto builder = ArrayBuilder<Record>{};
  for (auto const& value : rows) {
    auto row = builder.record();
    for (auto const& [name, field] : value) {
      append_data(row.field(name), field);
    }
  }
  auto length = static_cast<storage::Index>(rows.size());
  return Events{builder.finish(), storage::BitMap{length, true},
                Events::Meta::make_empty(length, "events")};
}

/// Gives every row its own import time, which splits the row-wise conversion
/// into one slice per row.
auto with_distinct_import_times(Events events) -> Events {
  auto times = ArrayBuilder<Time>{};
  for (auto i = storage::Index{0}; i < events.length(); ++i) {
    times.data(tenzir::time{std::chrono::seconds{i}});
  }
  events.meta.import_time = times.finish();
  return events;
}

/// Removes null record fields and orders the remaining fields by name. The
/// row-wise conversion unifies the record fields of adjacent rows, while the
/// schema groups keep each row's own fields and field order.
auto normalize(tenzir::data const& value) -> tenzir::data {
  if (auto const* record = tenzir::try_as<tenzir::record>(&value)) {
    auto fields = std::vector<std::pair<std::string, tenzir::data>>{};
    for (auto const& [name, field] : *record) {
      if (not tenzir::is<caf::none_t>(field)) {
        fields.emplace_back(name, normalize(field));
      }
    }
    std::ranges::sort(fields, {}, &std::pair<std::string, tenzir::data>::first);
    auto result = tenzir::record{};
    for (auto& [name, field] : fields) {
      result.emplace(std::move(name), std::move(field));
    }
    return result;
  }
  if (auto const* list = tenzir::try_as<tenzir::list>(&value)) {
    auto result = tenzir::list{};
    for (auto const& element : *list) {
      result.push_back(normalize(element));
    }
    return result;
  }
  return value;
}

auto slice_values(tenzir::table_slice const& slice)
  -> std::vector<tenzir::data> {
  auto result = std::vector<tenzir::data>{};
  CHECK(to_record_batch(slice)->ValidateFull().ok());
  auto series = tenzir::series{slice};
  for (auto i = int64_t{0}; i < series.length(); ++i) {
    result.push_back(normalize(tenzir::materialize(series.at(i))));
  }
  return result;
}

/// Exports `events` through the schema groups and returns the values of the
/// active rows in their original order, as well as the number of groups.
auto export_grouped(Events const& events, SchemaGroupingOptions options)
  -> std::pair<std::vector<tenzir::data>, size_t> {
  auto groups = group_by_schema(events, options);
  REQUIRE(groups);
  auto values = std::vector<tenzir::data>(events.length());
  auto seen = std::vector<bool>(events.length(), false);
  for (auto const& group : groups.unwrap()) {
    REQUIRE(tenzir::is<tenzir::record_type>(group.schema));
    CHECK(group.schema.name().empty());
    CHECK(std::ranges::is_sorted(group.rows));
    auto batch = to_arrow_record_batch(events.data, group.schema, group.rows);
    REQUIRE(batch);
    auto output = slice_values(tenzir::table_slice{batch.unwrap()});
    REQUIRE_EQUAL(output.size(), group.rows.size());
    for (auto i = size_t{0}; i < group.rows.size(); ++i) {
      auto row = group.rows[i];
      CHECK(events.mask.get(row));
      CHECK(not seen[row]);
      seen[row] = true;
      values[row] = std::move(output[i]);
    }
  }
  auto result = std::vector<tenzir::data>{};
  for (auto row = storage::Index{0}; row < events.length(); ++row) {
    CHECK_EQUAL(seen[row], events.mask.get(row));
    if (seen[row]) {
      result.push_back(std::move(values[row]));
    }
  }
  return {std::move(result), groups.unwrap().size()};
}

/// Checks that the grouped export yields the same values as the row-wise
/// conversion through `to_table_slices`, both with exact grouping and with
/// null refinement, and returns the number of groups of each.
auto check_equivalent(Events const& events) -> std::pair<size_t, size_t> {
  auto expected = std::vector<tenzir::data>{};
  for (auto const& slice : to_table_slices(events)) {
    auto values = slice_values(slice);
    expected.insert(expected.end(), values.begin(), values.end());
  }
  auto [exact, exact_groups] = export_grouped(events, {});
  CHECK_EQUAL(exact, expected);
  auto [refined, refined_groups]
    = export_grouped(events, {.refine_nulls = true});
  CHECK_EQUAL(refined, expected);
  CHECK_LESS_EQUAL(refined_groups, exact_groups);
  return {exact_groups, refined_groups};
}

} // namespace

TEST("schema groups match row-wise conversion for homogeneous events") {
  auto rows = std::vector<Record>{};
  for (auto i = 0; i < 16; ++i) {
    rows.push_back(Record{
      {"time", Time{std::chrono::seconds{i}}},
      {"id", Int{i}},
      {"message", String{"event " + std::to_string(i)}},
      {"ok", Bool{i % 2 == 0}},
      {"score", Float{i * 0.5}},
    });
  }
  auto events = make_events(rows);
  CHECK_EQUAL(check_equivalent(events), (std::pair<size_t, size_t>{1, 1}));
  CHECK_EQUAL(check_equivalent(with_distinct_import_times(events)),
              (std::pair<size_t, size_t>{1, 1}));
}

TEST("schema groups match row-wise conversion for interleaved schemas") {
  auto rows = std::vector<Record>{};
  for (auto i = 0; i < 12; ++i) {
    switch (i % 3) {
      case 0:
        rows.push_back(Record{{"class", Int{1}}, {"src", String{"a"}}});
        break;
      case 1:
        rows.push_back(Record{{"class", Int{2}}, {"dst", UInt{7}}});
        break;
      default:
        rows.push_back(Record{{"dst", UInt{8}}, {"class", Int{3}}});
        break;
    }
  }
  auto events = with_distinct_import_times(make_events(rows));
  // The row-wise conversion produces one slice per row here.
  CHECK_EQUAL(to_table_slices(events).size(), rows.size());
  CHECK_EQUAL(check_equivalent(events), (std::pair<size_t, size_t>{3, 3}));
}

TEST("schema groups match row-wise conversion for null-only columns") {
  auto events = make_events({
    Record{{"x", Null{}}, {"y", Int{1}}},
    Record{{"x", Null{}}, {"y", Null{}}},
    Record{{"x", Int{2}}, {"y", Null{}}},
    Record{{"x", Null{}}, {"y", Int{3}}},
    Record{{"x", Null{}}},
  });
  CHECK_EQUAL(check_equivalent(events), (std::pair<size_t, size_t>{4, 2}));
}

TEST("schema groups match row-wise conversion for nested records") {
  auto events = make_events({
    Record{{"src", Record{{"ip", tenzir::ip::v4(uint32_t{0x0a000001})},
                          {"port", UInt{443}}}},
           {"meta", Record{{"tags", Record{{"a", String{"x"}}}}}}},
    Record{{"src", Record{{"ip", tenzir::ip::v4(uint32_t{0x0a000002})}}},
           {"meta", Null{}}},
    Record{{"src", Null{}}, {"meta", Record{{"tags", Record{}}}}},
    Record{{"src", Record{{"port", UInt{80}}, {"ip", Null{}}}}},
  });
  CHECK_EQUAL(check_equivalent(events), (std::pair<size_t, size_t>{4, 3}));
}

TEST("schema groups match row-wise conversion for lists") {
  auto events = make_events({
    Record{{"xs", List{Int{1}, Int{2}}}},
    Record{{"xs", List{}}},
    Record{{"xs", List{Null{}, Int{3}}}},
    Record{{"xs", Null{}}},
    Record{{"xs", List{List{String{"a"}}, List{}}}},
    Record{{"obs", List{Record{{"typ", String{"ip"}}, {"val", String{"1"}}},
                        Record{{"typ", Null{}}, {"val", String{"2"}}}}}},
    Record{{"obs", List{Record{{"typ", Null{}}, {"val", Null{}}}}}},
  });
  check_equivalent(events);
}

TEST("schema groups match row-wise conversion for mixed types") {
  auto events = make_events({
    Record{{"x", Int{1}}, {"d", Duration{std::chrono::seconds{1}}}},
    Record{{"x", String{"two"}}, {"d", Duration{std::chrono::seconds{2}}}},
    Record{{"x", Float{3.0}}, {"b", Blob{std::byte{1}}}},
    Record{{"x", Int{4}}, {"d", Duration{std::chrono::seconds{4}}}},
    Record{{"x", Record{{"y", Bool{false}}}}},
    Record{{"x", String{"six"}}, {"d", Duration{std::chrono::seconds{6}}}},
  });
  CHECK_EQUAL(check_equivalent(events), (std::pair<size_t, size_t>{4, 4}));
  // Inactive rows neither take part in grouping nor appear in the output.
  auto mask = storage::BitMap::Mutable{events.length()};
  mask.set(1, true);
  mask.set(3, true);
  mask.set(5, true);
  events.mask = std::move(mask).finish();
  CHECK_EQUAL(check_equivalent(events), (std::pair<size_t, size_t>{2, 2}));
}

TEST("schema groups ignore event metadata") {
  auto events = make_events({
    Record{{"x", Int{1}}},
    Record{{"x", Int{2}}},
    Record{{"x", Int{3}}},
  });
  auto names = ArrayBuilder<String>{};
  names.data("one");
  names.data("two");
  names.data("one");
  events.meta.name = names.finish();
  auto internal = ArrayBuilder<Bool>{};
  internal.data(false);
  internal.data(true);
  internal.data(false);
  events.meta.internal = internal.finish();
  events = with_distinct_import_times(std::move(events));
  CHECK_EQUAL(to_table_slices(events).size(), 3u);
  auto groups = group_by_schema(events);
  REQUIRE(groups);
  REQUIRE_EQUAL(groups.unwrap().size(), 1u);
  CHECK_EQUAL(groups.unwrap()[0].rows, (std::vector<storage::Index>{0, 1, 2}));
}

TEST("schema groups follow the order of their first rows") {
  // The groups' order determines the order in which a sink evolves its
  // schema, so it must follow the arrival order of the events.
  auto events = make_events({
    Record{{"b", Int{1}}},
    Record{{"a", Int{2}}},
    Record{{"b", Int{3}}},
    Record{{"a", String{"x"}}},
    Record{{"a", Int{4}}},
    Record{{"b", Null{}}},
  });
  auto groups = group_by_schema(events);
  REQUIRE(groups);
  auto const& result = groups.unwrap();
  REQUIRE_EQUAL(result.size(), 4u);
  auto field = [](SchemaGroup const& group) {
    return tenzir::as<tenzir::record_type>(group.schema).field(0);
  };
  CHECK_EQUAL(field(result[0]).name, "b");
  CHECK_EQUAL(field(result[0]).type, tenzir::type{tenzir::int64_type{}});
  CHECK_EQUAL(result[0].rows, (std::vector<storage::Index>{0, 2}));
  CHECK_EQUAL(field(result[1]).name, "a");
  CHECK_EQUAL(field(result[1]).type, tenzir::type{tenzir::int64_type{}});
  CHECK_EQUAL(result[1].rows, (std::vector<storage::Index>{1, 4}));
  CHECK_EQUAL(field(result[2]).type, tenzir::type{tenzir::string_type{}});
  CHECK_EQUAL(result[2].rows, (std::vector<storage::Index>{3}));
  CHECK_EQUAL(field(result[3]).type, tenzir::type{tenzir::null_type{}});
  CHECK_EQUAL(result[3].rows, (std::vector<storage::Index>{5}));
}

TEST("schema groups merge null-compatible types only on request") {
  auto events = make_events({
    Record{{"x", Null{}}, {"y", Int{1}}},
    Record{{"x", String{"a"}}, {"y", Null{}}},
    Record{{"x", Int{2}}, {"y", Int{3}}},
    Record{{"x", Null{}}, {"y", Null{}}},
  });
  auto exact = group_by_schema(events);
  REQUIRE(exact);
  CHECK_EQUAL(exact.unwrap().size(), 4u);
  auto refined = group_by_schema(events, {.refine_nulls = true});
  REQUIRE(refined);
  auto const& groups = refined.unwrap();
  REQUIRE_EQUAL(groups.size(), 2u);
  CHECK_EQUAL(groups[0].schema, (tenzir::type{tenzir::record_type{
                                  {"x", tenzir::string_type{}},
                                  {"y", tenzir::int64_type{}},
                                }}));
  CHECK_EQUAL(groups[0].rows, (std::vector<storage::Index>{0, 1, 3}));
  CHECK_EQUAL(groups[1].rows, (std::vector<storage::Index>{2}));
  for (auto const& group : groups) {
    CHECK(to_arrow_record_batch(events.data, group.schema, group.rows));
  }
}

TEST("schema groups reject secrets and heterogeneous lists") {
  auto secrets = make_events({Record{{"s", Secret{}}}});
  CHECK(not group_by_schema(secrets));
  auto [data, censored] = censor_secrets(Array<Data>{std::move(secrets.data)});
  CHECK(censored);
  secrets.data = std::move(*data.try_as<Record>());
  auto groups = group_by_schema(secrets);
  REQUIRE(groups);
  REQUIRE_EQUAL(groups.unwrap().size(), 1u);
  auto batch = to_arrow_record_batch(secrets.data, groups.unwrap()[0].schema,
                                     groups.unwrap()[0].rows);
  REQUIRE(batch);
  CHECK_EQUAL(slice_values(tenzir::table_slice{batch.unwrap()}),
              (std::vector<tenzir::data>{tenzir::record{{"s", "***"}}}));
  CHECK(not group_by_schema(make_events({
    Record{{"xs", List{Int{1}, String{"two"}}}},
  })));
}

TEST("schema groups are empty without active rows") {
  auto events = make_events({Record{{"x", Int{1}}}});
  events.mask = storage::BitMap{1, false};
  auto groups = group_by_schema(events);
  REQUIRE(groups);
  CHECK(groups.unwrap().empty());
}

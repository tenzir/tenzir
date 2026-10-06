//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "clickhouse/block_builder.hpp"

#include "clickhouse/table_registry.hpp"

#include <tenzir/data.hpp>
#include <tenzir/nova/data_array_builder.hpp>
#include <tenzir/test/test.hpp>

#include <clickhouse/columns/array.h>
#include <clickhouse/columns/json.h>
#include <clickhouse/columns/nullable.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/tuple.h>

using namespace tenzir;
using namespace tenzir::plugins::clickhouse;

namespace {

using nova::storage::BitMap;
using nova::storage::Index;

auto make_schema(std::vector<ColumnDescription> description) -> TableSchema {
  auto dh = collecting_diagnostic_handler{};
  auto schema = build_table_schema(std::move(description), dh);
  REQUIRE(schema);
  return std::move(schema).unwrap();
}

auto column(std::string name, std::string type, std::string default_kind = "",
            std::string comment = "") -> ColumnDescription {
  return {std::move(name), std::move(type), std::move(default_kind), "",
          std::move(comment)};
}

/// Builds a batch of `rows`, all of them active unless `mask` says otherwise.
auto make_events(std::vector<data> const& rows, Option<std::vector<bool>> mask
                                                = None{}) -> nova::Events {
  auto dh = collecting_diagnostic_handler{};
  auto builder = nova::ArrayBuilder<nova::Data>{};
  for (auto const& row : rows) {
    nova::append_legacy_data(builder, row, dh);
  }
  auto records = builder.finish().try_as<nova::Record>();
  REQUIRE(records);
  auto const length = records->length();
  auto active = BitMap{length, true};
  if (mask) {
    auto bits = BitMap::Builder{};
    for (auto bit : *mask) {
      bits.emplace_back(bit);
    }
    active = bits.finish();
  }
  return nova::Events{std::move(*records), std::move(active),
                      nova::Events::Meta::make_empty(length)};
}

auto bits(BitMap const& mask) -> std::vector<bool> {
  auto result = std::vector<bool>{};
  for (auto i = Index{0}; i < mask.length(); ++i) {
    result.push_back(mask.get(i));
  }
  return result;
}

auto build(TableSchema const& schema, std::vector<nova::Events> const& events,
           collecting_diagnostic_handler& dh) -> std::vector<PreparedInsert> {
  return build_inserts(schema.root, events, "t", dh);
}

auto get(::clickhouse::Block const& block, std::string_view name)
  -> ::clickhouse::ColumnRef {
  for (auto i = size_t{0}; i < block.GetColumnCount(); ++i) {
    if (block.GetColumnName(i) == name) {
      return block[i];
    }
  }
  FAIL("missing column");
}

template <class T>
auto values(::clickhouse::ColumnRef const& column) {
  auto typed = column->As<::clickhouse::ColumnVector<T>>();
  REQUIRE(typed);
  return std::vector<T>(typed->GetWritableData());
}

/// Renders a nullable string column, with `NULL` for nulls.
auto strings(::clickhouse::ColumnRef const& column)
  -> std::vector<std::string> {
  auto result = std::vector<std::string>{};
  auto nullable = column->As<::clickhouse::ColumnNullable>();
  auto nested = nullable ? nullable->Nested() : column;
  auto typed = nested->As<::clickhouse::ColumnString>();
  REQUIRE(typed);
  for (auto i = size_t{0}; i < typed->Size(); ++i) {
    if (nullable and nullable->IsNull(i)) {
      result.emplace_back("NULL");
    } else {
      result.emplace_back(typed->At(i));
    }
  }
  return result;
}

auto json(::clickhouse::ColumnRef const& column) -> std::vector<std::string> {
  auto typed = column->As<::clickhouse::ColumnJSON>();
  REQUIRE(typed);
  auto result = std::vector<std::string>{};
  for (auto i = size_t{0}; i < typed->Size(); ++i) {
    result.emplace_back(typed->At(i));
  }
  return result;
}

auto warned(collecting_diagnostic_handler const& dh, std::string_view text)
  -> bool {
  auto copy = dh;
  for (auto const& diag : std::move(copy).collect()) {
    if (diag.message.find(text) != std::string::npos) {
      return true;
    }
  }
  return false;
}

} // namespace

TEST("rows of different shapes and types go into one insert") {
  auto schema = make_schema(
    {column("x", "Nullable(Int64)"), column("y", "Nullable(String)")});
  auto events = std::vector{make_events({
    record{{"x", int64_t{1}}},
    record{{"x", "a"}},
    record{{"y", "b"}},
  })};
  auto dh = collecting_diagnostic_handler{};
  auto inserts = build(schema, events, dh);
  REQUIRE_EQUAL(inserts.size(), size_t{1});
  CHECK_EQUAL(inserts[0].rows, uint64_t{2});
  auto x = get(inserts[0].block, "x")->As<::clickhouse::ColumnNullable>();
  REQUIRE(x);
  CHECK(not x->IsNull(0));
  CHECK(x->IsNull(1));
  CHECK_EQUAL(values<int64_t>(x->Nested())[0], int64_t{1});
  CHECK_EQUAL(strings(get(inserts[0].block, "y")),
              (std::vector<std::string>{"NULL", "b"}));
  CHECK(warned(dh, "incompatible type for column `x`"));
}

TEST("masked rows of several batches form one block") {
  auto schema = make_schema({column("x", "Int64")});
  auto events = std::vector{
    make_events({record{{"x", int64_t{1}}}, record{{"x", int64_t{2}}}},
                std::vector{false, true}),
    make_events({record{{"x", int64_t{3}}}, record{{"x", int64_t{4}}}},
                std::vector{true, true}),
  };
  auto dh = collecting_diagnostic_handler{};
  auto inserts = build(schema, events, dh);
  REQUIRE_EQUAL(inserts.size(), size_t{1});
  CHECK_EQUAL(values<int64_t>(get(inserts[0].block, "x")),
              (std::vector<int64_t>{2, 3, 4}));
  CHECK_EQUAL(bits(inserts[0].input_rows[0]), (std::vector<bool>{false, true}));
  CHECK_EQUAL(bits(inserts[0].input_rows[1]), (std::vector<bool>{true, true}));
}

TEST("a rejection in a later column drops the row from earlier columns") {
  auto schema = make_schema(
    {column("a", "UInt16"), column("b", "String"), column("c", "UInt16")});
  auto events = std::vector{make_events({
    record{{"a", uint64_t{1}}, {"b", "x"}, {"c", uint64_t{1}}},
    record{{"a", uint64_t{70000}}, {"b", "y"}, {"c", uint64_t{2}}},
    record{{"a", uint64_t{3}}, {"b", "z"}, {"c", uint64_t{70000}}},
    record{{"a", uint64_t{4}}, {"b", "w"}, {"c", uint64_t{4}}},
  })};
  auto dh = collecting_diagnostic_handler{};
  auto inserts = build(schema, events, dh);
  REQUIRE_EQUAL(inserts.size(), size_t{1});
  auto const& block = inserts[0].block;
  CHECK_EQUAL(inserts[0].rows, uint64_t{2});
  CHECK_EQUAL(values<uint16_t>(get(block, "a")), (std::vector<uint16_t>{1, 4}));
  CHECK_EQUAL(strings(get(block, "b")), (std::vector<std::string>{"x", "w"}));
  CHECK_EQUAL(values<uint16_t>(get(block, "c")), (std::vector<uint16_t>{1, 4}));
  CHECK(warned(dh, "value out of range for ClickHouse column `a`"));
  CHECK(warned(dh, "value out of range for ClickHouse column `c`"));
}

TEST("a failing list element drops its row and undoes its elements") {
  auto schema
    = make_schema({column("id", "UInt64"), column("xs", "Array(UInt16)")});
  auto events = std::vector{make_events({
    record{{"id", uint64_t{1}}, {"xs", list{uint64_t{1}, uint64_t{70000}}}},
    record{{"id", uint64_t{2}}, {"xs", list{uint64_t{2}, uint64_t{3}}}},
  })};
  auto dh = collecting_diagnostic_handler{};
  auto inserts = build(schema, events, dh);
  REQUIRE_EQUAL(inserts.size(), size_t{1});
  auto const& block = inserts[0].block;
  CHECK_EQUAL(values<uint64_t>(get(block, "id")), (std::vector<uint64_t>{2}));
  auto xs = get(block, "xs")->As<::clickhouse::ColumnArray>();
  REQUIRE(xs);
  REQUIRE_EQUAL(xs->Size(), size_t{1});
  CHECK_EQUAL(values<uint16_t>(xs->GetAsColumn(0)),
              (std::vector<uint16_t>{2, 3}));
}

TEST("lists of records and lists are written row by row") {
  auto schema = make_schema({
    column("id", "UInt64"),
    column("xs", "Array(Tuple(a Nullable(Int64), b Array(Nullable(String))))"),
  });
  auto events = std::vector{make_events({
    record{{"id", uint64_t{1}},
           {"xs", list{record{{"a", int64_t{1}}, {"b", list{"x", data{}}}},
                       record{{"b", list{}}}}}},
    record{{"id", uint64_t{2}}, {"xs", list{record{{"a", "bad"}}}}},
    record{{"id", uint64_t{3}}, {"xs", list{record{{"a", int64_t{3}}}}}},
  })};
  auto dh = collecting_diagnostic_handler{};
  auto inserts = build(schema, events, dh);
  REQUIRE_EQUAL(inserts.size(), size_t{1});
  auto const& block = inserts[0].block;
  CHECK_EQUAL(values<uint64_t>(get(block, "id")),
              (std::vector<uint64_t>{1, 3}));
  auto xs = get(block, "xs")->As<::clickhouse::ColumnArray>();
  REQUIRE(xs);
  REQUIRE_EQUAL(xs->Size(), size_t{2});
  auto first = xs->GetAsColumn(0)->As<::clickhouse::ColumnTuple>();
  REQUIRE(first);
  REQUIRE_EQUAL(first->Size(), size_t{2});
  auto bs = (*first)[1]->As<::clickhouse::ColumnArray>();
  REQUIRE(bs);
  CHECK_EQUAL(strings(bs->GetAsColumn(0)),
              (std::vector<std::string>{"x", "NULL"}));
  CHECK_EQUAL(bs->GetAsColumn(1)->Size(), size_t{0});
  auto second = xs->GetAsColumn(1)->As<::clickhouse::ColumnTuple>();
  REQUIRE(second);
  CHECK_EQUAL(second->Size(), size_t{1});
  CHECK(warned(dh, "incompatible type for column `xs.[].a`"));
}

TEST("constant lists are written for every row") {
  auto schema = make_schema({column("xs", "Array(Int64)")});
  auto list_value = nova::List{};
  list_value.emplace_back(int64_t{7});
  list_value.emplace_back(int64_t{8});
  auto lists = nova::Array<nova::Data>{nova::Array<nova::List>{
    nova::storage::ConstantStorage<nova::List, nova::RowView<nova::List>>{
      3, std::move(list_value)}}};
  auto fields = std::vector<
    std::pair<std::string_view, nova::MaskedArray<nova::Array<nova::Data>>>>{
    {"xs", {std::move(lists), BitMap{3, true}}},
  };
  auto records = nova::Array<nova::Record>::from_fields(fields);
  auto events = std::vector{nova::Events{std::move(records), BitMap{3, true},
                                         nova::Events::Meta::make_empty(3)}};
  auto dh = collecting_diagnostic_handler{};
  auto inserts = build(schema, events, dh);
  REQUIRE_EQUAL(inserts.size(), size_t{1});
  auto xs = get(inserts[0].block, "xs")->As<::clickhouse::ColumnArray>();
  REQUIRE(xs);
  REQUIRE_EQUAL(xs->Size(), size_t{3});
  for (auto i = size_t{0}; i < 3; ++i) {
    CHECK_EQUAL(values<int64_t>(xs->GetAsColumn(i)),
                (std::vector<int64_t>{7, 8}));
  }
}

TEST("rows that omit a column with a default go into their own insert") {
  auto schema
    = make_schema({column("id", "UInt64"), column("d", "UInt64", "DEFAULT")});
  auto events = std::vector{make_events({
    record{{"id", uint64_t{1}}, {"d", uint64_t{5}}},
    record{{"id", uint64_t{2}}},
    record{{"id", uint64_t{3}}, {"d", uint64_t{6}}},
  })};
  auto dh = collecting_diagnostic_handler{};
  auto inserts = build(schema, events, dh);
  REQUIRE_EQUAL(inserts.size(), size_t{2});
  CHECK_EQUAL(inserts[0].block.GetColumnCount(), size_t{2});
  CHECK_EQUAL(values<uint64_t>(get(inserts[0].block, "d")),
              (std::vector<uint64_t>{5, 6}));
  CHECK_EQUAL(inserts[1].block.GetColumnCount(), size_t{1});
  CHECK_EQUAL(values<uint64_t>(get(inserts[1].block, "id")),
              (std::vector<uint64_t>{2}));
}

TEST("a null for a non-nullable column with a default omits the column") {
  auto schema = make_schema({
    column("id", "UInt64"),
    column("d", "UInt64", "DEFAULT"),
    column("s", "String", "DEFAULT"),
  });
  auto events = std::vector{make_events({
    record{{"id", uint64_t{1}}, {"d", uint64_t{5}}, {"s", "a"}},
    record{{"id", uint64_t{2}}, {"d", data{}}, {"s", "b"}},
    record{{"id", uint64_t{3}}, {"s", "c"}},
    record{{"id", uint64_t{4}}, {"d", uint64_t{6}}, {"s", data{}}},
  })};
  auto dh = collecting_diagnostic_handler{};
  auto inserts = build(schema, events, dh);
  CHECK(not warned(dh, "not nullable"));
  REQUIRE_EQUAL(inserts.size(), size_t{3});
  CHECK_EQUAL(inserts[0].block.GetColumnCount(), size_t{3});
  CHECK_EQUAL(values<uint64_t>(get(inserts[0].block, "id")),
              (std::vector<uint64_t>{1}));
  // A null and a missing field both leave the default to the server.
  CHECK_EQUAL(inserts[1].block.GetColumnCount(), size_t{2});
  CHECK_EQUAL(values<uint64_t>(get(inserts[1].block, "id")),
              (std::vector<uint64_t>{2, 3}));
  CHECK_EQUAL(strings(get(inserts[1].block, "s")),
              (std::vector<std::string>{"b", "c"}));
  CHECK_EQUAL(inserts[2].block.GetColumnCount(), size_t{2});
  CHECK_EQUAL(values<uint64_t>(get(inserts[2].block, "d")),
              (std::vector<uint64_t>{6}));
  CHECK_EQUAL(bits(inserts[1].input_rows[0]),
              (std::vector<bool>{false, true, true, false}));
}

TEST("a null for a column that is null in all rows omits the column") {
  auto schema
    = make_schema({column("id", "UInt64"), column("d", "UInt64", "DEFAULT")});
  auto events = std::vector{make_events({
    record{{"id", uint64_t{1}}, {"d", data{}}},
    record{{"id", uint64_t{2}}, {"d", data{}}},
  })};
  auto dh = collecting_diagnostic_handler{};
  auto inserts = build(schema, events, dh);
  CHECK(dh.empty());
  REQUIRE_EQUAL(inserts.size(), size_t{1});
  CHECK_EQUAL(inserts[0].block.GetColumnCount(), size_t{1});
  CHECK_EQUAL(values<uint64_t>(get(inserts[0].block, "id")),
              (std::vector<uint64_t>{1, 2}));
}

TEST("a null for a nullable column with a default stays null") {
  auto schema = make_schema(
    {column("id", "UInt64"), column("d", "Nullable(UInt64)", "DEFAULT")});
  auto events = std::vector{make_events({
    record{{"id", uint64_t{1}}, {"d", uint64_t{5}}},
    record{{"id", uint64_t{2}}, {"d", data{}}},
  })};
  auto dh = collecting_diagnostic_handler{};
  auto inserts = build(schema, events, dh);
  REQUIRE_EQUAL(inserts.size(), size_t{1});
  auto d = get(inserts[0].block, "d")->As<::clickhouse::ColumnNullable>();
  REQUIRE(d);
  REQUIRE_EQUAL(d->Size(), size_t{2});
  CHECK(not d->IsNull(0));
  CHECK(d->IsNull(1));
}

TEST("a null for a composite column with a default omits the column") {
  auto schema = make_schema({
    column("id", "UInt64"),
    column("a", "Array(Nullable(String))", "DEFAULT"),
    column("t", "Tuple(x Nullable(Int64))", "DEFAULT"),
    column("j", "JSON", "DEFAULT"),
  });
  auto events = std::vector{make_events({
    record{{"id", uint64_t{1}}, {"a", data{}}, {"t", data{}}, {"j", data{}}},
    record{{"id", uint64_t{2}}},
  })};
  auto dh = collecting_diagnostic_handler{};
  auto inserts = build(schema, events, dh);
  CHECK(dh.empty());
  REQUIRE_EQUAL(inserts.size(), size_t{1});
  CHECK_EQUAL(inserts[0].block.GetColumnCount(), size_t{1});
  CHECK_EQUAL(values<uint64_t>(get(inserts[0].block, "id")),
              (std::vector<uint64_t>{1, 2}));
}

TEST("a null for a list column with a default omits it in catch-all tables") {
  auto schema = make_schema({
    column("id", "UInt64"),
    column("a", "Array(Nullable(String))", "DEFAULT"),
    column("event", "JSON", "", "tenzir:catch_all"),
  });
  auto events = std::vector{make_events({
    record{{"id", uint64_t{1}}, {"a", data{}}},
    record{{"id", uint64_t{2}}},
  })};
  auto dh = collecting_diagnostic_handler{};
  auto inserts = build(schema, events, dh);
  CHECK(dh.empty());
  REQUIRE_EQUAL(inserts.size(), size_t{1});
  CHECK_EQUAL(inserts[0].block.GetColumnCount(), size_t{2});
  CHECK_EQUAL(values<uint64_t>(get(inserts[0].block, "id")),
              (std::vector<uint64_t>{1, 2}));
}

TEST("a null for a list column without a default is an empty list") {
  auto schema = make_schema(
    {column("id", "UInt64"), column("a", "Array(Nullable(String))")});
  auto events = std::vector{make_events({
    record{{"id", uint64_t{1}}, {"a", data{}}},
  })};
  auto dh = collecting_diagnostic_handler{};
  auto inserts = build(schema, events, dh);
  CHECK(dh.empty());
  REQUIRE_EQUAL(inserts.size(), size_t{1});
  auto a = get(inserts[0].block, "a")->As<::clickhouse::ColumnArray>();
  REQUIRE(a);
  REQUIRE_EQUAL(a->Size(), size_t{1});
  CHECK_EQUAL(a->GetAsColumn(0)->Size(), size_t{0});
}

TEST("a null for a non-nullable column without a default drops the event") {
  auto schema = make_schema({column("id", "UInt64"), column("d", "UInt64")});
  auto events = std::vector{make_events({
    record{{"id", uint64_t{1}}, {"d", uint64_t{5}}},
    record{{"id", uint64_t{2}}, {"d", data{}}},
  })};
  auto dh = collecting_diagnostic_handler{};
  auto inserts = build(schema, events, dh);
  CHECK(warned(dh, "not nullable"));
  REQUIRE_EQUAL(inserts.size(), size_t{1});
  CHECK_EQUAL(values<uint64_t>(get(inserts[0].block, "id")),
              (std::vector<uint64_t>{1}));
}

TEST("a null for a mapped column with a default omits it in catch-all "
     "tables") {
  auto schema = make_schema({
    column("class_uid", "Int32", "DEFAULT"),
    column("activity_id", "Int32", "DEFAULT"),
    column("event", "JSON", "", "tenzir:catch_all"),
  });
  auto events = std::vector{make_events({
    record{{"class_uid", int64_t{4001}}, {"activity_id", data{}}, {"x", "a"}},
    record{{"class_uid", int64_t{4001}}},
    record{{"class_uid", int64_t{4001}}, {"activity_id", int64_t{2}}},
  })};
  auto dh = collecting_diagnostic_handler{};
  auto inserts = build(schema, events, dh);
  CHECK(dh.empty());
  REQUIRE_EQUAL(inserts.size(), size_t{2});
  auto const& omitting = inserts[0].block;
  CHECK_EQUAL(omitting.GetColumnCount(), size_t{2});
  CHECK_EQUAL(values<int32_t>(get(omitting, "class_uid")),
              (std::vector<int32_t>{4001, 4001}));
  // The null neither reaches the catch-all nor drops the event.
  CHECK_EQUAL(json(get(omitting, "event")),
              (std::vector<std::string>{R"({"x":"a"})", "{}"}));
  CHECK_EQUAL(values<int32_t>(get(inserts[1].block, "activity_id")),
              (std::vector<int32_t>{2}));
}

TEST("tuples stay consistent when rows are removed") {
  auto schema = make_schema({
    column("id", "UInt16"),
    column("t", "Tuple(a Nullable(String), b Array(Nullable(String)))"),
  });
  auto events = std::vector{make_events({
    record{{"id", uint64_t{1}},
           {"t", record{{"a", "x"}, {"b", list{"p", data{}}}}}},
    record{{"id", uint64_t{70000}}, {"t", record{{"b", list{"q"}}}}},
    record{{"id", uint64_t{3}}, {"t", record{{"a", "z"}}}},
  })};
  auto dh = collecting_diagnostic_handler{};
  auto inserts = build(schema, events, dh);
  REQUIRE_EQUAL(inserts.size(), size_t{1});
  auto t = get(inserts[0].block, "t")->As<::clickhouse::ColumnTuple>();
  REQUIRE(t);
  REQUIRE_EQUAL(t->Size(), size_t{2});
  CHECK_EQUAL(strings((*t)[0]), (std::vector<std::string>{"x", "z"}));
  auto b = (*t)[1]->As<::clickhouse::ColumnArray>();
  REQUIRE(b);
  CHECK_EQUAL(strings(b->GetAsColumn(0)),
              (std::vector<std::string>{"p", "NULL"}));
  CHECK_EQUAL(b->GetAsColumn(1)->Size(), size_t{0});
}

TEST("JSON columns serialize values and replace non-objects") {
  auto schema = make_schema({column("id", "UInt64"), column("j", "JSON")});
  auto events = std::vector{make_events({
    record{{"id", uint64_t{1}}, {"j", record{{"a", int64_t{1}}, {"n", data{}}}}},
    record{{"id", uint64_t{2}}, {"j", R"({"b":2})"}},
    record{{"id", uint64_t{3}}, {"j", int64_t{42}}},
  })};
  auto dh = collecting_diagnostic_handler{};
  auto inserts = build(schema, events, dh);
  REQUIRE_EQUAL(inserts.size(), size_t{1});
  CHECK_EQUAL(json(get(inserts[0].block, "j")),
              (std::vector<std::string>{R"({"a":1})", R"({"b":2})", "{}"}));
  CHECK(warned(dh, "cannot write `j` into a ClickHouse JSON column"));
}

TEST("catch-all tables map paths and keep the rest") {
  auto schema = make_schema({
    column("id", "UInt64"),
    column("src.port", "UInt16"),
    column("extra", "JSON", "", "tenzir:catch_all"),
  });
  auto events = std::vector{make_events({
    record{{"id", uint64_t{1}},
           {"src", record{{"port", uint64_t{443}}, {"name", "web"}}},
           {"other", "x"}},
    record{{"id", uint64_t{2}}, {"src", record{{"port", uint64_t{80}}}}},
    record{{"id", uint64_t{3}}, {"src", record{}}},
  })};
  auto dh = collecting_diagnostic_handler{};
  auto inserts = build(schema, events, dh);
  REQUIRE_EQUAL(inserts.size(), size_t{2});
  auto const& mapped = inserts[0].block;
  CHECK_EQUAL(values<uint64_t>(get(mapped, "id")),
              (std::vector<uint64_t>{1, 2}));
  CHECK_EQUAL(values<uint16_t>(get(mapped, "src.port")),
              (std::vector<uint16_t>{443, 80}));
  CHECK_EQUAL(
    json(get(mapped, "extra")),
    (std::vector<std::string>{R"({"src":{"name":"web"},"other":"x"})", "{}"}));
  CHECK_EQUAL(json(get(inserts[1].block, "extra")),
              (std::vector<std::string>{R"({"src":{}})"}));
}

TEST("an input field named like the catch-all moves into it") {
  auto schema = make_schema({
    column("id", "UInt64"),
    column("extra", "JSON", "", "tenzir:catch_all"),
  });
  auto events = std::vector{make_events({
    record{{"id", uint64_t{1}}, {"extra", record{{"a", int64_t{1}}}}},
    record{{"id", uint64_t{2}}, {"b", "x"}},
  })};
  auto dh = collecting_diagnostic_handler{};
  auto inserts = build(schema, events, dh);
  REQUIRE_EQUAL(inserts.size(), size_t{1});
  CHECK_EQUAL(
    json(get(inserts[0].block, "extra")),
    (std::vector<std::string>{R"({"extra":{"a":1}})", R"({"b":"x"})"}));
  CHECK(warned(dh, "field `extra` has the name of the catch-all column"));
}

TEST("union columns with nulls keep the row order") {
  auto schema = make_schema({column("x", "Nullable(Int64)")});
  auto events = std::vector{make_events({
    record{{"x", int64_t{1}}},
    record{{"x", data{}}},
    record{{"x", duration{std::chrono::nanoseconds{5}}}},
    record{{"x", int64_t{3}}},
  })};
  auto dh = collecting_diagnostic_handler{};
  auto inserts = build(schema, events, dh);
  REQUIRE_EQUAL(inserts.size(), size_t{1});
  auto x = get(inserts[0].block, "x")->As<::clickhouse::ColumnNullable>();
  REQUIRE(x);
  REQUIRE_EQUAL(x->Size(), size_t{4});
  CHECK(not x->IsNull(0));
  CHECK(x->IsNull(1));
  CHECK_EQUAL(values<int64_t>(x->Nested()), (std::vector<int64_t>{1, 0, 5, 3}));
}

TEST("nullable columns of one type keep the row order") {
  auto schema = make_schema(
    {column("x", "Nullable(Int64)"), column("y", "Nullable(String)")});
  auto events = std::vector{make_events({
    record{{"x", int64_t{1}}, {"y", "a"}},
    record{{"x", data{}}, {"y", data{}}},
    record{{"x", int64_t{3}}, {"y", "c"}},
  })};
  auto dh = collecting_diagnostic_handler{};
  auto inserts = build(schema, events, dh);
  REQUIRE_EQUAL(inserts.size(), size_t{1});
  auto x = get(inserts[0].block, "x")->As<::clickhouse::ColumnNullable>();
  REQUIRE(x);
  REQUIRE_EQUAL(x->Size(), size_t{3});
  CHECK(not x->IsNull(0));
  CHECK(x->IsNull(1));
  CHECK(not x->IsNull(2));
  CHECK_EQUAL(values<int64_t>(x->Nested()), (std::vector<int64_t>{1, 0, 3}));
  CHECK_EQUAL(strings(get(inserts[0].block, "y")),
              (std::vector<std::string>{"a", "NULL", "c"}));
}

TEST("overlapping list spans are written for each row") {
  auto schema = make_schema({column("xs", "Array(Int64)")});
  auto elements = nova::ArrayBuilder<nova::Data>{};
  auto dh = collecting_diagnostic_handler{};
  for (auto i = int64_t{0}; i < 4; ++i) {
    nova::append_legacy_data(elements, data{i}, dh);
  }
  // Row 0 has [1, 2, 3], row 1 has [0, 1], and row 2 repeats [1, 2, 3].
  auto spans = nova::storage::DataOwner<nova::storage::Span[]>::make_value(
    3, nova::storage::Span{0, 0});
  spans.begin()[0] = {1, 4};
  spans.begin()[1] = {0, 2};
  spans.begin()[2] = {1, 4};
  auto lists = nova::Array<nova::Data>{
    nova::Array<nova::List>{std::move(spans), elements.finish()}};
  auto fields = std::vector<
    std::pair<std::string_view, nova::MaskedArray<nova::Array<nova::Data>>>>{
    {"xs", {std::move(lists), BitMap{3, true}}},
  };
  auto events = std::vector{
    nova::Events{nova::Array<nova::Record>::from_fields(fields),
                 BitMap{3, true}, nova::Events::Meta::make_empty(3)}};
  auto inserts = build(schema, events, dh);
  REQUIRE_EQUAL(inserts.size(), size_t{1});
  auto xs = get(inserts[0].block, "xs")->As<::clickhouse::ColumnArray>();
  REQUIRE(xs);
  REQUIRE_EQUAL(xs->Size(), size_t{3});
  CHECK_EQUAL(values<int64_t>(xs->GetAsColumn(0)),
              (std::vector<int64_t>{1, 2, 3}));
  CHECK_EQUAL(values<int64_t>(xs->GetAsColumn(1)),
              (std::vector<int64_t>{0, 1}));
  CHECK_EQUAL(values<int64_t>(xs->GetAsColumn(2)),
              (std::vector<int64_t>{1, 2, 3}));
}

TEST("later columns skip rows that earlier columns rejected") {
  auto schema = make_schema({column("a", "UInt16"), column("b", "Int64")});
  auto events = std::vector{make_events({
    record{{"a", uint64_t{70000}}, {"b", "bad"}},
    record{{"a", uint64_t{1}}, {"b", int64_t{1}}},
  })};
  auto dh = collecting_diagnostic_handler{};
  auto inserts = build(schema, events, dh);
  REQUIRE_EQUAL(inserts.size(), size_t{1});
  CHECK_EQUAL(inserts[0].rows, uint64_t{1});
  CHECK(warned(dh, "value out of range for ClickHouse column `a`"));
  CHECK(not warned(dh, "incompatible type for column `b`"));
}

//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "clickhouse/catch_all_reader.hpp"

#include <tenzir/data.hpp>
#include <tenzir/nova/data_array_builder.hpp>
#include <tenzir/nova/materialize.hpp>
#include <tenzir/test/test.hpp>

#include <vector>

using namespace tenzir;
using namespace tenzir::plugins::clickhouse;

namespace {

auto column(std::string name, std::string type, std::string comment = "")
  -> ColumnDescription {
  return {std::move(name), std::move(type), "", "", std::move(comment)};
}

auto layout(std::vector<ColumnDescription> const& description)
  -> CatchAllLayout {
  auto dh = collecting_diagnostic_handler{};
  auto result = make_catch_all_layout(description, dh);
  REQUIRE(result);
  REQUIRE(*result);
  return std::move(**result);
}

/// Reconstructs `events`, whose block holds the columns of `description` in
/// order, followed by the columns `added` after the table was described.
auto reconstruct(nova::Events events,
                 std::vector<ColumnDescription> const& description,
                 diagnostic_handler& dh,
                 std::vector<std::string_view> added = {}) -> nova::Events {
  auto columns = std::vector<std::string_view>{};
  for (auto const& column : description) {
    columns.push_back(column.name);
  }
  columns.insert(columns.end(), added.begin(), added.end());
  return reconstruct_catch_all(std::move(events), columns, layout(description),
                               "ClickHouse", dh);
}

/// Builds the events that `block_to_events` decodes from `rows`, which hold
/// `JSON` columns as strings.
auto make_events(std::vector<data> const& rows) -> nova::Events {
  auto dh = collecting_diagnostic_handler{};
  auto builder = nova::ArrayBuilder<nova::Data>{};
  for (auto const& row : rows) {
    nova::append_legacy_data(builder, row, dh);
  }
  auto records = builder.finish().try_as<nova::Record>();
  REQUIRE(records);
  auto const length = records->length();
  return nova::Events{std::move(*records), nova::storage::BitMap{length, true},
                      nova::Events::Meta::make_empty(length, "test")};
}

auto rows(nova::Events const& events) -> std::vector<data> {
  auto all = nova::Array<nova::Data>{events.data};
  auto result = std::vector<data>{};
  for (auto i = nova::storage::Index{0}; i < all.length(); ++i) {
    result.push_back(nova::materialize_legacy(all.get(i)));
  }
  return result;
}

} // namespace

TEST("tables without a marker have no layout") {
  auto dh = collecting_diagnostic_handler{};
  auto result = make_catch_all_layout(
    std::vector{column("id", "Int64"), column("extra", "JSON")}, dh);
  REQUIRE(result);
  CHECK(not *result);
}

TEST("invalid markers fail as they do for writes") {
  auto dh = collecting_diagnostic_handler{};
  auto result = make_catch_all_layout(
    std::vector{column("extra", "String", "tenzir:catch_all")}, dh);
  CHECK(not result);
  auto diagnostics = std::move(dh).collect();
  REQUIRE_EQUAL(diagnostics.size(), size_t{1});
  CHECK_EQUAL(diagnostics[0].message,
              "invalid ClickHouse catch-all column `extra`");
}

TEST("the layout places columns at the paths of their names") {
  auto result = layout({
    column("extra", "JSON", "tenzir:catch_all"),
    column("src.port", "UInt16"),
    column("payload", "Nullable(JSON)"),
    column("a..b", "String"),
  });
  CHECK_EQUAL(result.column, "extra");
  REQUIRE_EQUAL(result.columns.size(), size_t{3});
  CHECK_EQUAL(result.columns[0].name, "src.port");
  REQUIRE_EQUAL(result.columns[0].path.size(), size_t{2});
  CHECK_EQUAL(result.columns[0].path[0].id.name, "src");
  CHECK_EQUAL(result.columns[0].path[1].id.name, "port");
  CHECK(not result.columns[0].json);
  CHECK(result.columns[1].json);
  // No mapping produces an empty path component, so the name stays whole.
  REQUIRE_EQUAL(result.columns[2].path.size(), size_t{1});
  CHECK_EQUAL(result.columns[2].path[0].id.name, "a..b");
}

TEST("only plain columns keep their name in the reconstructed events") {
  auto result = layout({
    column("id", "Int64"),
    column("extra", "JSON", "tenzir:catch_all"),
    column("src.port", "UInt16"),
    column("payload", "JSON"),
  });
  CHECK(result.addressable("id"));
  CHECK(not result.addressable("extra"));
  CHECK(not result.addressable("src.port"));
  CHECK(not result.addressable("payload"));
  CHECK(not result.addressable("missing"));
}

TEST("columns merge into the fields of the catch-all") {
  auto events = make_events({record{
    {"id", int64_t{1}},
    {"src.port", uint64_t{443}},
    {"extra", R"({"src":{"label":"https"},"a.b":1,"a":{"b":2},)"
              R"("peer":"10.0.0.1"})"},
  }});
  auto dh = collecting_diagnostic_handler{};
  auto result = reconstruct(std::move(events),
                            {
                              column("id", "Int64"),
                              column("src.port", "UInt16"),
                              column("extra", "JSON", "tenzir:catch_all"),
                            },
                            dh);
  CHECK(std::move(dh).collect().empty());
  // Literal dotted keys stay apart from nested paths, and strings in the
  // catch-all get their types inferred.
  CHECK_EQUAL(rows(result), (std::vector<data>{record{
                              {"src",
                               record{
                                 {"label", "https"},
                                 {"port", uint64_t{443}},
                               }},
                              {"a.b", int64_t{1}},
                              {"a", record{{"b", int64_t{2}}}},
                              {"peer", ip::v4(0x0a000001)},
                              {"id", int64_t{1}},
                            }}));
  CHECK_EQUAL(std::string_view{*result.meta.name.get(0)}, "test");
}

TEST("columns win over catch-all fields at the same path") {
  auto events = make_events({
    record{
      {"id", int64_t{1}},
      {"name", caf::none},
      {"extra", R"({"id":5,"x":"y"})"},
    },
    record{
      {"id", int64_t{2}},
      {"name", "n"},
      {"extra", "{}"},
    },
  });
  auto dh = collecting_diagnostic_handler{};
  auto result = reconstruct(std::move(events),
                            {
                              column("id", "Int64"),
                              column("name", "Nullable(String)"),
                              column("extra", "JSON", "tenzir:catch_all"),
                            },
                            dh);
  CHECK(std::move(dh).collect().empty());
  CHECK_EQUAL(rows(result), (std::vector<data>{
                              record{
                                {"id", int64_t{1}},
                                {"x", "y"},
                                {"name", caf::none},
                              },
                              record{
                                {"id", int64_t{2}},
                                {"name", "n"},
                              },
                            }));
}

TEST("generated columns win over catch-all fields of the same name") {
  // With `asterisk_include_alias_columns=1`, `SELECT *` returns the ALIAS
  // column `a`, while `to_clickhouse` kept the input field `a` in the
  // catch-all. The value that ClickHouse returns as a column stays.
  auto events = make_events({record{
    {"id", int64_t{1}},
    {"a", "computed"},
    {"extra", R"({"a":"input"})"},
  }});
  auto alias = column("a", "String");
  alias.default_kind = "ALIAS";
  auto dh = collecting_diagnostic_handler{};
  auto result = reconstruct(std::move(events),
                            {
                              column("id", "Int64"),
                              alias,
                              column("extra", "JSON", "tenzir:catch_all"),
                            },
                            dh);
  CHECK(std::move(dh).collect().empty());
  CHECK_EQUAL(rows(result), (std::vector<data>{record{
                              {"a", "computed"},
                              {"id", int64_t{1}},
                            }}));
}

TEST("catch-all records never replace a column below them") {
  auto events = make_events({record{
    {"src.port", uint64_t{443}},
    {"extra", R"({"src":{"port":1,"label":"https"}})"},
  }});
  auto dh = collecting_diagnostic_handler{};
  auto result = reconstruct(std::move(events),
                            {
                              column("src.port", "UInt16"),
                              column("extra", "JSON", "tenzir:catch_all"),
                            },
                            dh);
  CHECK(std::move(dh).collect().empty());
  CHECK_EQUAL(rows(result), (std::vector<data>{record{
                              {"src",
                               record{
                                 {"port", uint64_t{443}},
                                 {"label", "https"},
                               }},
                            }}));
}

TEST("columns that the table gained after it was described stay") {
  auto events = make_events({record{
    {"id", int64_t{1}},
    {"extra", R"({"x":{"z":1}})"},
    {"later", int64_t{2}},
    {"x.y", int64_t{7}},
  }});
  auto dh = collecting_diagnostic_handler{};
  auto result = reconstruct(std::move(events),
                            {
                              column("id", "Int64"),
                              column("extra", "JSON", "tenzir:catch_all"),
                            },
                            dh, {"later", "x.y"});
  CHECK(std::move(dh).collect().empty());
  CHECK_EQUAL(rows(result), (std::vector<data>{record{
                              {"x",
                               record{
                                 {"z", int64_t{1}},
                                 {"y", int64_t{7}},
                               }},
                              {"id", int64_t{1}},
                              {"later", int64_t{2}},
                            }}));
}

TEST("other JSON columns become values") {
  auto events = make_events({
    record{{"payload", R"({"k":[1,2]})"}, {"extra", "{}"}},
    record{{"payload", caf::none}, {"extra", "{}"}},
  });
  auto dh = collecting_diagnostic_handler{};
  auto result = reconstruct(std::move(events),
                            {
                              column("payload", "Nullable(JSON)"),
                              column("extra", "JSON", "tenzir:catch_all"),
                            },
                            dh);
  CHECK(std::move(dh).collect().empty());
  CHECK_EQUAL(
    rows(result),
    (std::vector<data>{
      record{{"payload", record{{"k", list{int64_t{1}, int64_t{2}}}}}},
      record{{"payload", caf::none}},
    }));
}

TEST("invalid JSON becomes null with a warning") {
  auto events = make_events({
    record{{"id", int64_t{1}}, {"extra", "{"}},
    record{{"id", int64_t{2}}, {"extra", R"({"x":1})"}},
  });
  auto dh = collecting_diagnostic_handler{};
  auto result = reconstruct(std::move(events),
                            {
                              column("id", "Int64"),
                              column("extra", "JSON", "tenzir:catch_all"),
                            },
                            dh);
  auto diagnostics = std::move(dh).collect();
  REQUIRE_EQUAL(diagnostics.size(), size_t{1});
  CHECK_EQUAL(diagnostics[0].severity, severity::warning);
  CHECK_EQUAL(diagnostics[0].message,
              "failed to parse JSON of ClickHouse column `extra`");
  CHECK_EQUAL(rows(result), (std::vector<data>{
                              record{{"id", int64_t{1}}},
                              record{{"x", int64_t{1}}, {"id", int64_t{2}}},
                            }));
}

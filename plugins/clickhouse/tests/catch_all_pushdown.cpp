//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

// The origins of the fields of a catch-all table, which reconstruction takes
// from plain columns, dotted columns, `JSON` columns, and the catch-all column,
// and to which the stages that `from_clickhouse` takes over bind.

#include "clickhouse/catch_all_reader.hpp"

#include <tenzir/diagnostics.hpp>
#include <tenzir/test/test.hpp>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <string>
#include <vector>

namespace tenzir::plugins::clickhouse {

namespace {

auto column(std::string name, std::string type, std::string default_kind = "",
            std::string comment = "") -> ColumnDescription {
  return {std::move(name), std::move(type), std::move(default_kind), "",
          std::move(comment)};
}

auto description() -> std::vector<ColumnDescription> {
  return {
    column("time", "DateTime64(9)"),
    column("class_uid", "UInt32"),
    column("src_endpoint.ip", "Nullable(IPv6)"),
    column("src_endpoint.port", "UInt16"),
    column("unmapped", "JSON"),
    column("a.j", "Nullable(JSON)"),
    column("typed", "JSON(obj Map(String, String))"),
    column("dyn", "JSON(obj Dynamic)"),
    column("skip", "JSON(SKIP x)"),
    column("sized", "JSON(max_dynamic_types=2, max_dynamic_paths=3)"),
    column("t", "Tuple(a String, b Int64)"),
    column("d.t", "Tuple(a String)"),
    column("g", "String", "ALIAS"),
    column("c", "Int64"),
    column("c.d", "Int64"),
    column("event", "JSON", "", "tenzir:catch_all"),
  };
}

auto layout(std::vector<ColumnDescription> const& description)
  -> CatchAllLayout {
  auto dh = collecting_diagnostic_handler{};
  auto result = make_catch_all_layout(description, dh);
  REQUIRE(result);
  REQUIRE(*result);
  return std::move(**result);
}

auto origin(CatchAllLayout const& layout, std::vector<std::string> path)
  -> std::string {
  auto result = layout.origin(path);
  if (not result) {
    return "none";
  }
  return fmt::format("{}{}:{}", result->column, result->json ? " (json)" : "",
                     fmt::join(result->path, "."));
}

} // namespace

TEST("fields of a catch-all table have the origin of their column") {
  auto const catch_all = layout(description());
  CHECK_EQUAL(origin(catch_all, {"class_uid"}), "class_uid:");
  CHECK_EQUAL(origin(catch_all, {"src_endpoint", "port"}),
              "src_endpoint.port:");
  CHECK_EQUAL(origin(catch_all, {"src_endpoint", "port", "x"}),
              "src_endpoint.port:x");
  CHECK_EQUAL(origin(catch_all, {"src_endpoint", "hostname"}),
              "event (json):src_endpoint.hostname");
  CHECK_EQUAL(origin(catch_all, {"src_endpoint"}), "none");
  CHECK_EQUAL(origin(catch_all, {"unmapped"}), "unmapped (json):");
  CHECK_EQUAL(origin(catch_all, {"unmapped", "a", "b"}), "unmapped (json):a.b");
  CHECK_EQUAL(origin(catch_all, {"a", "j", "k"}), "a.j (json):k");
  CHECK_EQUAL(origin(catch_all, {"a"}), "none");
  CHECK_EQUAL(origin(catch_all, {"a", "b"}), "event (json):a.b");
  CHECK_EQUAL(origin(catch_all, {"g"}), "none");
  CHECK_EQUAL(origin(catch_all, {"g", "x"}), "none");
  CHECK_EQUAL(origin(catch_all, {"c"}), "none");
  CHECK_EQUAL(origin(catch_all, {"c", "d"}), "none");
  CHECK_EQUAL(origin(catch_all, {"c", "e"}), "c:e");
  CHECK_EQUAL(origin(catch_all, {"event", "x"}), "event (json):event.x");
  CHECK_EQUAL(origin(catch_all, {}), "none");
  CHECK(catch_all.addressable("class_uid"));
  CHECK(catch_all.addressable("t"));
  CHECK(not catch_all.addressable("c"));
  CHECK(not catch_all.addressable("g"));
  CHECK(not catch_all.addressable("src_endpoint.port"));
}

TEST("a catch-all column with typed or skipped paths is invalid") {
  // Its fields could hold records without paths below them in ClickHouse.
  for (auto type : {"JSON(obj Map(String, String))", "JSON(obj Dynamic)",
                    "JSON(SKIP x)", "Nullable(JSON)"}) {
    auto dh = collecting_diagnostic_handler{};
    auto result = make_catch_all_layout(
      std::vector{column("event", type, "", "tenzir:catch_all")}, dh);
    CHECK(not result);
  }
}

} // namespace tenzir::plugins::clickhouse

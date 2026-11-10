//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

// Pushdown of predicates on the fields of a catch-all table, which
// reconstruction takes from plain columns, dotted columns, `JSON` columns,
// and the catch-all column.

#include "clickhouse/catch_all_reader.hpp"
#include "clickhouse/sql_pushdown.hpp"

#include <tenzir/diagnostics.hpp>
#include <tenzir/pushdown/translate.hpp>
#include <tenzir/session.hpp>
#include <tenzir/test/registry.hpp>
#include <tenzir/test/test.hpp>
#include <tenzir/tql2/parser.hpp>
#include <tenzir/tql2/resolve.hpp>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <array>
#include <span>
#include <string_view>

namespace tenzir::plugins::clickhouse {

namespace {

/// A predicate, what of it pushes, and whether it also stays local.
struct Case {
  std::string_view predicate;
  std::string_view pushed;
  bool local;
};

auto parse(std::string_view source) -> ast::expression {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh, test::builtin_registry());
  auto ctx = provider.as_session();
  auto expr
    = parse_expression_with_location_override(source, location::unknown, ctx);
  REQUIRE(expr);
  REQUIRE(resolve_entities(*expr, ExternPrelude::empty(), ctx));
  return std::move(*expr);
}

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

constexpr auto cases = std::to_array<Case>({
  // Plain and dotted scalar columns push exactly.
  Case{"class_uid == 4001", "`class_uid` = 4001", false},
  Case{"src_endpoint.port == 443", "`src_endpoint.port` = 443", false},
  Case{"src_endpoint.ip == null", "`src_endpoint.ip` IS NULL", false},
  Case{"t.a == \"x\"", "`t`.`a` = 'x'", false},
  // Fields of `JSON` columns, including the catch-all column, stay local.
  Case{R"(metadata.product.name == "test")", "", true},
  Case{R"(src_endpoint.hostname == "a")", "", true},
  Case{"src_endpoint != null", "", true},
  Case{R"(unmapped.source_field == "x")", "", true},
  Case{R"(this["a.b"] == 1)", "", true},
  // Dotted tuples, generated columns, and columns that share a field have no
  // single origin.
  Case{R"(d.t.a == "x")", "", true},
  Case{R"(g == "x")", "", true},
  Case{"c == 1", "", true},
  Case{"c.e == 1", "", true},
  // Conjunctions split.
  Case{
    R"(class_uid == 4001 and x == "a")",
    "`class_uid` = 4001",
    true,
  },
  Case{R"(class_uid == 4001 or x == "a")", "", true},
});

auto check_pushdown(std::span<Case const> cases, SqlTarget target) -> void {
  auto const columns = description();
  auto const catch_all = layout(columns);
  for (auto const& x : cases) {
    auto filter = ir::OptimizeFilter{};
    filter.push_back(parse(x.predicate));
    auto const schema = make_catch_all_schema(columns, catch_all);
    auto split = pushdown::split_filter(std::move(filter), schema.model(),
                                        ClickHouseRenderer{target, &schema},
                                        test::builtin_registry());
    CHECK_EQUAL(fmt::format("{}: {}", x.predicate,
                            fmt::join(split.pushed, " AND ")),
                fmt::format("{}: {}", x.predicate, x.pushed));
    CHECK_EQUAL(fmt::format("{}: {}", x.predicate, not split.remaining.empty()),
                fmt::format("{}: {}", x.predicate, x.local));
  }
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

TEST("hydrolix pushes predicates on the columns of a catch-all table") {
  check_pushdown(cases, SqlTarget::hydrolix);
}

} // namespace tenzir::plugins::clickhouse

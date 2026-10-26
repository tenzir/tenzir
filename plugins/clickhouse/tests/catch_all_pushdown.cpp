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
  // The example of the issue reads the path of the catch-all column. The
  // predicate stays local, because ClickHouse only narrows the rows.
  Case{
    R"(metadata.product.name == "test")",
    "toString(`event`.`metadata`.`product`.`name`) = 'test'",
    true,
  },
  // Plain and dotted scalar columns push exactly.
  Case{"class_uid == 4001", "`class_uid` = 4001", false},
  Case{"src_endpoint.port == 443", "`src_endpoint.port` = 443", false},
  Case{"src_endpoint.ip == null", "`src_endpoint.ip` IS NULL", false},
  Case{"t.a == \"x\"", "`t`.`a` = 'x'", false},
  // Siblings of dotted columns come from the catch-all column, while their
  // parent merges both.
  Case{
    R"(src_endpoint.hostname == "a")",
    "toString(`event`.`src_endpoint`.`hostname`) = 'a'",
    true,
  },
  Case{"src_endpoint != null", "", true},
  // Fields below other `JSON` columns read from them, also with dots in
  // their names, but their whole value is a record.
  Case{
    R"(unmapped.source_field == "x")",
    "toString(`unmapped`.`source_field`) = 'x'",
    true,
  },
  Case{
    R"(a.j.k == "x")",
    "toString(`a.j`.`k`) = 'x'",
    true,
  },
  Case{"unmapped != null", "", true},
  // A typed path may hold a record of TQL that ClickHouse has no paths below,
  // so only `JSON` columns without typed or skipped paths read fields.
  Case{R"(typed.obj.k == "value")", "", true},
  Case{"typed.obj.k != null", "", true},
  Case{R"(dyn.obj.k == "value")", "", true},
  Case{"dyn.obj != null", "", true},
  Case{"dyn.obj.k != null", "", true},
  Case{R"(skip.y == "value")", "", true},
  Case{
    R"(sized.k == "value")",
    "toString(`sized`.`k`) = 'value'",
    true,
  },
  // Dotted tuples, generated columns, and columns that share a field have no
  // single origin.
  Case{R"(d.t.a == "x")", "", true},
  Case{R"(g == "x")", "", true},
  Case{R"(g.x == "x")", "", true},
  Case{"c == 1", "", true},
  Case{"c.d == 1", "", true},
  Case{"c.e == 1", "", true},
  // Keys with dots are escaped, other keys are literal.
  Case{
    R"(this["a.b"] == 1)",
    "toFloat64OrNull(toString(`event`.`a%2Eb`)) = 1",
    true,
  },
  Case{
    "a.b == 2",
    "toFloat64OrNull(toString(`event`.`a`.`b`)) = 2",
    true,
  },
  Case{
    R"(this["it's"]["`x:"] == "y")",
    R"(toString(`event`.`it's`.`\`x:`) = 'y')",
    true,
  },
  Case{R"(this[""] == "y")", "", true},
  Case{R"(x[""] == "y")", "", true},
  // Every literal compares with the text of the value.
  Case{"x > 5", "toFloat64OrNull(toString(`event`.`x`)) > 5", true},
  Case{"x <= -2.5", "toFloat64OrNull(toString(`event`.`x`)) <= -2.5", true},
  Case{"x == true", "toString(`event`.`x`) = 'true'", true},
  Case{"x", "toString(`event`.`x`) = 'true'", true},
  Case{"x == false", "toString(`event`.`x`) = 'false'", true},
  Case{R"(x < "b")", "toString(`event`.`x`) < 'b'", true},
  Case{
    R"(x in ["a", 1, true])",
    "(toString(`event`.`x`) = 'a' OR "
    "toFloat64OrNull(toString(`event`.`x`)) = 1 OR "
    "toString(`event`.`x`) = 'true')",
    true,
  },
  Case{
    R"(x in ["a"])",
    "toString(`event`.`x`) = 'a'",
    true,
  },
  Case{
    R"(x.starts_with("a"))",
    "startsWith(toString(`event`.`x`), 'a')",
    true,
  },
  Case{
    R"(x.ends_with("a"))",
    "endsWith(toString(`event`.`x`), 'a')",
    true,
  },
  Case{
    R"("a" in x)",
    "position(toString(`event`.`x`), 'a') > 0",
    true,
  },
  // A list escapes these characters in its text.
  Case{R"("it's" in x)", "", true},
  Case{R"("a\\b" in x)", "", true},
  Case{R"("a\nb" in x)", "", true},
  // Objects have no value of their own, but paths below them.
  Case{
    "x.y != null",
    "(`event`.`x`.`y` IS NOT NULL OR "
    "toString(`event`.^`x`.`y`) != '{}')",
    true,
  },
  // Negations, null checks, and comparisons whose type TQL decides stay
  // local.
  Case{"x == null", "", true},
  Case{R"(x != "a")", "", true},
  Case{R"(not (x == "a"))", "", true},
  Case{R"(x.starts_with("a", ignore_case=true))", "", true},
  Case{R"(x.match_regex("a"))", "", true},
  Case{"x + 1 > 2", "", true},
  Case{"x == y", "", true},
  Case{"x == class_uid", "", true},
  Case{"x == 2026-01-01", "", true},
  Case{"x == 1.2.3.4", "", true},
  Case{"x == 9007199254740993", "", true},
  Case{"x > 9007199254740992",
       "toFloat64OrNull(toString(`event`.`x`)) >= "
       "9007199254740992",
       true},
  Case{"x < -9007199254740992",
       "toFloat64OrNull(toString(`event`.`x`)) <= "
       "-9007199254740992",
       true},
  Case{"xs.where(x => x == 1).length() > 0", "", true},
  // Conjunctions split, and disjunctions with exact parts narrow too.
  Case{
    R"(class_uid == 4001 and x == "a")",
    "`class_uid` = 4001 AND toString(`event`.`x`) = 'a'",
    true,
  },
  Case{
    R"(x == "a" and y.length() > 3)",
    "toString(`event`.`x`) = 'a'",
    true,
  },
  Case{
    R"(class_uid == 4001 or x == "a")",
    "(`class_uid` = 4001 OR toString(`event`.`x`) = 'a')",
    true,
  },
  Case{R"(class_uid == 4001 or not (x == "a"))", "", true},
});

auto check_pushdown(std::span<Case const> cases, SqlTarget target) -> void {
  auto const columns = description();
  auto const catch_all = layout(columns);
  for (auto const& x : cases) {
    auto filter = ir::OptimizeFilter{};
    filter.push_back(parse(x.predicate));
    auto const schema = make_catch_all_schema(columns, catch_all, filter,
                                              target == SqlTarget::clickhouse);
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

TEST("pushdown of predicates on the fields of a catch-all table") {
  check_pushdown(cases, SqlTarget::clickhouse);
}

TEST("hydrolix gets no prefilters on the fields of a catch-all table") {
  check_pushdown(
    std::to_array<Case>({
      Case{R"(x == "a")", "", true},
      Case{"src_endpoint.port == 443", "`src_endpoint.port` = 443", false},
    }),
    SqlTarget::hydrolix);
}

TEST("a dotted column shadows the paths below it in the catch-all column") {
  // ClickHouse resolves `event`.`a`.`b` to the column `event.a`, so the paths
  // that start with `a` stay local. A column deeper than the path, such as
  // `event.p.q.r` for the path `p.q`, does not capture it.
  auto columns = std::vector{
    column("class_uid", "UInt32"),
    column("event.a", "String"),
    column("event.p.q.r", "String"),
    column("event", "JSON", "", "tenzir:catch_all"),
  };
  auto const catch_all = layout(columns);
  auto const check = [&](std::string_view predicate, std::string_view pushed,
                         std::string_view shadowing) {
    auto filter = ir::OptimizeFilter{};
    filter.push_back(parse(predicate));
    auto const schema = make_catch_all_schema(columns, catch_all, filter, true);
    auto split = pushdown::split_filter(
      std::move(filter), schema.model(),
      ClickHouseRenderer{SqlTarget::clickhouse, &schema},
      test::builtin_registry());
    CHECK_EQUAL(fmt::format("{}: {}", predicate,
                            fmt::join(split.pushed, " AND ")),
                fmt::format("{}: {}", predicate, pushed));
    CHECK(not split.remaining.empty());
    auto shadowed = std::vector<std::string>{};
    for (auto const& field : schema.shadowed_fields()) {
      shadowed.push_back(
        fmt::format("{} by {}", fmt::join(field.path, "."), field.column));
    }
    CHECK_EQUAL(fmt::format("{}", fmt::join(shadowed, ", ")), shadowing);
  };
  check(R"(a.b == "x")", "", "a.b by event.a");
  check(R"(a == "x")", "", "a by event.a");
  check(R"(p.q == "x")", "toString(`event`.`p`.`q`) = 'x'", "");
  check("p.q != null",
        "(`event`.`p`.`q` IS NOT NULL OR toString(`event`.^`p`.`q`) != '{}')",
        "");
}

} // namespace tenzir::plugins::clickhouse

//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/diagnostics.hpp"
#include "tenzir/pushdown/splunk.hpp"
#include "tenzir/pushdown/sql_source.hpp"
#include "tenzir/session.hpp"
#include "tenzir/test/registry.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/tql2/parser.hpp"
#include "tenzir/tql2/resolve.hpp"

#include <string>
#include <string_view>
#include <vector>

using namespace tenzir;
using namespace tenzir::pushdown;
using namespace std::literals;

namespace {

using Path = std::vector<std::string>;

/// The field at `path` as an expression.
auto field_at(std::vector<std::string> const& path) -> ast::expression {
  auto result = ast::expression{
    ast::root_field{ast::identifier{path.front(), location::unknown}, false}};
  for (auto i = size_t{1}; i < path.size(); ++i) {
    result = ast::field_access{std::move(result), location::unknown, false,
                               ast::identifier{path[i], location::unknown}};
  }
  return result;
}

auto parse(std::string_view source) -> ast::expression {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh, test::builtin_registry());
  auto ctx = provider.as_session();
  auto expr
    = parse_expression_with_location_override(source, location::unknown, ctx);
  if (not expr) {
    FAIL("failed to parse: {}", source);
  }
  REQUIRE(expr);
  REQUIRE(resolve_entities(*expr, ExternPrelude::empty(), ctx));
  return std::move(*expr);
}

auto paths(std::vector<std::string_view> sources) -> ir::OptimizeProjection {
  auto result = ir::OptimizeProjection{};
  for (auto source : sources) {
    auto path = ast::field_path::try_from(parse(source));
    REQUIRE(path);
    result.push_back(std::move(*path));
  }
  return result;
}

auto at(std::string_view source) -> tenzir::time {
  auto expr = parse(source);
  auto const* constant = try_as<ast::constant>(expr);
  REQUIRE(constant);
  return as<tenzir::time>(constant->value);
}

auto filter(std::string_view predicate) -> ir::QueryStage {
  return {ir::QueryStage::Filter{parse(predicate)}};
}

auto select(std::vector<std::pair<Path, Path>> fields) -> ir::QueryStage {
  auto result = ir::QueryStage::Select{};
  for (auto& [output, input] : fields) {
    result.fields.push_back({.output = output, .input = input});
  }
  return {std::move(result)};
}

auto drop(std::vector<Path> fields) -> ir::QueryStage {
  return {ir::QueryStage::Drop{std::move(fields)}};
}

auto sort(std::vector<std::string_view> keys, Option<uint64_t> bound = None{})
  -> ir::QueryStage {
  auto result = ir::QueryStage::Sort{.keys = {}, .bound = bound};
  for (auto key : keys) {
    auto descending = key.starts_with('-');
    if (descending) {
      key.remove_prefix(1);
    }
    result.keys.push_back(
      {.field = {std::string{key}}, .descending = descending});
  }
  return {std::move(result)};
}

auto limit(uint64_t count) -> ir::QueryStage {
  return {ir::QueryStage::Limit{count}};
}

auto main_index() -> SplSource {
  return {.search = None{}, .indexes = {"main"}};
}

/// The search that reads `source` and computes `stages`, or `None` if Splunk
/// does not spell them.
auto plan_spl_query(SplSource const& source,
                    std::span<ir::QueryStage const> stages,
                    Option<ir::OptimizeProjection> const& projection)
  -> Option<SplQuery> {
  auto planned = spl_planner(source, None{}, projection).plan(stages, nullptr);
  if (not planned) {
    return None{};
  }
  return std::move(planned).unwrap();
}

/// Returns whether the planner of `source` takes over no stage.
auto spl_declines(SplSource const& source) -> bool {
  return spl_planner(source, None{}, None{})
    .plan(std::vector{filter("n > 1")}, nullptr)
    .is_err();
}

auto user(std::string_view search) -> SplSource {
  return {.search = std::string{search}, .indexes = {}};
}

auto spl2(std::string_view search) -> SplSource {
  return {.search = std::string{search}, .indexes = {}, .spl2 = true};
}

/// The search that reads `source` and computes `stages`, or the empty string
/// if Splunk does not spell them.
auto spl(SplSource const& source, std::vector<ir::QueryStage> stages,
         Option<std::vector<std::string_view>> projection = None{})
  -> std::string {
  auto projected = Option<ir::OptimizeProjection>{};
  if (projection) {
    projected = paths(*projection);
  }
  auto query = plan_spl_query(source, stages, projected);
  if (not query) {
    return {};
  }
  return query->search;
}

/// The terms that a filter adds to the search for the index `main`, and the
/// stages after it, without the final `fields *`.
auto index_filter(std::string_view predicate) -> std::string {
  auto search = spl(main_index(), {filter(predicate)});
  if (search.empty()) {
    return {};
  }
  constexpr auto prefix = "search index=\"main\""sv;
  constexpr auto suffix = "\n| fields *"sv;
  REQUIRE(search.starts_with(prefix));
  REQUIRE(search.ends_with(suffix));
  return search.substr(prefix.size(),
                       search.size() - prefix.size() - suffix.size());
}

/// The `where` expression of a filter after a user's search, or the empty
/// string if Splunk does not spell it.
auto where(std::string_view predicate) -> std::string {
  auto search = spl(user("S"), {filter(predicate)});
  if (search.empty()) {
    return {};
  }
  constexpr auto prefix = "search S\n| where "sv;
  constexpr auto suffix = "\n| fields *"sv;
  REQUIRE(search.starts_with(prefix));
  REQUIRE(search.ends_with(suffix));
  return search.substr(prefix.size(),
                       search.size() - prefix.size() - suffix.size());
}

} // namespace

TEST("spl spells leading filters of indexes as search terms") {
  CHECK_EQUAL(index_filter("n > 6"), " n>6");
  CHECK_EQUAL(index_filter("6 < n"), " n>6");
  CHECK_EQUAL(index_filter("n >= -1.5"), " n>=-1.5");
  CHECK_EQUAL(index_filter("n == 7"), " n=7");
  CHECK_EQUAL(index_filter(R"(user == "alice")"), R"( user="alice")");
  CHECK_EQUAL(index_filter(R"(user == "a\"b\\c")"), R"( user="a\"b\\c")");
  CHECK_EQUAL(index_filter("flag == true"), " flag=true");
  CHECK_EQUAL(index_filter("src == 192.0.2.10"), R"( src="192.0.2.10")");
  CHECK_EQUAL(index_filter("src in 10.0.0.0/8"), R"( src="10.0.0.0/8")");
  CHECK_EQUAL(index_filter(R"(user in ["alice", 5, true])"),
              R"( user IN ("alice", 5, true))");
  CHECK_EQUAL(index_filter("action != null"), " action=*");
  CHECK_EQUAL(index_filter(R"(user.starts_with("al"))"), R"( user="al*")");
  CHECK_EQUAL(index_filter(R"(user.ends_with("ce"))"), R"( user="*ce")");
  CHECK_EQUAL(index_filter(R"("lic" in user)"), R"( user="*lic*")");
  CHECK_EQUAL(index_filter(R"(this["weird field"] == "x")"),
              R"( "weird field"="x")");
  CHECK_EQUAL(index_filter(R"(n > 6 or user == "bob")"),
              R"( (n>6 OR user="bob"))");
  CHECK_EQUAL(index_filter("_time >= 2025-01-01T00:01:00.5Z"),
              " _time>=1735689660.5");
  // Each conjunct of a filter goes into the search or into a `where` stage
  // as a whole.
  CHECK_EQUAL(index_filter(R"(n > 6 and user != "bob" and x == 1)"),
              " n>6 x=1\n| where ('user' != \"bob\")");
  // A wildcard in a string would match more than the string, and search
  // modifiers are no fields, so these stay in `where`.
  CHECK_EQUAL(index_filter(R"(user == "a*")"), "\n| where ('user' == \"a*\")");
  CHECK_EQUAL(index_filter(R"(earliest == "x")"),
              "\n| where ('earliest' == \"x\")");
  CHECK_EQUAL(index_filter(R"(user.starts_with("a*"))"),
              "\n| where (substr('user', 1, len(\"a*\"))==\"a*\")");
}

TEST("spl spells other filters as where stages") {
  CHECK_EQUAL(where("n > 6"), "('n' > 6)");
  CHECK_EQUAL(where("n != 6.5"), "('n' != 6.5)");
  CHECK_EQUAL(where(R"(user == "a\"b")"), R"(('user' == "a\"b"))");
  CHECK_EQUAL(where("flag == true"), R"(('flag' == "true"))");
  CHECK_EQUAL(where("x == null"), "isnull('x')");
  CHECK_EQUAL(where("x != null"), "isnotnull('x')");
  CHECK_EQUAL(where("n in [1, 2]"), "in('n', 1, 2)");
  CHECK_EQUAL(where(R"(not (user == "a") or n + 1 > 2)"),
              R"(((NOT ('user' == "a")) OR (('n' + 1) > 2)))");
  CHECK_EQUAL(where(R"(user.starts_with("al"))"),
              R"((substr('user', 1, len("al"))=="al"))");
  CHECK_EQUAL(where(R"(user.ends_with("ce"))"),
              R"((substr('user', len('user')-len("ce")+1)=="ce"))");
  CHECK_EQUAL(where(R"(user.match_regex("^a"))"), R"(match('user', "^a"))");
  CHECK_EQUAL(where(R"(user.to_lower() == "a")"), R"((lower('user') == "a"))");
  CHECK_EQUAL(where(R"(this["a b"] == 1)"), "('a b' == 1)");
  CHECK_EQUAL(where("_time > 2025-01-01T00:01:00Z"), "('_time' > 1735689660)");
  // A string or a field name that SPL cannot hold, nested paths, bare
  // fields, which SPL does not read as predicates, and times of other fields,
  // which hold text, have no spelling.
  CHECK_EQUAL(where("user == \"a\\nb\""), "");
  CHECK_EQUAL(where(R"(this["a|b"] == 1)"), "");
  CHECK_EQUAL(where("obj.k == 1"), "");
  CHECK_EQUAL(where("flag"), "");
  CHECK_EQUAL(where("ts > 2025-01-01T00:01:00Z"), "");
  CHECK_EQUAL(where("src in 10.0.0.0/8"), "");
  // Filters after other stages use `where` also for indexes.
  CHECK_EQUAL(spl(main_index(), {limit(5), filter("n > 6")}),
              "search index=\"main\"\n| head 5\n| where ('n' > 6)\n| fields *");
}

TEST("spl selects fields with fields and renames in the layout") {
  auto query = plan_spl_query(
    main_index(),
    std::vector{
      select({{{"u"}, {"user"}}, {{"n"}, {"n"}}, {{"x", "y"}, {"n"}}})},
    None{});
  REQUIRE(query);
  CHECK_EQUAL(query->search, "search index=\"main\"\n| fields user, n");
  REQUIRE(query->layout.fields);
  auto const expected = LayoutFields{
    {{"u"}, "user"},
    {{"n"}, "n"},
    {{"x", "y"}, "n"},
  };
  CHECK_EQUAL(*query->layout.fields, expected);
  CHECK(not query->sentinel);
  // Commands name other fields in double quotes. Every input refers to the
  // fields before the stage.
  CHECK_EQUAL(spl(main_index(),
                  {select({{{"a"}, {"a b"}}, {{"b"}, {"tags{}"}}})}),
              "search index=\"main\"\n| fields \"a b\", \"tags{}\"");
  CHECK_EQUAL(spl(main_index(),
                  {select({{{"b"}, {"a"}}, {{"a"}, {"b"}}}), filter("a > 1")}),
              "search index=\"main\"\n| fields a, b\n| where ('b' > 1)");
  // A field that neither a command nor an expression can name has no
  // spelling.
  CHECK_EQUAL(spl(main_index(), {select({{{"y"}, {"a*"}}})}), "");
  // An empty selection could make Splunk drop the results.
  CHECK_EQUAL(spl(main_index(), {select({})}), "");
  // Fields that the selection removed are gone.
  CHECK_EQUAL(spl(main_index(), {select({{{"a"}, {"a"}}}), filter("b > 1")}),
              "");
}

TEST("spl spells times before the epoch") {
  CHECK_EQUAL(index_filter("_time == 1969-12-31T23:59:59.5Z"), " _time=-0.5");
  CHECK_EQUAL(index_filter("_time == 1969-12-31T23:59:58Z"), " _time=-2");
}

TEST("spl drops fields") {
  CHECK_EQUAL(spl(main_index(), {drop({{"a"}, {"b c"}})}),
              "search index=\"main\"\n| fields - a, \"b c\"\n| fields *");
  CHECK_EQUAL(spl(main_index(), {drop({{"a"}}), filter("a > 1")}), "");
  CHECK_EQUAL(spl(main_index(), {drop({{"a", "b"}})}), "");
  // Splunk treats fields that start with `__` as internal, and `fields - __x`
  // discards every result.
  CHECK_EQUAL(spl(main_index(), {drop({{"__x"}})}), "");
  CHECK_EQUAL(spl(main_index(), {select({{{"a"}, {"__x"}}})}), "");
  // After a selection, a drop only changes the layout.
  auto query = plan_spl_query(
    main_index(),
    std::vector{select({{{"a"}, {"a"}}, {{"b"}, {"b"}}}), drop({{"a"}})},
    None{});
  REQUIRE(query);
  CHECK_EQUAL(query->search, "search index=\"main\"\n| fields a, b");
  CHECK_EQUAL(*query->layout.fields, (LayoutFields{{{"b"}, "b"}}));
  // A drop that leaves no field has no spelling.
  CHECK_EQUAL(spl(main_index(), {select({{{"a"}, {"a"}}}), drop({{"a"}})}), "");
}

TEST("spl sorts with an explicit count") {
  CHECK_EQUAL(spl(main_index(), {sort({"-n", "user"})}),
              "search index=\"main\"\n| sort 0 -n, user\n| fields *");
  CHECK_EQUAL(spl(main_index(), {sort({"-n"}, 3), limit(3)}),
              "search index=\"main\"\n| sort 3 -n\n| head 3\n| fields *");
  CHECK_EQUAL(spl(main_index(), {sort({"n"}, 0), limit(0)}), "");
  CHECK_EQUAL(spl(main_index(), {sort({"a*"})}), "");
  // `head 0` ends the search without running the stages after it.
  CHECK_EQUAL(spl(main_index(), {limit(0)}), "");
}

TEST("spl keeps the fields that downstream needs") {
  CHECK_EQUAL(spl(main_index(), {}), "search index=\"main\"\n| fields *");
  CHECK_EQUAL(spl(main_index(), {}, std::vector{"user"sv, "n"sv}),
              "search index=\"main\"\n| fields user, n");
  CHECK_EQUAL(spl(main_index(), {}, std::vector{R"(this["a b"])"sv}),
              "search index=\"main\"\n| fields \"a b\"");
  CHECK_EQUAL(spl(main_index(), {}, std::vector{R"(this["a*"])"sv}),
              "search index=\"main\"\n| fields *");
  CHECK_EQUAL(spl(main_index(), {}, std::vector<std::string_view>{}),
              "search index=\"main\"\n| fields *");
  // A selection keeps its own fields.
  CHECK_EQUAL(spl(main_index(), {select({{{"a"}, {"a"}}})}, std::vector{"a"sv}),
              "search index=\"main\"\n| fields a");
}

TEST("spl narrows the time window of indexes") {
  auto window = [](SplSource const& source,
                   std::vector<ir::QueryStage> stages) -> TimeWindow {
    auto query = plan_spl_query(source, stages, None{});
    REQUIRE(query);
    return query->window;
  };
  auto const lower
    = window(main_index(), {filter("_time >= 2025-01-01T00:01:00Z and n > 1"),
                            filter("_time < 2025-01-01T00:02:00Z"),
                            filter("_time > 2025-01-01T00:00:30Z")});
  REQUIRE(lower.begin);
  REQUIRE(lower.end);
  CHECK(*lower.begin == at("2025-01-01T00:01:00Z"));
  CHECK(*lower.end == at("2025-01-01T00:02:00Z") - duration{1});
  // A disjunction encloses its operands, if they all bound a side.
  auto enclosed
    = window(main_index(), {filter("(_time >= 2025-01-01T00:01:00Z and _time < "
                                   "2025-01-01T00:02:00Z) or (_time >= "
                                   "2025-01-01T00:03:00Z and _time < "
                                   "2025-01-01T00:04:00Z)")});
  REQUIRE(enclosed.begin);
  REQUIRE(enclosed.end);
  CHECK(*enclosed.begin == at("2025-01-01T00:01:00Z"));
  CHECK(*enclosed.end == at("2025-01-01T00:04:00Z") - duration{1});
  // A disjunction with an unbounded operand tells nothing, and neither does a
  // filter after a limit.
  auto none
    = window(main_index(), {filter("_time >= 2025-01-01T00:01:00Z or n > 1"),
                            limit(5), filter("_time < 2025-01-01T00:02:00Z")});
  CHECK(not none.begin);
  CHECK(not none.end);
  // A selection keeps the provenance of `_time`, but a field renamed to
  // `_time` is another field.
  auto kept = window(main_index(), {select({{{"_time"}, {"_time"}}}),
                                    filter("_time >= 2025-01-01T00:01:00Z")});
  CHECK(kept.begin);
  CHECK_EQUAL(spl(main_index(), {select({{{"_time"}, {"x"}}}),
                                 filter("_time >= 2025-01-01T00:01:00Z")}),
              "");
  // Only `_time` holds epoch seconds, so other fields do not compare with
  // times.
  CHECK_EQUAL(spl(main_index(), {select({{{"t"}, {"_time"}}}),
                                 filter("t >= 2025-01-01T00:01:00Z")}),
              "");
  // A time literal compares only with Splunk's own `_time`, not with another
  // field that a stage made `_time`, nor after a drop of `_time`.
  CHECK_EQUAL(spl(main_index(), {select({{{"_time"}, {"event_time"}}}),
                                 filter("_time >= 2025-01-01T00:01:00Z")}),
              "");
  CHECK_EQUAL(spl(user("index=main"),
                  {select({{{"_time"}, {"event_time"}}}),
                   filter("_time >= 2025-01-01T00:01:00Z")}),
              "");
  CHECK_EQUAL(spl(main_index(),
                  {drop({{"_time"}}), filter("_time >= 2025-01-01T00:01:00Z")}),
              "");
  CHECK(not spl(main_index(), {select({{{"_time"}, {"_time"}}}),
                               filter("_time >= 2025-01-01T00:01:00Z")})
              .empty());
  // Neither an identity selection nor a summary brings back the provenance
  // that an earlier stage ended.
  CHECK_EQUAL(spl(main_index(), {select({{{"_time"}, {"event_time"}}}),
                                 select({{{"_time"}, {"_time"}}}),
                                 filter("_time >= 2025-01-01T00:01:00Z")}),
              "");
  CHECK_EQUAL(spl(main_index(), {select({{{"_time"}, {"event_time"}}}),
                                 ir::QueryStage{ir::QueryStage::Aggregate{
                                   .columns = {{.output = {"_time"},
                                                .function = "min",
                                                .args = {field_at({"_time"})}}},
                                   .schema_name = "tenzir.summarize"}},
                                 filter("_time >= 2025-01-01T00:01:00Z")}),
              "");
  // A command after the first one of a user's search may redefine `_time`,
  // also without naming it, while a plain search keeps Splunk's own, also
  // with a subsearch.
  for (auto search : {"index=main | rename event_time AS _time",
                      "index=main | rename event* AS _*", "| makeresults",
                      "index=main `macro`"}) {
    CHECK_EQUAL(spl(user(search), {filter("_time >= 2025-01-01T00:01:00Z")}),
                "");
  }
  for (auto search : {"index=main", "search index=main x=1",
                      "index=main [search index=other | fields host]"}) {
    CHECK(
      not spl(user(search), {filter("_time >= 2025-01-01T00:01:00Z")}).empty());
  }
  // A list of times, also one that the optimizer folds into a constant, tests
  // the membership of `_time` only.
  auto folded = [](std::string_view field) {
    auto list = tenzir::list{data{at("2025-01-01T00:00:00Z")}};
    return ir::QueryStage{ir::QueryStage::Filter{ast::binary_expr{
      parse(field), ast::binary_op::in,
      ast::constant::make(located{data{std::move(list)}, location::unknown})}}};
  };
  CHECK_EQUAL(spl(main_index(), {folded("ts")}), "");
  CHECK_EQUAL(where("ts in [2025-01-01T00:00:00Z]"), "");
  CHECK(not spl(main_index(), {folded("_time")}).empty());
  // A filter without a time literal still binds `_time` to its column.
  CHECK(not spl(main_index(), {select({{{"_time"}, {"event_time"}}}),
                               filter("_time != null")})
              .empty());
  // A user's search may assign `_time` itself.
  auto search
    = window(user("index=main"), {filter("_time >= 2025-01-01T00:01:00Z")});
  CHECK(not search.begin);
}

TEST("spl appends stages only to searches that accept them") {
  CHECK_EQUAL(spl(user("index=main"), {filter("n > 1"), limit(2)}),
              "search index=main\n| where ('n' > 1)\n| head 2\n| fields *");
  // A `head` could cut side effects short, and `fieldformat` changes what
  // later stages see.
  CHECK_EQUAL(spl(user("index=main | outputlookup x.csv"), {limit(2)}), "");
  CHECK_EQUAL(spl(user("index=main | outputlookup x.csv"), {filter("n > 1")}),
              "search index=main | outputlookup x.csv\n| where ('n' > 1)\n| "
              "fields *");
  CHECK_EQUAL(spl(user("index=main | fieldformat n=n*2"), {filter("n > 1")}),
              "");
  CHECK_EQUAL(spl(user("index=main | fieldformat n=n*2"), {}),
              "search index=main | fieldformat n=n*2\n| fields *");
  CHECK(spl_declines(user("index=main | fieldformat n=n*2")));
  CHECK(spl_declines(user("index=main |")));
  CHECK(not spl_declines(user("index=main")));
  CHECK(not spl_declines(main_index()));
  // A search that does not accept stages goes out as it is.
  CHECK_EQUAL(spl(user("search index=main |"), {}), "search index=main |");
  CHECK_EQUAL(spl(user("search index=main |"), {filter("n > 1")}), "");
}

TEST("spl sources take over the longest prefix that SPL spells") {
  auto const absorb
    = [](SplSource const& source, std::vector<ir::QueryStage> stages) {
        return pushdown::absorb(ir::QueryChain{.stages = std::move(stages),
                                               .end = ""},
                                spl_planner(source, None{}, None{}));
      };
  auto all = absorb(main_index(), {filter("n > 1"), sort({"n"}), limit(3)});
  CHECK_EQUAL(all.prefix, size_t{3});
  auto partial = absorb(main_index(),
                        {filter("n > 1"), filter("ts > 2025-01-01"), limit(3)});
  CHECK_EQUAL(partial.prefix, size_t{1});
  CHECK_EQUAL(partial.declined, "Splunk cannot spell the stage"s);
}

TEST("spl redacts searches for logs") {
  CHECK_EQUAL(
    redact_query(R"(search user="a\"b" ```note "x"``` | where 'f'==1)",
                 spl_lexicon),
    R"(search user="***" ``` *** ``` | where '***'==1)");
  CHECK_EQUAL(redact_query("search ```unterminated", spl_lexicon),
              "search ``` *** ```");
}

TEST("spl lexes searches before appending stages") {
  for (auto search : {
         "search index=main",
         "| makeresults count=2",
         R"(search user="a|b" | eval x="]")",
         "search index=main ```trailing | comment```",
         "search [search index=other | fields user] | stats count",
         "search index=main | eval 'x y'=1",
       }) {
    auto stages = spl_stages(search);
    CHECK(stages.accepted);
    CHECK(stages.limitable);
    CHECK(not stages.formatted);
  }
  for (auto search : {
         "",
         "   ",
         "```only a comment```",
         "search index=main |",
         "search index=main | ```comment```",
         R"(search user="unterminated)",
         "search index=main ```unterminated",
         "search [search index=other",
         "search index=main ]",
         "search `unterminated",
         "search user=O'Brien",
       }) {
    CHECK(not spl_stages(search).accepted);
  }
  // A plain search is a single `search` command without macros, which keeps
  // Splunk's own fields, such as `_time`.
  for (auto search : {"search index=main", R"(search user="a|b")",
                      "search index=main [search index=other | fields user]",
                      "search index=main ```a | comment```"}) {
    CHECK(spl_stages(search).plain);
  }
  for (auto search : {"| makeresults count=2", "search index=main | eval x=1",
                      "search index=main `macro`"}) {
    CHECK(not spl_stages(search).plain);
  }
  for (auto search : {
         "search index=main | outputlookup x.csv",
         "search index=main | collect index=summary",
         "search index=main | sendemail to=a@example.com",
         "search index=main | `my_macro`",
         "`my_macro`",
       }) {
    auto stages = spl_stages(search);
    CHECK(stages.accepted);
    CHECK(not stages.limitable);
  }
  for (auto search : {
         "search index=main | fieldformat n=n*2",
         "search index=main | FIELDFORMAT n=n*2",
         "search index=main | `my_macro`",
       }) {
    CHECK(spl_stages(search).formatted);
  }
  CHECK(not spl_stages("search index=main | eval n=n*2").formatted);
}

TEST("spl prepends the search command where Splunk Web does") {
  CHECK_EQUAL(spl_search_command("index=main"), "search index=main");
  CHECK_EQUAL(spl_search_command("  index=main | stats count"),
              "search   index=main | stats count");
  CHECK_EQUAL(spl_search_command("searchterm"), "search searchterm");
  CHECK_EQUAL(spl_search_command("search index=main"), "search index=main");
  CHECK_EQUAL(spl_search_command("SEARCH index=main"), "SEARCH index=main");
  CHECK_EQUAL(spl_search_command("search"), "search");
  CHECK_EQUAL(spl_search_command("| makeresults"), "| makeresults");
  CHECK_EQUAL(spl_search_command("```note``` | makeresults"),
              "```note``` | makeresults");
  CHECK_EQUAL(spl_search_command("```note``` index=main"),
              "search ```note``` index=main");
}

TEST("spl generates searches for indexes") {
  auto const main = std::vector<std::string>{"main"};
  CHECK_EQUAL(spl_index_search(main), "search index=\"main\"");
  auto const two = std::vector<std::string>{"main", "web_*"};
  CHECK_EQUAL(spl_index_search(two), R"(search index IN ("main", "web_*"))");
  auto empty = std::vector<std::string>{""};
  CHECK(not spl_index_search(empty));
  auto control = std::vector<std::string>{"a\nb"};
  CHECK(not spl_index_search(control));
}

// The spellings of SPL2 below compile and run on Splunk Enterprise 10.4.3.
TEST("spl2 appends the stages in the syntax of SPL2") {
  // Without a projection, SPL2 returns every field, and has no `fields *`.
  CHECK_EQUAL(spl(spl2("from main"), {}), "from main");
  CHECK_EQUAL(spl(spl2("from main"), {filter(R"(user == "a" and x != null)")}),
              "from main\n| where (('user' == \"a\") AND isnotnull('x'))");
  CHECK_EQUAL(spl(spl2("from main"), {filter("n in [1, 2]")}),
              "from main\n| where in('n', 1, 2)");
  CHECK_EQUAL(spl(spl2("from main"), {filter(R"(user.starts_with("al"))")}),
              "from main\n| where (substr('user', 1, len(\"al\"))==\"al\")");
  // Commands name fields in single quotes, as expressions do.
  CHECK_EQUAL(spl(spl2("from main"), {},
                  std::vector{"user"sv, R"(this["a b"])"sv}),
              "from main\n| fields 'user', 'a b'");
  CHECK_EQUAL(spl(spl2("from main"),
                  {select({{{"u"}, {"user"}}, {{"t"}, {"tags{}"}}})}),
              "from main\n| fields 'user', 'tags{}'");
  CHECK_EQUAL(spl(spl2("from main"), {select({{{"u"}, {"a|b"}}})}), "");
  CHECK_EQUAL(spl(spl2("from main"), {drop({{"a"}, {"b c"}})}),
              "from main\n| fields - 'a', 'b c'");
  CHECK_EQUAL(spl(spl2("from main"), {sort({"-n", "user"}, 3), limit(3)}),
              "from main\n| sort 3 -'n', 'user'\n| head 3");
  // `stats` calls `count` with parentheses, also for the sentinel row.
  auto summary = ir::QueryStage{ir::QueryStage::Aggregate{
    .columns
    = {{.output = {"user"}, .function = None{}, .args = {field_at({"user"})}},
       {.output = {"n"}, .function = "count", .args = {}}},
    .schema_name = "tenzir.summarize"}};
  auto query = plan_spl_query(spl2("from main"), std::vector{summary}, None{});
  REQUIRE(query);
  CHECK_EQUAL(query->search,
              "from main\n| stats count() as tenzir_column_0 by 'user'\n| "
              "appendpipe [stats count() as tenzir_groups_0]");
  CHECK_EQUAL(query->sentinel, Option<std::string>{"tenzir_groups_0"});
  // The clauses of `from` may redefine `_time`, which then holds no time.
  CHECK_EQUAL(spl(spl2("from main"), {filter("_time > 2025-01-01T00:01:00Z")}),
              "");
}

TEST("spl2 lexes searches before appending stages") {
  for (auto search : {
         "from main",
         "from main where user == \"a|b\" // a | comment",
         "from main /* a | comment */ | where x == 1",
         R"(from main | where path == @"C:\" | where 'it\'s' == 1)",
         R"(from main | where x == @"a""b" | where `x` == 1)",
       }) {
    auto stages = spl_stages(search, true);
    CHECK(stages.accepted);
    CHECK(stages.limitable);
    CHECK(not stages.formatted);
    CHECK(not stages.plain);
  }
  for (auto search : {
         "",
         "// only a comment",
         "from main |",
         "from main;",
         "$a = from main; $main = from $a",
         "from main /* unterminated",
         R"(from main | where x == @"a"")",
         R"(from main | where 'it\'s == 1)",
         "from main | where x IN [1, 2",
       }) {
    CHECK(not spl_stages(search, true).accepted);
  }
  // SPL2 searches go out as they are, without the `search` command.
  CHECK_EQUAL(spl(spl2("| from main"), {limit(1)}), "| from main\n| head 1");
  CHECK_EQUAL(spl(spl2("from main;"), {}), "from main;");
  CHECK_EQUAL(spl(spl2("from main;"), {limit(1)}), "");
}

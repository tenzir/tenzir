//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/diagnostics.hpp"
#include "tenzir/pushdown/splunk.hpp"
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

namespace {

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

auto filters(std::vector<std::string_view> sources) -> ir::OptimizeFilter {
  auto result = ir::OptimizeFilter{};
  for (auto source : sources) {
    result.push_back(parse(source));
  }
  return result;
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

auto plan(std::string_view search, std::vector<std::string_view> predicates,
          Option<std::vector<std::string_view>> projection = None{},
          Option<uint64_t> limit = None{}) -> PipePlan {
  auto projected = Option<ir::OptimizeProjection>{};
  if (projection) {
    projected = paths(*projection);
  }
  return plan_spl_search(search, filters(predicates), projected, limit,
                         test::builtin_registry());
}

/// The `search` stage for a single predicate, or the empty string if nothing
/// was pushed.
auto search(std::string_view predicate) -> std::string {
  auto planned = plan("S", {predicate});
  if (not planned.filtered) {
    return {};
  }
  constexpr auto prefix = std::string_view{"S\n| search "};
  constexpr auto suffix = std::string_view{"\n| fields *"};
  REQUIRE(planned.query.starts_with(prefix));
  REQUIRE(planned.query.ends_with(suffix));
  return planned.query.substr(prefix.size(), planned.query.size()
                                               - prefix.size() - suffix.size());
}

} // namespace

TEST("spl renders number comparisons") {
  CHECK_EQUAL(search("n > 6"), "n>6");
  CHECK_EQUAL(search("n >= -1.5"), "n>=-1.5");
  CHECK_EQUAL(search("n < 0.25"), "n<0.25");
  CHECK_EQUAL(search("n <= 7"), "n<=7");
  CHECK_EQUAL(search("n == 7"), "n=7");
  CHECK_EQUAL(search("6 < n"), "n>6");
  // Exponents might read as text.
  CHECK_EQUAL(search("n > 0.0000001"), "");
}

TEST("spl renders string and boolean equality") {
  CHECK_EQUAL(search(R"(user == "alice")"), R"(user="alice")");
  CHECK_EQUAL(search(R"(user == "a\"b\\c")"), R"(user="a\"b\\c")");
  CHECK_EQUAL(search(R"(user == "")"), R"(user="")");
  CHECK_EQUAL(search("flag == true"), "flag=true");
  CHECK_EQUAL(search("flag"), "flag=true");
  // Wildcards in values only widen the match.
  CHECK_EQUAL(search(R"(user == "a*")"), R"(user="a*")");
  // Control characters stay local.
  CHECK_EQUAL(search("user == \"a\\nb\""), "");
  // Ordering on strings stays local.
  CHECK_EQUAL(search(R"(user < "b")"), "");
  // `!=` must keep missing fields, which `search` cannot prove.
  CHECK_EQUAL(search(R"(user != "alice")"), "");
  CHECK_EQUAL(search("n != 5"), "");
}

TEST("spl quotes field names that are not identifiers") {
  CHECK_EQUAL(search(R"(this["weird field"] == "x")"), R"("weird field"="x")");
  CHECK_EQUAL(search(R"(this["obj.k"] == "x")"), R"("obj.k"="x")");
  CHECK_EQUAL(search(R"(this["tags{}"] == "x")"), R"("tags{}"="x")");
  CHECK_EQUAL(search(R"(this["ümlaut"] == "x")"), R"("ümlaut"="x")");
  CHECK_EQUAL(search(R"(this["OR"] == "x")"), R"("OR"="x")");
  CHECK_EQUAL(search(R"(_time_ms == 1)"), "_time_ms=1");
  // Names that a quoted field name cannot hold stay local.
  CHECK_EQUAL(search(R"(this["a*"] == "x")"), "");
  CHECK_EQUAL(search(R"(this["a\"b"] == "x")"), "");
  // Splunk field names are flat.
  CHECK_EQUAL(search(R"(obj.k == "x")"), "");
}

TEST("spl renders membership") {
  CHECK_EQUAL(search(R"(user in ["alice", "bob"])"),
              R"(user IN ("alice", "bob"))");
  CHECK_EQUAL(search("n in [5, 7]"), "n IN (5, 7)");
}

TEST("spl renders string relations as wildcard patterns") {
  CHECK_EQUAL(search(R"(user.starts_with("al"))"), R"(user="al*")");
  CHECK_EQUAL(search(R"(user.ends_with("ce"))"), R"(user="*ce")");
  CHECK_EQUAL(search(R"("lic" in user)"), R"(user="*lic*")");
  CHECK_EQUAL(search(R"(user.starts_with("AL", ignore_case=true))"),
              R"(user="AL*")");
}

TEST("spl renders null checks") {
  CHECK_EQUAL(search("action != null"), "action=*");
  CHECK_EQUAL(search("action == null"), "");
}

TEST("spl renders _time comparisons in whole seconds") {
  CHECK_EQUAL(search("_time >= 2025-01-01T00:01:00Z"), "_time>=1735689660");
  CHECK_EQUAL(search("_time > 2025-01-01T00:01:00.5Z"), "_time>=1735689660");
  CHECK_EQUAL(search("_time < 2025-01-01T00:01:00.5Z"), "_time<=1735689661");
  CHECK_EQUAL(search("_time == 2025-01-01T00:01:00.5Z"),
              "(_time>=1735689660 _time<=1735689661)");
  // Other fields hold timestamps as text.
  CHECK_EQUAL(search("ts >= 2025-01-01T00:01:00Z"), "");
}

TEST("spl renders junctions") {
  CHECK_EQUAL(search(R"(n > 6 or user == "bob")"), R"((n>6 OR user="bob"))");
  // `and` drops operands that do not render.
  CHECK_EQUAL(search(R"((n > 6 or user == "bob") and (n > 1 and user != "x"))"),
              R"((n>6 OR user="bob") n>1)");
  // `or` needs every operand.
  CHECK_EQUAL(search(R"(n > 6 or user != "bob")"), "");
  CHECK_EQUAL(search("not (n > 6)"), "");
}

TEST("spl appends projection and limit stages") {
  CHECK_EQUAL(plan("S", {}).query, "S\n| fields *");
  CHECK_EQUAL(plan("S", {"n > 6"}, std::vector<std::string_view>{"user"}).query,
              "S\n| search n>6\n| fields user, n");
  CHECK_EQUAL(
    plan("S", {}, std::vector<std::string_view>{"this[\"weird field\"]"}).query,
    "S\n| fields \"weird field\"");
  CHECK_EQUAL(plan("S", {}, None{}, 10).query, "S\n| fields *\n| head 10");
  // A remaining predicate runs after the limit, so the limit stays local.
  CHECK_EQUAL(plan("S", {"n > 6"}, None{}, 10).query,
              "S\n| search n>6\n| fields *");
}

TEST("spl keeps searches with side effects complete") {
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
    CHECK_EQUAL(plan(search, {}, None{}, 10).query,
                fmt::format("{}\n| fields *", search));
  }
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
    CHECK_EQUAL(plan(search, {"n > 6"}).query, search);
  }
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
  auto plan_indexes
    = [](std::vector<std::string> indexes,
         std::vector<std::string_view> predicates,
         Option<std::vector<std::string_view>> projection = None{},
         Option<uint64_t> limit = None{}) -> std::string {
    auto projected = Option<ir::OptimizeProjection>{};
    if (projection) {
      projected = paths(*projection);
    }
    auto planned = plan_spl_indexes(indexes, filters(predicates), projected,
                                    limit, test::builtin_registry());
    REQUIRE(planned);
    return planned->query;
  };
  CHECK_EQUAL(plan_indexes({"main"}, {}), "search index=\"main\"\n| fields *");
  CHECK_EQUAL(plan_indexes({"main", "web_*"}, {}),
              "search index IN (\"main\", \"web_*\")\n| fields *");
  // Prefilters extend the base search.
  CHECK_EQUAL(plan_indexes({"main"},
                           {R"(sourcetype == "linux_secure")", "bytes > 1024"},
                           std::vector<std::string_view>{"user"}),
              "search index=\"main\" sourcetype=\"linux_secure\" bytes>1024\n"
              "| fields user, sourcetype, bytes");
  CHECK_EQUAL(plan_indexes({"main"}, {}, None{}, 5),
              "search index=\"main\"\n| fields *\n| head 5");
  auto empty = std::vector<std::string>{""};
  CHECK(not spl_index_search(empty));
  auto control = std::vector<std::string>{"a\nb"};
  CHECK(not spl_index_search(control));
}

TEST("spl keeps non-time comparisons of _time local") {
  // Splunk holds `_time` as epoch seconds, not as the exported string.
  CHECK_EQUAL(search(R"(_time == "2025-01-01")"), "");
  CHECK_EQUAL(search(R"(_time.starts_with("2025-01-01"))"), "");
  CHECK_EQUAL(search(R"(_time in ["2025-01-01"])"), "");
  CHECK_EQUAL(search("_time > 5"), "");
  CHECK_EQUAL(search("_time != null"), "_time=*");
}

TEST("spl keeps fields named like search modifiers local") {
  // `earliest="x"` would set the time range of the search.
  CHECK_EQUAL(search(R"(earliest == "not-a-time")"), "");
  CHECK_EQUAL(search(R"(this["LATEST"] == "x")"), "");
  CHECK_EQUAL(search("starttime > 5"), "");
  CHECK_EQUAL(search("earliest != null"), "");
  // Projections may still name them.
  CHECK_EQUAL(plan("S", {}, std::vector<std::string_view>{"earliest"}).query,
              "S\n| fields earliest");
}

TEST("spl keeps _time comparisons local after searches that mention _time") {
  // The search may assign `_time` a formatted string.
  auto planned = plan(R"(index=main | eval _time=strftime(_time, "%FT%T"))",
                      {"_time >= 2025-01-01T00:01:00Z", "n > 6"});
  CHECK_EQUAL(planned.query,
              "index=main | eval _time=strftime(_time, \"%FT%T\")\n"
              "| search n>6\n| fields *");
  auto indexes = std::vector<std::string>{"main"};
  auto generated
    = plan_spl_indexes(indexes, filters({"_time >= 2025-01-01T00:01:00Z"}),
                       None{}, None{}, test::builtin_registry());
  REQUIRE(generated);
  CHECK_EQUAL(generated->query,
              "search index=\"main\" _time>=1735689660\n| fields *");
}

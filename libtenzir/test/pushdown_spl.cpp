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
          Option<uint64_t> limit = None{}, bool inference = true) -> PipePlan {
  auto projected = Option<ir::OptimizeProjection>{};
  if (projection) {
    projected = paths(*projection);
  }
  return plan_spl_search(search, filters(predicates), projected, limit,
                         inference, test::builtin_registry());
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
  // `!=` goes into a `where` stage; see the negation test.
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
  CHECK_EQUAL(
    search(R"((n > 6 or user == "bob") and (n > 1 and user.match_regex("x")))"),
    R"((n>6 OR user="bob") n>1)");
  // `or` needs every operand.
  CHECK_EQUAL(search(R"(n > 6 or user.match_regex("^b"))"), "");
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

TEST("spl keeps predicates local after fieldformat") {
  // Results show the formatted values, but `search` compares the original
  // ones, so a prefilter could drop results that the predicate keeps. The
  // projection and the limit still apply.
  for (auto search : {
         "search index=main | fieldformat n=n*2",
         "search index=main | FIELDFORMAT n=n*2",
         "search index=main | `my_macro`",
       }) {
    auto stages = spl_stages(search);
    CHECK(stages.accepted);
    CHECK(stages.formatted);
    CHECK_EQUAL(
      plan(search, {"n > 10"}, std::vector<std::string_view>{"n"}).query,
      fmt::format("{}\n| fields n", search));
    // Negations would compare the original values in a `where` stage, and
    // address terms in the `search` stage.
    CHECK_EQUAL(plan(search,
                     {R"(user != "root")", "not (n in [1, 2])",
                      "src == 10.0.0.1", "src in 10.0.0.0/8"},
                     std::vector<std::string_view>{"user"})
                  .query,
                fmt::format("{}\n| fields user, n, src", search));
  }
  // The same predicates go into the search without `fieldformat`.
  CHECK_EQUAL(
    plan("search index=main", {R"(user != "root")", "src in 10.0.0.0/8"},
         std::vector<std::string_view>{"user"})
      .query,
    "search index=main\n| search (src=\"10.0.0.0/8\" OR "
    "src=\"::ffff:10.0.0.0/104\")\n| where (isnull(user) OR mvcount(user)>1 OR "
    "if(user==\"root\", false(), true()))\n| fields user, src");
  CHECK_EQUAL(
    plan("search index=main | fieldformat n=n*2", {}, None{}, 10).query,
    "search index=main | fieldformat n=n*2\n| fields *\n| head 10");
  CHECK(not spl_stages("search index=main | eval n=n*2").formatted);
}

TEST("spl tells whether the results keep a field") {
  for (auto search : {
         "search index=main",
         "| makeresults",
         "search index=main | eval x=1 | fields + x | where x > 0",
         // Splunk returns results whose fields `eval` set to `null()` as
         // empty objects.
         "| makeresults | eval _time=null()",
         "search index=main | fields x | eval _raw=null(), _time=null()",
         "| makeresults count=2 annotate=t splunk_server=local | fields x",
         "| makeresults COUNT=2 splunk_server_group=* | fields x",
         R"(| makeresults splunk_server="format=csv" | fields x)",
         "| makeresults | eval format=1, data=2 | fields x",
       }) {
    CHECK(spl_stages(search).keeps_fields);
  }
  for (auto search : {
         "| makeresults | fields - _time",
         "| makeresults | FIELDS -_time",
         // Results with only the fields that `data` lists, but no `_time`.
         "| makeresults format=csv data=\"x\n1\" | fields y",
         "| makeresults FORMAT=csv DATA=\"x\n1\"",
         "| makeresults \"format\"=csv \"data\"=\"x\n1\"",
         // Options that the lexer does not tell apart from these: Splunk
         // ignores the first two, and `format=json` keeps `_time`.
         "| makeresults count=1 format = csv data = \"x\n1\"",
         "| makeresults 'format'=csv 'data'=\"x\n1\"",
         "| makeresults format=json data=\"[{}]\"",
         "search index=main | table x",
         "search index=main | rename _raw as __raw",
         "search index=main | chart count by x",
         "| rest /services/server/info",
         "search index=main | `my_macro`",
       }) {
    CHECK(not spl_stages(search).keeps_fields);
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
                                    limit, true, test::builtin_registry());
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
                       None{}, None{}, true, test::builtin_registry());
  REQUIRE(generated);
  CHECK_EQUAL(generated->query,
              "search index=\"main\" _time>=1735689660\n| fields *");
}

/// The appended stages for a single predicate, without `fields *`.
auto stages(std::string_view predicate, bool inference = true) -> std::string {
  auto planned = plan("S", {predicate}, None{}, None{}, inference);
  constexpr auto suffix = std::string_view{"\n| fields *"};
  REQUIRE(planned.query.starts_with("S"));
  REQUIRE(planned.query.ends_with(suffix));
  return planned.query.substr(1, planned.query.size() - 1 - suffix.size());
}

/// The `where` expression of a negation of `field` with `values`.
auto negated_where(std::string_view field, std::vector<std::string_view> values)
  -> std::string {
  auto equalities = std::vector<std::string>{};
  for (auto value : values) {
    equalities.push_back(fmt::format("{}=={}", field, value));
  }
  return fmt::format("(isnull({0}) OR mvcount({0})>1 OR if({1}, false(), "
                     "true()))",
                     field, fmt::join(equalities, " OR "));
}

auto where(std::string_view expression) -> std::string {
  return fmt::format("\n| where {}", expression);
}

TEST("spl renders negations that keep missing and multivalue fields") {
  CHECK_EQUAL(stages(R"(user != "alice")"),
              where(negated_where("user", {R"("alice")"})));
  CHECK_EQUAL(stages(R"("alice" != user)"),
              where(negated_where("user", {R"("alice")"})));
  CHECK_EQUAL(stages(R"(not (user == "alice"))"),
              where(negated_where("user", {R"("alice")"})));
  CHECK_EQUAL(stages(R"(not (user in ["alice", "bob"]))"),
              where(negated_where("user", {R"("alice")", R"("bob")"})));
  CHECK_EQUAL(stages("n != 5"), where(negated_where("n", {"5"})));
  CHECK_EQUAL(stages("not (n in [5, 7])"),
              where(negated_where("n", {"5", "7"})));
  CHECK_EQUAL(stages("flag != true"),
              where(negated_where("flag", {R"("true")"})));
  CHECK_EQUAL(stages(R"(this["tags{}"] != "x")"),
              where(negated_where("'tags{}'", {R"("x")"})));
  CHECK_EQUAL(stages(R"(this["OR"] != "x")"),
              where(negated_where("'OR'", {R"("x")"})));
  // `eval` compares exactly, so wildcards, empty strings, and whitespace work.
  CHECK_EQUAL(stages(R"(x != "a*b")"), where(negated_where("x", {R"("a*b")"})));
  CHECK_EQUAL(stages(R"(x != "")"), where(negated_where("x", {R"("")"})));
  CHECK_EQUAL(stages(R"(x != " a")"), where(negated_where("x", {R"(" a")"})));
  // Negations combine with `search` terms in separate stages.
  CHECK_EQUAL(stages(R"(user != "alice" and n > 6)"),
              "\n| search n>6" + where(negated_where("user", {R"("alice")"})));
  CHECK_EQUAL(plan("S", {R"(user != "alice")", "n != 5"}).query,
              "S"
                + where(negated_where("user", {R"("alice")"}) + " AND "
                        + negated_where("n", {"5"}))
                + "\n| fields *");
  // A disjunction cannot mix stages, and a conjunction drops expressions.
  CHECK_EQUAL(stages(R"(user != "alice" or n > 6)"), "");
  CHECK_EQUAL(stages(R"(user != "alice" or n != 5)"),
              where("(" + negated_where("user", {R"("alice")"}) + " OR "
                    + negated_where("n", {"5"}) + ")"));
  CHECK_EQUAL(stages(R"((user != "alice" and n > 6) or n > 9)"),
              "\n| search (n>6 OR n>9)");
  // A string that inference turns into another type differs from the value
  // that TQL sees, which negating the match would drop.
  for (auto value : {"5", "-1.5", "true", "false", "192.0.2.1", "10.0.0.0/8",
                     "5m", "2025-01-01T00:00:00Z", "now", "inf", "nan"}) {
    CHECK_EQUAL(stages(fmt::format(R"(x != "{}")", value)), "");
  }
  // Escaping of quotes and backslashes in `eval` is not verified.
  for (auto value : {R"(a\"b)", R"(a\\b)"}) {
    CHECK_EQUAL(stages(fmt::format(R"(x != "{}")", value)), "");
  }
  // Other negations, `_time`, search modifiers, and nested paths stay local.
  CHECK_EQUAL(stages("not (n > 6)"), "");
  CHECK_EQUAL(stages(R"(not user.starts_with("a"))"), "");
  CHECK_EQUAL(stages(R"(_time != "x")"), "");
  CHECK_EQUAL(stages(R"(earliest != "x")"), "");
  CHECK_EQUAL(stages("_time != 2025-01-01"), "");
  CHECK_EQUAL(stages(R"(obj.k != "x")"), "");
  CHECK_EQUAL(stages(R"(this["it's"] != "x")"), "");
  // Names that `eval` cannot read stay local: `|` and brackets fail the
  // search, and a backslash or `::` reads another field.
  for (auto name : {"a|b", "a[b]", "a]b", R"(a\\b)", "tag::x", " a", "a "}) {
    CHECK_EQUAL(stages(fmt::format(R"(this["{}"] != "x")", name)), "");
  }
  CHECK_EQUAL(stages(R"(this["a b"] != "x")"),
              where(negated_where("'a b'", {R"("x")"})));
  CHECK_EQUAL(stages(R"(this["a-b.c/d@e"] != "x")"),
              where(negated_where("'a-b.c/d@e'", {R"("x")"})));
  // Presence checks are unchanged.
  CHECK_EQUAL(stages("action != null"), "\n| search action=*");
  // The numeric boundary of exact doubles stays local, also in lists.
  CHECK_EQUAL(stages("n != 9007199254740992"), "");
  CHECK_EQUAL(stages("n != -9007199254740992"), "");
  CHECK_EQUAL(stages("not (n in [1, 9007199254740992])"), "");
  CHECK_EQUAL(stages("not (n in [-9007199254740992])"), "");
  CHECK_EQUAL(stages("not (n in [uint(9007199254740992)])"), "");
  // Nova keeps mixed lists as they are, so `true in [1, true]` holds and the
  // negation matches each literal by its own kind.
  CHECK_EQUAL(stages("not (x in [1, true])"),
              where(negated_where("x", {"1", R"("true")"})));
  CHECK_EQUAL(stages("not (n in [9007199254740991])"),
              where(negated_where("n", {"9007199254740991"})));
}

TEST("spl negates only strings without type inference") {
  // Without inference, `"5"` stays a string, which differs from `5`.
  CHECK_EQUAL(stages("n != 5", false), "");
  CHECK_EQUAL(stages("flag != true", false), "");
  CHECK_EQUAL(stages(R"(x != "5")", false),
              where(negated_where("x", {R"("5")"})));
  // Positive comparisons are unaffected.
  CHECK_EQUAL(stages("n > 6", false), "\n| search n>6");
}

TEST("spl appends negations after the base search of indexes") {
  auto indexes = std::vector<std::string>{"main"};
  auto planned
    = plan_spl_indexes(indexes, filters({R"(user != "alice")", "n > 6"}),
                       None{}, None{}, true, test::builtin_registry());
  REQUIRE(planned);
  CHECK_EQUAL(planned->query, "search index=\"main\" n>6"
                                + where(negated_where("user", {R"("alice")"}))
                                + "\n| fields *");
}

TEST("spl renders addresses and networks") {
  // Single-host networks match every spelling of an address.
  CHECK_EQUAL(search("src == 192.0.2.10"),
              R"((src="192.0.2.10/32" OR src="::ffff:192.0.2.10/128"))");
  CHECK_EQUAL(search("src.ip() == 192.0.2.10"),
              R"((src="192.0.2.10/32" OR src="::ffff:192.0.2.10/128"))");
  CHECK_EQUAL(search("src == 2001:db8::1"), R"(src="2001:db8::1/128")");
  CHECK_EQUAL(search("src in 192.0.2.0/24"),
              R"((src="192.0.2.0/24" OR src="::ffff:192.0.2.0/120"))");
  CHECK_EQUAL(search("src in 2001:db8::/32"), R"(src="2001:db8::/32")");
  CHECK_EQUAL(search("src in [192.0.2.10, 2001:db8::1]"),
              R"((src="192.0.2.10/32" OR src="::ffff:192.0.2.10/128" OR )"
              R"(src="2001:db8::1/128"))");
  // Splunk matches IPv4 networks only against IPv4 addresses and IPv6 ones
  // only against IPv6 addresses, so networks spanning both stay local.
  CHECK_EQUAL(search("src in ::/0"), "");
  CHECK_EQUAL(search("src in ::/64"), "");
  // Ordering and negation of addresses stay local.
  CHECK_EQUAL(search("src < 192.0.2.10"), "");
  CHECK_EQUAL(search("src != 192.0.2.10"), "");
}

//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/ir_lift.hpp"

#include "tenzir/diagnostics.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/session.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/parser.hpp"
#include "tenzir/tql2/resolve.hpp"

#include <fmt/format.h>

#include <string>
#include <string_view>
#include <vector>

using namespace tenzir;

namespace {

auto parse(std::string_view source) -> ast::expression {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto ctx = provider.as_session();
  auto expr
    = parse_expression_with_location_override(source, location::unknown, ctx);
  REQUIRE(expr);
  REQUIRE(resolve_entities(*expr, ctx));
  return std::move(*expr);
}

auto parse_chain(std::vector<std::string_view> const& sources)
  -> ir::OptimizeFilter {
  auto filter = ir::OptimizeFilter{};
  for (auto source : sources) {
    filter.push_back(parse(source));
  }
  return filter;
}

auto render_chain(ir::OptimizeFilter const& filter) -> std::string {
  auto result = std::string{};
  for (auto const& predicate : filter) {
    result += fmt::format("{:?}\n", predicate);
  }
  return result;
}

auto render(std::vector<std::string_view> const& sources) -> std::string {
  return render_chain(parse_chain(sources));
}

auto conjoin(std::vector<std::string_view> const& sources) -> std::string {
  return fmt::format("{:?}", ir::conjoin_filter(parse_chain(sources)));
}

struct Branch {
  Option<std::string_view> condition;
  std::vector<std::string_view> filter;
};

auto lift(std::vector<Branch> const& branches) -> std::string {
  auto routed = std::vector<ir::RoutedBranch>{};
  for (auto const& branch : branches) {
    routed.push_back({
      branch.condition.map(parse),
      parse_chain(branch.filter),
    });
  }
  auto result = ir::lift_branch_filters(std::move(routed));
  if (not result) {
    return "<none>";
  }
  return render_chain(*result);
}

} // namespace

TEST("conjoin filter chains") {
  CHECK_EQUAL(conjoin({}), fmt::format("{:?}", parse("true")));
  CHECK_EQUAL(conjoin({"x == 1"}), fmt::format("{:?}", parse("x == 1")));
  CHECK_EQUAL(conjoin({"x == 1", "y == 2", "z == 3"}),
              fmt::format("{:?}", parse("x == 1 and y == 2 and z == 3")));
  // A predicate that could fail sees only the events that the chain passes
  // to it.
  CHECK_EQUAL(conjoin({"x == 1", "y.length() == 2"}),
              fmt::format("{:?}", parse("y.length() == 2 if x == 1 else "
                                        "false")));
}

TEST("repeatable expressions") {
  CHECK(ir::is_repeatable(parse("x == 1 and y.length() > 2")));
  CHECK(ir::is_repeatable(parse("@name == \"foo\"")));
  CHECK(not ir::is_repeatable(parse("now() > x")));
  CHECK(not ir::is_repeatable(parse("random() < 0.5")));
  CHECK(not ir::is_repeatable(parse("(move x) == 1")));
  // An assignment in place of a value only warns.
  CHECK(not ir::is_repeatable(parse("(x = 42) else false")));
  CHECK(ir::is_repeatable(parse("x.slice(stride=-1) == \"\"")));
}

TEST("lift filters of two branches") {
  // A filter that both branches apply holds for every event.
  CHECK_EQUAL(lift({{"c", {"x == 1"}}, {None{}, {"x == 1"}}}),
              render({"x == 1"}));
  CHECK_EQUAL(lift({{"c", {"p", "a", "s"}}, {None{}, {"p", "b", "s"}}}),
              render({"p", "a if c else b", "s"}));
  CHECK_EQUAL(lift({{"c", {"a == 1"}}, {None{}, {"b == 1"}}}),
              render({"a == 1 if c else b == 1"}));
  // A branch without a filter keeps all of its events.
  CHECK_EQUAL(lift({{"x == 1", {}}, {None{}, {"b == 1"}}}),
              render({"x == 1 or b == 1"}));
  CHECK_EQUAL(lift({{"c", {"a == 1"}}, {None{}, {}}}),
              render({"a == 1 if c else true"}));
  // A branch that keeps no events contributes only its routing.
  CHECK_EQUAL(lift({{"x == 1", {"a == 1"}}, {None{}, {"false"}}}),
              render({"x == 1", "a == 1"}));
  CHECK_EQUAL(lift({{"c", {"false"}}, {None{}, {"b == 1"}}}),
              render({"false if c else b == 1"}));
  CHECK_EQUAL(lift({{"c", {"false"}}, {None{}, {"false"}}}), render({"false"}));
  // A filter that could fail stays behind its condition.
  CHECK_EQUAL(lift({{"x == 1", {"a.length() == 1"}}, {None{}, {"false"}}}),
              render({"x == 1", "a.length() == 1"}));
  CHECK_EQUAL(lift({{"c", {"a.length() == 1"}}, {None{}, {"b"}}}),
              render({"a.length() == 1 if c else b"}));
  // Constant conditions select a branch.
  CHECK_EQUAL(lift({{"true", {"a"}}, {None{}, {"b"}}}), render({"a"}));
  CHECK_EQUAL(lift({{"null", {"a"}}, {None{}, {"b"}}}), render({"b"}));
}

TEST("lift filters with conditions that may be null") {
  // The routing treats `null` as `false` without a warning, and so must the
  // filter, which warns for `null`.
  CHECK_EQUAL(lift({{"c", {}}, {None{}, {"b == 1"}}}),
              render({"(c else false) or b == 1"}));
  CHECK_EQUAL(lift({{"c and x == 1", {"a"}}, {None{}, {"false"}}}),
              render({"(c and x == 1) else false", "a"}));
  CHECK_EQUAL(lift({{"x > 1 and not (y == 2)", {}}, {None{}, {"b"}}}),
              render({"(x > 1 and not (y == 2)) or b"}));
  CHECK_EQUAL(lift({{"(x == 1 if c else false)", {}}, {None{}, {"b"}}}),
              render({"(x == 1 if c else false) or b"}));
  // The condition of an `if` treats `null` as `false`, too.
  CHECK_EQUAL(lift({{"c", {"a"}}, {None{}, {"b"}}}), render({"a if c else b"}));
}

TEST("lift filters of a single branch") {
  CHECK_EQUAL(lift({{None{}, {"a", "b"}}}), render({"a", "b"}));
}

TEST("lift filters of many branches") {
  // The first branch whose condition is `true` takes an event.
  CHECK_EQUAL(lift({{"c1", {"a"}}, {"c2", {"b"}}, {None{}, {"x"}}}),
              render({"a if c1 else (b if c2 else x)"}));
  // A branch with the filter of the last one need not route its events.
  CHECK_EQUAL(lift({{"c1", {"a"}}, {"c2", {"x"}}, {None{}, {"x"}}}),
              render({"a if c1 else x"}));
  CHECK_EQUAL(lift({{"c1", {"x"}}, {"c2", {"a"}}, {None{}, {"x"}}}),
              render({"x if c1 else (a if c2 else x)"}));
  // Branches without a filter keep the events of their conditions.
  CHECK_EQUAL(lift({{"e == 1", {}}, {"e == 2", {}}, {None{}, {"false"}}}),
              render({"e == 1 or e == 2"}));
}

TEST("lift filters common to the branches that keep events") {
  // A branch that keeps no event evaluates nothing after its routing.
  CHECK_EQUAL(
    lift({{"c1", {"false"}}, {"c2", {"a", "s"}}, {None{}, {"b", "s"}}}),
    render({"false if c1 else (a if c2 else b)", "s"}));
  CHECK_EQUAL(
    lift({{"c1", {"false"}}, {"c2", {"p", "a"}}, {None{}, {"p", "b"}}}),
    render({"p", "false if c1 else (a if c2 else b)"}));
  // Moving a predicate that could fail in front of the routing would evaluate
  // it for the events of the branch that keeps none.
  CHECK_EQUAL(lift({{"c1", {"false"}},
                    {"c2", {"p.length() == 1", "a"}},
                    {None{}, {"p.length() == 1", "b"}}}),
              render({"false if c1 else ((p.length() == 1 and a) if c2 else "
                      "(p.length() == 1 and b))"}));
}

TEST("lift filters with conditions that could fail") {
  // The result may skip a condition for the events that it drops, just like a
  // filter that moves past `set` skips its assignments for them. It still
  // evaluates a condition for every event that it keeps.
  CHECK_EQUAL(lift({{"x.length() == 1", {"p", "a"}}, {None{}, {"p", "b"}}}),
              render({"p", "a if x.length() == 1 else b"}));
  CHECK_EQUAL(lift({{"x.length() == 1", {"s"}}, {None{}, {"s"}}}),
              render({"s"}));
  CHECK_EQUAL(lift({{"x.length() == 1", {}}, {None{}, {}}}), render({}));
  CHECK_EQUAL(
    lift({{"x.length() == 1", {}}, {"y.length() == 2", {}}, {None{}, {}}}),
    render({}));
  CHECK_EQUAL(lift({{"x.length() == 1", {"false"}}, {None{}, {"false"}}}),
              render({"false"}));
  CHECK_EQUAL(lift({{"x.length() == 1", {"a"}}, {None{}, {"false"}}}),
              render({"x.length() == 1", "a"}));
}

TEST("lift filters within the size bound") {
  auto large = std::string{"e == 0"};
  for (auto i = 1; i < 1200; ++i) {
    large += fmt::format(" or e == {}", i);
  }
  // The result may grow up to the bound or the largest branch filter.
  CHECK_EQUAL(lift({{large, {"a"}}, {None{}, {"b"}}}), "<none>");
  CHECK_EQUAL(lift({{"c", {large}}, {None{}, {"b"}}}), "<none>");
  CHECK_EQUAL(lift({{large, {"x"}}, {None{}, {"x"}}}), render({"x"}));
  CHECK_EQUAL(lift({{"c", {large}}, {None{}, {large}}}), render({large}));
}

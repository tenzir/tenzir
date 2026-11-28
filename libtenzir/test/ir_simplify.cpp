//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/compile_ctx.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/ir_expr_props.hpp"
#include "tenzir/nova/array.hpp"
#include "tenzir/nova/data_array_builder.hpp"
#include "tenzir/nova/eval.hpp"
#include "tenzir/nova/eval_ctx.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/nova/instantiate_ctx.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/secret.hpp"
#include "tenzir/session.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/test/registry.hpp"
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
  auto provider = session_provider::make(dh, test::builtin_registry());
  auto ctx = provider.as_session();
  auto expr
    = parse_expression_with_location_override(source, location::unknown, ctx);
  REQUIRE(expr);
  REQUIRE(resolve_entities(*expr, ExternPrelude::empty(), ctx));
  return std::move(*expr);
}

/// Renders the simplified predicate for comparison.
auto simplify(std::string_view source) -> std::string {
  return fmt::format("{:?}", ir::simplify_predicate(parse(source),
                                                    test::builtin_registry()));
}

/// Renders a predicate as it would look after simplification.
auto render(std::string_view source) -> std::string {
  return fmt::format("{:?}", parse(source));
}

auto render_chain(ir::OptimizeFilter const& filter) -> std::string {
  auto result = std::string{};
  for (auto const& predicate : filter) {
    result += fmt::format("{:?}\n", predicate);
  }
  return result;
}

auto parse_chain(std::vector<std::string_view> const& sources)
  -> ir::OptimizeFilter {
  auto filter = ir::OptimizeFilter{};
  for (auto source : sources) {
    filter.push_back(parse(source));
  }
  return filter;
}

auto simplify_filter(std::vector<std::string_view> const& sources)
  -> std::string {
  return render_chain(
    ir::simplify_filter(parse_chain(sources), test::builtin_registry()));
}

auto render_filter(std::vector<std::string_view> const& sources)
  -> std::string {
  return render_chain(parse_chain(sources));
}

/// Replaces the field `s` with a secret constant, which is how a user-defined
/// operator passes an argument of type `secret`. The evaluator rejects it at
/// runtime.
class SecretInserter final : public ast::visitor<SecretInserter> {
public:
  template <class T>
  auto visit(T& x) -> void {
    enter(x);
  }

  auto visit(ast::expression& x) -> void {
    auto const* field = try_as<ast::root_field>(x);
    if (field and field->id.name == "s") {
      x = ast::constant{secret::make_literal("value"), location::unknown};
      return;
    }
    enter(x);
  }
};

auto parse_with_secret(std::string_view source) -> ast::expression {
  auto expr = parse(source);
  SecretInserter{}.visit(expr);
  return expr;
}

auto simplify_with_secret(std::string_view source) -> std::string {
  return fmt::format("{:?}", ir::simplify_predicate(parse_with_secret(source),
                                                    test::builtin_registry()));
}

auto render_with_secret(std::string_view source) -> std::string {
  return fmt::format("{:?}", parse_with_secret(source));
}

auto simplify_filter_with_secret(std::vector<std::string_view> const& sources)
  -> std::string {
  auto filter = ir::OptimizeFilter{};
  for (auto source : sources) {
    filter.push_back(parse_with_secret(source));
  }
  return render_chain(
    ir::simplify_filter(std::move(filter), test::builtin_registry()));
}

} // namespace

TEST("simplify folds constant subexpressions") {
  CHECK_EQUAL(simplify("3002 == 3002"), render("true"));
  CHECK_EQUAL(simplify("3002 == 4001"), render("false"));
  CHECK_EQUAL(simplify("3002 * 100 + 1 == 300201"), render("true"));
  CHECK_EQUAL(simplify("x == 3002 * 100 + 1"), render("x == 300201"));
  CHECK_EQUAL(simplify("\"203.0.113.7\".ip() == 203.0.113.7"), render("true"));
  CHECK_EQUAL(simplify("x in [1, 1 + 1]"), render("x in [1, 2]"));
  CHECK_EQUAL(simplify("xs.map(v => v * (2 + 3)) == [5]"),
              render("xs.map(v => v * 5) == [5]"));
}

TEST("simplify keeps expressions that read the event or are not "
     "deterministic") {
  CHECK_EQUAL(simplify("x == y"), render("x == y"));
  CHECK_EQUAL(simplify("@name == \"foo\""), render("@name == \"foo\""));
  CHECK_EQUAL(simplify("now() > 2020-01-01"), render("now() > 2020-01-01"));
  CHECK_EQUAL(simplify("random() < 2.0 or true"),
              render("random() < 2.0 or true"));
  CHECK_EQUAL(simplify("true or random() < 2.0"), render("true"));
}

TEST("simplify follows null semantics") {
  CHECK_EQUAL(simplify("null == null"), render("true"));
  CHECK_EQUAL(simplify("null == 42"), render("false"));
  CHECK_EQUAL(simplify("null != 42"), render("true"));
  CHECK_EQUAL(simplify("not null"), render("null"));
  CHECK_EQUAL(simplify("null and false"), render("false"));
  CHECK_EQUAL(simplify("false and null"), render("false"));
  CHECK_EQUAL(simplify("null or true"), render("true"));
  CHECK_EQUAL(simplify("null and true"), render("null"));
  CHECK_EQUAL(simplify("(null and false) == false"), render("true"));
  CHECK_EQUAL(simplify("(null and true) == null"), render("true"));
  // `null and x` is `false` or `null` depending on `x`, and `where` warns for
  // `null`.
  CHECK_EQUAL(simplify("null"), render("null"));
  CHECK_EQUAL(simplify("null and x"), render("null and x"));
  CHECK_EQUAL(simplify("x or null"), render("x or null"));
  CHECK_EQUAL(simplify("not (null and x)"), render("not (null and x)"));
  // The condition of an `if` silently treats `null` as `false`.
  CHECK_EQUAL(simplify("x if null else y"), render("y"));
  CHECK_EQUAL(simplify("x if (null and z) else y"), render("y"));
  CHECK_EQUAL(simplify("x if (null or z) else y"), render("x if z else y"));
  // Ordering `null` yields `null` without a diagnostic.
  CHECK_EQUAL(simplify("null > 42"), render("null"));
}

TEST("simplify handles missing fields") {
  CHECK_EQUAL(simplify("{a: 1}.a == 1"), render("true"));
  CHECK_EQUAL(simplify("{a: 1}.b? == null"), render("true"));
  CHECK_EQUAL(simplify("null.b? == null"), render("true"));
  // Reading a missing field without `?` warns at runtime.
  CHECK_EQUAL(simplify("{a: 1}.b == null"), render("{a: 1}.b == null"));
  CHECK_EQUAL(simplify("null.b == null"), render("null.b == null"));
}

TEST("simplify handles type mismatches") {
  CHECK_EQUAL(simplify("1 == 1.0"), render("true"));
  // Equality is total, so mismatched types fold instead of warning.
  CHECK_EQUAL(simplify("42 == \"42\""), render("false"));
  CHECK_EQUAL(simplify("42 != \"42\""), render("true"));
  // Ordering stays partial and warns at runtime.
  CHECK_EQUAL(simplify("42 < \"42\""), render("42 < \"42\""));
  // Operations that warn at runtime stay.
  CHECK_EQUAL(simplify("x == 1 / 0"), render("x == 1 / 0"));
  // An operation that evaluates a warning operand warns as well.
  CHECK_EQUAL(simplify("x == (1 / 0 + 1) * 2"), render("x == (1 / 0 + 1) * 2"));
  CHECK_EQUAL(simplify("x == [1 / 0, 1 + 1][1]"), render("x == [1 / 0, 2][1]"));
  CHECK_EQUAL(simplify("(1 / 0 == 1 + 1) or true"), render("true"));
  CHECK_EQUAL(simplify("x if 1 / 0 == null else 1 + 1"),
              render("x if 1 / 0 == null else 2"));
  CHECK_EQUAL(simplify("not 42"), render("not 42"));
  // A value other than `bool` or `null` is neither `true` nor `false`.
  CHECK_EQUAL(simplify("x and 42"), render("x and 42"));
  CHECK_EQUAL(simplify("42 or x"), render("42 or x"));
}

TEST("simplify prunes logical operators") {
  CHECK_EQUAL(simplify("x and true"), render("x"));
  CHECK_EQUAL(simplify("true and x"), render("x"));
  CHECK_EQUAL(simplify("x or false"), render("x"));
  CHECK_EQUAL(simplify("false or x"), render("x"));
  CHECK_EQUAL(simplify("x and false"), render("false"));
  CHECK_EQUAL(simplify("false and x"), render("false"));
  CHECK_EQUAL(simplify("x or true"), render("true"));
  CHECK_EQUAL(simplify("true or x"), render("true"));
  CHECK_EQUAL(simplify("not not x"), render("x"));
  CHECK_EQUAL(simplify("not not not not (x and true)"), render("x"));
  CHECK_EQUAL(simplify("not not not x"), render("not x"));
  CHECK_EQUAL(simplify("not (true and not x)"), render("x"));
  CHECK_EQUAL(simplify("not not (true and not not x)"), render("x"));
  CHECK_EQUAL(simplify("not not (1 + 1 == 2)"), render("true"));
  CHECK_EQUAL(simplify("not not 42"), render("42"));
  // A condition treats `null` as `false`, also behind a double negation.
  CHECK_EQUAL(simplify("x if not not (null or y) else z"),
              render("x if y else z"));
  CHECK_EQUAL(simplify("x if not (true and not (null or y)) else z"),
              render("x if y else z"));
  CHECK_EQUAL(simplify("x if not not not (null and y) else z"),
              render("x if not (null and y) else z"));
  CHECK_EQUAL(simplify("not (x and true)"), render("not x"));
  CHECK_EQUAL(simplify("(EventID == 4624 and 3002 == 3002)"
                       " or (EventID == 4625 and 3002 == 4001)"),
              render("EventID == 4624"));
  // A value other than `bool` or `null` distinguishes `x` from `true and x`.
  CHECK_EQUAL(simplify("x == (true and false)"), render("x == false"));
  CHECK_EQUAL(simplify("x == (true and y)"), render("x == (true and y)"));
  CHECK_EQUAL(simplify("(not not x) == true"), render("(not not x) == true"));
}

TEST("simplify keeps operands with invalid function calls") {
  // The evaluator instantiates every call when the pipeline starts, even in
  // operands that it never evaluates, and an invalid call fails the pipeline.
  CHECK_EQUAL(simplify("int(x, base=3) == 1 or true"),
              render("int(x, base=3) == 1 or true"));
  CHECK_EQUAL(simplify("true or int(x, base=3) == 1"),
              render("true or int(x, base=3) == 1"));
  CHECK_EQUAL(simplify("false and int(x, base=3) == 1"),
              render("false and int(x, base=3) == 1"));
  CHECK_EQUAL(simplify("int(x, base=3) == 1 if false else true"),
              render("int(x, base=3) == 1 if false else true"));
  CHECK_EQUAL(simplify("true if true else int(x, base=3) == 1"),
              render("true if true else int(x, base=3) == 1"));
  CHECK_EQUAL(simplify("(1 else int(x, base=3)) == 1"),
              render("(1 else int(x, base=3)) == 1"));
  CHECK_EQUAL(simplify("true or int(x, base=16) == 1"), render("true"));
  CHECK_EQUAL(simplify("true or (true or (false and int(x, base=3) == 1))"),
              render("true or (true or (false and int(x, base=3) == 1))"));
  CHECK_EQUAL(simplify("true or (int(x, base=3) == 1 if false else true)"),
              render("true or (int(x, base=3) == 1 if false else true)"));
  CHECK_EQUAL(simplify("true or (x == 1 if true else [int(x, base=3)])"),
              render("true or (x == 1 if true else [int(x, base=3)])"));
  CHECK_EQUAL(
    simplify_filter({"int(x, base=3) == 1", "x == 1", "false",
                     "int(y, base=3) == 1", "y == 1"}),
    render_filter({"int(x, base=3) == 1", "false", "int(y, base=3) == 1"}));
  CHECK_EQUAL(
    simplify_filter({"false", "true or (false and int(y, base=3) == 1)"}),
    render_filter({"false", "true or (false and int(y, base=3) == 1)"}));
}

TEST("simplify keeps evaluated operands that could fail") {
  // A function may fail depending on its input, like `slice` with a negative
  // stride for a string.
  CHECK_NOT_EQUAL(simplify("slice(x, stride=-1) == \"\" or true"),
                  render("true"));
  CHECK_EQUAL(simplify("x.length() == 1 and false"),
              render("x.length() == 1 and false"));
  // The evaluator rejects a record literal with several spreads.
  CHECK_EQUAL(simplify("{...x, ...y} == z or true"),
              render("{...x, ...y} == z or true"));
  // Operators only warn.
  CHECK_EQUAL(simplify("x.y[0] + 1 == -z or true"), render("true"));
  CHECK_EQUAL(simplify("{...x, a: y} == [z] and false"), render("false"));
  CHECK_EQUAL(simplify_filter({"x.length() == 1", "x == 1", "false"}),
              render_filter({"x.length() == 1", "false"}));
}

TEST("branch lifting keeps constants that the evaluator rejects") {
  auto const& reg = test::builtin_registry();
  CHECK(ir::analyze(parse("x == 1"), reg).is_tql_infallible());
  CHECK(not ir::analyze(parse("x.length() == 1"), reg).is_tql_infallible());
  CHECK(not ir::analyze(parse_with_secret("s == x"), reg).is_tql_infallible());
  CHECK(
    not ir::analyze(parse_with_secret("{a: s} == x"), reg).is_tql_infallible());
  CHECK(
    not ir::analyze(parse_with_secret("[s] == x"), reg).is_tql_infallible());
  auto chain = ir::OptimizeFilter{parse("x == 1"), parse_with_secret("s == y")};
  CHECK_EQUAL(fmt::format("{:?}", ir::conjoin_filter(std::move(chain), reg)),
              render_with_secret("s == y if x == 1 else false"));
}

TEST("simplify keeps constants that the evaluator rejects") {
  // The evaluator fails the pipeline for a secret constant that it evaluates.
  CHECK_EQUAL(simplify_with_secret("s or true"),
              render_with_secret("s or true"));
  CHECK_EQUAL(simplify_with_secret("{a: s} == null or true"),
              render_with_secret("{a: s} == null or true"));
  CHECK_EQUAL(simplify_with_secret("[s] == x and false"),
              render_with_secret("[s] == x and false"));
  // The runtime never evaluates the right operand of a decided `and` or `or`,
  // nor a branch that no row takes.
  CHECK_EQUAL(simplify_with_secret("true or s"), render("true"));
  CHECK_EQUAL(simplify_with_secret("false and s == x"), render("false"));
  CHECK_EQUAL(simplify_with_secret("true if true else s"), render("true"));
  CHECK_EQUAL(simplify_with_secret("(s if false) == null"), render("true"));
  CHECK_EQUAL(simplify_with_secret("(1 else s) == 1"), render("true"));
  // The secret stays where the runtime evaluates it.
  CHECK_EQUAL(simplify_with_secret("s and true"), render_with_secret("s"));
  CHECK_EQUAL(simplify_with_secret("(null else s) == x"),
              render_with_secret("s == x"));
  CHECK_EQUAL(simplify_filter_with_secret({"s", "x == 1", "false", "s"}),
              render_chain(ir::OptimizeFilter{parse_with_secret("s"),
                                              parse("false"),
                                              parse_with_secret("s")}));
}

TEST("simplify keeps assignments in place of values") {
  // The evaluator only warns for them at the top level and does not support
  // them inside other expressions.
  CHECK_EQUAL(simplify("(x = 1 + 1) else false"), render("(x = 2) else false"));
  CHECK_EQUAL(simplify("int(\"10\", base=16) == 16"), render("true"));
}

TEST("simplify keeps the arguments of invalid calls") {
  // Some arguments are syntax rather than values, and simplifying them could
  // turn a rejected call into a valid one.
  CHECK_EQUAL(simplify("drop_null_fields({a: x, b: 1 + 1}, a if true) == y"),
              render("drop_null_fields({a: x, b: 1 + 1}, a if true) == y"));
  CHECK_EQUAL(simplify("count_if([1 + 1], (v => true) if true) == 1"),
              render("count_if([1 + 1], (v => true) if true) == 1"));
  CHECK_EQUAL(simplify("int(y, base=16 if x == null or true) == 1"),
              render("int(y, base=16 if x == null or true) == 1"));
  // The arguments of valid calls keep the syntax they need.
  CHECK_EQUAL(simplify("drop_null_fields({a: x, b: 1 + 1}, a) == y"),
              render("drop_null_fields({a: x, b: 2}, a) == y"));
  CHECK_EQUAL(simplify("count_if(xs, v => v > 1 + 1) == 1"),
              render("count_if(xs, v => v > 2) == 1"));
  CHECK_EQUAL(simplify("int(y, base=16 if true) == 1"),
              render("int(y, base=16) == 1"));
  // A call in the arguments of a valid call is valid as well.
  CHECK_EQUAL(simplify("int(int(y, base=16 if true).string()) == 1"),
              render("int(int(y, base=16).string()) == 1"));
  CHECK_EQUAL(simplify("count_if(xs, v => int(v, base=8 * 2) > 1) == 1"),
              render("count_if(xs, v => int(v, base=16) > 1) == 1"));
  // A call with an invalid call in its arguments is invalid as well.
  CHECK_EQUAL(
    simplify("int(int(y, base=16 if x == null or true).string()) == 1"),
    render("int(int(y, base=16 if x == null or true).string()) == 1"));
  // A branch that no row takes need not be constant.
  CHECK_EQUAL(simplify("int(y, base=16 if true else x) == 1"),
              render("int(y, base=16) == 1"));
}

TEST("simplify selects branches of conditional expressions") {
  CHECK_EQUAL(simplify("x if true else y"), render("x"));
  CHECK_EQUAL(simplify("x if false else y"), render("y"));
  CHECK_EQUAL(simplify("x if 1 == 1"), render("x"));
  CHECK_EQUAL(simplify("x if 1 == 2"), render("null"));
  CHECK_EQUAL(simplify("(y if true else 42) == null"), render("y == null"));
  CHECK_EQUAL(simplify("(y if false else 42) == 42"), render("true"));
  CHECK_EQUAL(simplify("(y if false) == null"), render("true"));
  CHECK_EQUAL(simplify("(y if x else 42) == 1"),
              render("(y if x else 42) == 1"));
  CHECK_EQUAL(simplify("(null else x) == 1"), render("x == 1"));
  CHECK_EQUAL(simplify("(1 else x) == 1"), render("true"));
  CHECK_EQUAL(simplify("(x else 1) == 1"), render("(x else 1) == 1"));
  // Equal branches make the condition irrelevant, unless it could fail.
  CHECK_EQUAL(simplify("false if x == 1 else false"), render("false"));
  CHECK_EQUAL(simplify("true if x else (1 == 1)"), render("true"));
  CHECK_EQUAL(simplify("false if x.length() == 1 else false"),
              render("false if x.length() == 1 else false"));
  CHECK_EQUAL(simplify("true if x else false"), render("true if x else false"));
}

TEST("simplify filter chains") {
  CHECK_EQUAL(simplify_filter({"x == 1", "1 == 1", "y == 2"}),
              render_filter({"x == 1", "y == 2"}));
  CHECK_EQUAL(simplify_filter({"x == 1", "3002 == 4001", "y == 2"}),
              render_filter({"false"}));
  CHECK_EQUAL(simplify_filter({"true"}), render_filter({}));
  // A `null` predicate keeps no event, but warns.
  CHECK_EQUAL(simplify_filter({"x == 1", "null", "y == 2"}),
              render_filter({"x == 1", "null", "y == 2"}));
}

TEST("expression size") {
  auto const& reg = test::builtin_registry();
  CHECK_EQUAL(ir::analyze(parse("x"), reg).node_count, uint32_t{1});
  CHECK_EQUAL(ir::analyze(parse("x == 1"), reg).node_count, uint32_t{3});
  CHECK_EQUAL(ir::analyze(parse("x.y == [1, 2]"), reg).node_count, uint32_t{6});
}

TEST("substitution stops when the predicate grows beyond the bound") {
  // Every step replaces `x` with `x + y.z`, which reads it once.
  auto const growing
    = [](ast::field_path const& field) -> Option<ast::expression> {
    if (field.path().size() != 1 or field.path().front().id.name != "x") {
      return field.inner();
    }
    return ast::binary_expr{field.inner(), ast::binary_op::add, parse("y.z")};
  };
  auto predicate = parse("x == 1");
  auto steps = 0;
  while (true) {
    auto split = ir::split_filter_by_substitution(
      {predicate}, test::builtin_registry(), growing);
    if (split.independent.empty()) {
      break;
    }
    predicate = std::move(split.independent.front());
    ++steps;
  }
  // After `n` steps, the predicate has `3n + 3` nodes, and the bound is 4096
  // nodes.
  auto const& reg = test::builtin_registry();
  CHECK_EQUAL(steps, 1364);
  CHECK_EQUAL(ir::analyze(predicate, reg).node_count, uint32_t{4095});
  // A substitution that does not grow the predicate may exceed the bound.
  auto const rename = [](ast::field_path const&) -> Option<ast::expression> {
    return ast::root_field{ast::identifier{"y", location::unknown}};
  };
  auto large = parse("x");
  for (auto i = 0; i < 12; ++i) {
    large = ast::binary_expr{large, ast::binary_op::add, large};
  }
  large = ast::binary_expr{large, ast::binary_op::eq, parse("1")};
  REQUIRE_GREATER(ir::analyze(large, reg).node_count, uint32_t{4096});
  auto split = ir::split_filter_by_substitution(
    {large}, test::builtin_registry(), rename);
  REQUIRE_EQUAL(split.independent.size(), size_t{1});
  CHECK_EQUAL(ir::analyze(split.independent.front(), reg).node_count,
              ir::analyze(large, reg).node_count);
}

TEST("plain and cheap expressions") {
  auto const& reg = test::builtin_registry();
  CHECK(ir::analyze(parse("x"), reg).is_plain());
  CHECK(ir::analyze(parse("x.y[\"z\"]"), reg).is_plain());
  CHECK(ir::analyze(parse("this"), reg).is_plain());
  CHECK(ir::analyze(parse("@name"), reg).is_plain());
  CHECK(ir::analyze(parse("42"), reg).is_plain());
  CHECK(ir::analyze(parse("[]"), reg).is_plain());
  CHECK(ir::analyze(parse("[1, \"a\"]"), reg).is_plain());
  CHECK(ir::analyze(parse("{a: 1, b: {c: [2.0]}}"), reg).is_plain());
  CHECK(ir::analyze(parse("{a: [1]}.a"), reg).is_plain());
  CHECK(not ir::analyze(parse("x + 1"), reg).is_plain());
  CHECK(not ir::analyze(parse("x[y]"), reg).is_plain());
  CHECK(not ir::analyze(parse("{a: x}"), reg).is_plain());
  CHECK(not ir::analyze(parse("[1, x]"), reg).is_plain());
  CHECK(not ir::analyze(parse("{a: {b: x}}"), reg).is_plain());
  CHECK(not ir::analyze(parse("[1, [1 + 1]]"), reg).is_plain());
  CHECK(not ir::analyze(parse("[...x]"), reg).is_plain());
  CHECK(not ir::analyze(parse("{...x}"), reg).is_plain());
  CHECK(ir::analyze(parse("x + 1 == y.z"), reg).is_cheap());
  CHECK(ir::analyze(parse("{a: x, b: [y, 1]}"), reg).is_cheap());
  CHECK(
    ir::analyze(parse("x.ip() == 1.2.3.4 and y.length() > 1"), reg).is_cheap());
  CHECK(not ir::analyze(parse("x.parse_json()"), reg).is_cheap());
  CHECK(
    not ir::analyze(parse("x + y.parse_syslog().app_name"), reg).is_cheap());
  // A cheap function with an expensive argument is expensive.
  CHECK(not ir::analyze(parse("x.parse_json().length()"), reg).is_cheap());
}

TEST("substitution copies plain values freely") {
  auto const rename = [](ast::field_path const&) -> Option<ast::expression> {
    return parse("y.z");
  };
  auto split = ir::split_filter_by_substitution(
    parse_chain({"x == 1 or x == 2", "x != 3"}), test::builtin_registry(),
    rename);
  CHECK_EQUAL(render_chain(split.independent),
              render_filter({"y.z == 1 or y.z == 2", "y.z != 3"}));
  CHECK(split.dependent.empty());
}

TEST("substitution copies literals of constants freely") {
  // As for `metadata = {profiles: ["cloud"], version: "1.9.0"}`.
  auto const literal
    = [](ast::field_path const& field) -> Option<ast::expression> {
    if (field.path().front().id.name == "m") {
      return parse(R"({profiles: ["cloud"], version: "1.9.0"})");
    }
    return field.inner();
  };
  auto split = ir::split_filter_by_substitution(
    parse_chain({R"(m.profiles == ["a"] or m.profiles == ["b"])",
                 R"(x in m.profiles or y in m.profiles)"}),
    test::builtin_registry(), literal);
  CHECK_EQUAL(split.independent.size(), size_t{2});
  CHECK(split.dependent.empty());
}

TEST("substitution does not copy expensive function calls") {
  auto const call = [](ast::field_path const&) -> Option<ast::expression> {
    return parse("y.parse_json()");
  };
  auto split = ir::split_filter_by_substitution(parse_chain({"x == 1"}),
                                                test::builtin_registry(), call);
  CHECK(split.independent.empty());
  CHECK_EQUAL(render_chain(split.dependent), render_filter({"x == 1"}));
  // This holds below an expensive call that the predicate makes already.
  split = ir::split_filter_by_substitution(
    parse_chain({R"(x.replace("a", "b") == "c")"}), test::builtin_registry(),
    call);
  CHECK(split.independent.empty());
  // An expensive part that the substitution makes constant does not make up
  // for one that it copies.
  auto const mixed
    = [](ast::field_path const& field) -> Option<ast::expression> {
    if (field.path().front().id.name == "x") {
      return parse(R"("[1]")");
    }
    return parse("raw.parse_json()");
  };
  split = ir::split_filter_by_substitution(
    parse_chain({"y.foo == 1 and x.parse_json() == [1]"}),
    test::builtin_registry(), mixed);
  CHECK(split.independent.empty());
  // A format string is expensive as well.
  auto const format = [](ast::field_path const&) -> Option<ast::expression> {
    return parse(R"(f"{y}")");
  };
  split = ir::split_filter_by_substitution(parse_chain({R"(x == "a")"}),
                                           test::builtin_registry(), format);
  CHECK(split.independent.empty());
  // A cheap function call counts as a computed value.
  auto const conversion
    = [](ast::field_path const&) -> Option<ast::expression> {
    return parse("y.ip()");
  };
  split = ir::split_filter_by_substitution(
    parse_chain({"x == 1.2.3.4"}), test::builtin_registry(), conversion);
  CHECK_EQUAL(render_chain(split.independent),
              render_filter({"y.ip() == 1.2.3.4"}));
  split = ir::split_filter_by_substitution(
    parse_chain({"x == 1.2.3.4 or x == 5.6.7.8"}), test::builtin_registry(),
    conversion);
  CHECK(split.independent.empty());
}

TEST("substitution reads a described member of an expensive value once") {
  // As for `x = ocsf_cast(this)`, whose members the simplifier describes.
  auto const cast
    = [](ast::field_path const& field) -> Option<ast::expression> {
    return parse(
      fmt::format("ocsf_cast(this).{}", field.path().back().id.name));
  };
  auto split = ir::split_filter_by_substitution(
    parse_chain({"x.activity_id == 1", "x.severity_id == 2"}),
    test::builtin_registry(), cast);
  CHECK_EQUAL(split.independent.size(), size_t{2});
  CHECK(split.dependent.empty());
  split = ir::split_filter_by_substitution(parse_chain({"x.activity_id == 1 or "
                                                        "x.activity_id == 2"}),
                                           test::builtin_registry(), cast);
  CHECK(split.independent.empty());
}

TEST("substitution copies a computed value at most once") {
  // `x` and `w` are computed, `p` is expensive, and the others are plain.
  auto const computed
    = [](ast::field_path const& field) -> Option<ast::expression> {
    auto const name = field.path().front().id.name;
    if (name == "x") {
      return parse("y + 1");
    }
    if (name == "w") {
      return parse(fmt::format("(y + 2).{}", field.path().back().id.name));
    }
    if (name == "p") {
      return parse("y.parse_json()");
    }
    return field.inner();
  };
  auto split = ir::split_filter_by_substitution(
    parse_chain({"x == 1", "z == 2"}), test::builtin_registry(), computed);
  CHECK_EQUAL(render_chain(split.independent),
              render_filter({"y + 1 == 1", "z == 2"}));
  CHECK(split.dependent.empty());
  // Only the first predicate that reads `x` moves.
  split = ir::split_filter_by_substitution(
    parse_chain({"x > 1", "x < 10", "z == 2"}), test::builtin_registry(),
    computed);
  CHECK_EQUAL(render_chain(split.independent),
              render_filter({"y + 1 > 1", "z == 2"}));
  CHECK_EQUAL(render_chain(split.dependent), render_filter({"x < 10"}));
  // A predicate that stays for another reason does not count as a read.
  split = ir::split_filter_by_substitution(
    parse_chain({"p == 1", "x.int() == 1", "x == 2"}), test::builtin_registry(),
    computed);
  CHECK_EQUAL(render_chain(split.independent), render_filter({"y + 1 == 2"}));
  CHECK_EQUAL(render_chain(split.dependent),
              render_filter({"p == 1", "x.int() == 1"}));
  split = ir::split_filter_by_substitution(parse_chain({"x == 1 or x == 2"}),
                                           test::builtin_registry(), computed);
  CHECK(split.independent.empty());
  // Members of the same computed value read it as well.
  split = ir::split_filter_by_substitution(
    parse_chain({"w.a == 1 or w.b == 2"}), test::builtin_registry(), computed);
  CHECK(split.independent.empty());
  // Doubling the field in every step therefore stops after one step.
  auto const doubling
    = [](ast::field_path const& field) -> Option<ast::expression> {
    return ast::binary_expr{field.inner(), ast::binary_op::add, field.inner()};
  };
  split = ir::split_filter_by_substitution(parse_chain({"x == 1"}),
                                           test::builtin_registry(), doubling);
  REQUIRE_EQUAL(split.independent.size(), size_t{1});
  split = ir::split_filter_by_substitution(std::move(split.independent),
                                           test::builtin_registry(), doubling);
  CHECK(split.independent.empty());
}

TEST("split conjunctions") {
  auto const split = [](std::string_view source) {
    return render_chain(ir::split_conjunction(parse(source)));
  };
  CHECK_EQUAL(split("x == 1"), render_filter({"x == 1"}));
  CHECK_EQUAL(split("a and b"), render_filter({"a", "b"}));
  // Every top-level `and` splits, in the order of its operands.
  CHECK_EQUAL(split("a and b and c"), render_filter({"a", "b", "c"}));
  CHECK_EQUAL(split("a and (b and c)"), render_filter({"a", "b", "c"}));
  // An `and` below another expression stays whole.
  CHECK_EQUAL(split("(a and b) or c"), render_filter({"(a and b) or c"}));
  CHECK_EQUAL(split("not (a and b) and c"),
              render_filter({"not (a and b)", "c"}));
  CHECK_EQUAL(split("(a and b) == true"), render_filter({"(a and b) == true"}));
}

TEST("split conjunctions move the operands that a substitution rewrites") {
  auto const keep_others
    = [](ast::field_path const& field) -> Option<ast::expression> {
    if (field.path().size() == 1 and field.path().front().id.name == "t") {
      return None{};
    }
    return field.inner();
  };
  auto split = ir::split_filter_by_substitution(
    ir::split_conjunction(parse("x == 1 and t > 0 and y == 2")),
    test::builtin_registry(), keep_others);
  CHECK_EQUAL(render_chain(split.independent),
              render_filter({"x == 1", "y == 2"}));
  CHECK_EQUAL(render_chain(split.dependent), render_filter({"t > 0"}));
  // An operand that could fail stays behind one that stays.
  split = ir::split_filter_by_substitution(
    ir::split_conjunction(parse("t > 0 and x.length() == 1")),
    test::builtin_registry(), keep_others);
  CHECK(split.independent.empty());
  CHECK_EQUAL(render_chain(split.dependent),
              render_filter({"t > 0", "x.length() == 1"}));
}

TEST("predicates that could fail stay behind the ones that stay") {
  // `slice` fails for a string with a negative stride. Moving it in front of
  // `c`, which stays behind the operator, would evaluate it for the events that
  // `c` drops.
  auto const touched = ast::collect_refs(parse("c"));
  REQUIRE(touched);
  auto split = ir::split_filter_by_dependents(
    parse_chain({"c", "x.slice(stride=-1) == \"\"", "y == 1"}), *touched);
  CHECK_EQUAL(render_chain(split.independent), render_filter({"y == 1"}));
  CHECK_EQUAL(render_chain(split.dependent),
              render_filter({"c", "x.slice(stride=-1) == \"\""}));
  split = ir::split_filter_by_dependents(
    parse_chain({"x.slice(stride=-1) == \"\"", "c"}), *touched);
  CHECK_EQUAL(render_chain(split.independent),
              render_filter({"x.slice(stride=-1) == \"\""}));
  CHECK_EQUAL(render_chain(split.dependent), render_filter({"c"}));
  // The same holds for a substitution that has no replacement for `c`.
  auto const keep_others
    = [](ast::field_path const& field) -> Option<ast::expression> {
    if (field.path().size() == 1 and field.path().front().id.name == "c") {
      return None{};
    }
    return field.inner();
  };
  split = ir::split_filter_by_substitution(
    parse_chain({"c", "x.slice(stride=-1) == \"\"", "y == 1"}),
    test::builtin_registry(), keep_others);
  CHECK_EQUAL(render_chain(split.independent), render_filter({"y == 1"}));
  CHECK_EQUAL(render_chain(split.dependent),
              render_filter({"c", "x.slice(stride=-1) == \"\""}));
}

TEST("simplify reads fields of call results from the arguments") {
  // `drop_matching` keeps or drops every top-level field by its name.
  CHECK_EQUAL(simplify("drop_matching(this, \"^x\").a == 1"), render("a == 1"));
  CHECK_EQUAL(simplify("drop_matching(this, \"^x\").a.b?"), render("a.b?"));
  CHECK_EQUAL(simplify("drop_matching(this, \"^x\").xa == 1"), render("false"));
  CHECK_EQUAL(simplify("drop_matching(this, \"^x\").xa != 1"), render("true"));
  CHECK_EQUAL(simplify("1 == select_matching(y, \"^a\").a"),
              render("1 == y.a"));
  CHECK_EQUAL(simplify("not (select_matching(this, \"^a\").a in [1, 2])"),
              render("not (a in [1, 2])"));
  // A conditional in an argument stays in the field it reads.
  CHECK_EQUAL(simplify("select_matching({a: 1} if c else {a: 2}, \".*\").a "
                       "== 2"),
              render("({a: 1} if c else {a: 2}).a == 2"));
}

TEST("simplify keeps fields of call results that it cannot rewrite") {
  // Omitting a call whose argument could fail would hide its error.
  CHECK(
    simplify("select_matching(slice(s, stride=-1), \"^keep$\").gone != \"x\"")
      .contains("select_matching"));
  // The rewrite applies to predicates only, not to lambdas and arguments.
  CHECK_EQUAL(
    simplify("xs.where(x => drop_matching(this, \"^x\").a == 1).length() > 0"),
    render("xs.where(x => drop_matching(this, \"^x\").a == 1).length() > 0"));
  CHECK_EQUAL(simplify("string(drop_matching(this, \"^x\").a == 1) == "
                       "\"true\""),
              render("string(drop_matching(this, \"^x\").a == 1) == \"true\""));
  // An invalid call stays and fails the pipeline.
  CHECK_EQUAL(simplify("drop_matching(this, \"(\").a == 1"),
              render("drop_matching(this, \"(\").a == 1"));
  // A call without a rewrite stays.
  CHECK_EQUAL(simplify("merge(x, y).a == 1"), render("merge(x, y).a == 1"));
}

TEST("split_filter_by_rewrite enforces rejection, size bound, and "
     "infallibility") {
  // Rejected predicates stay unchanged; infallible predicates after them move
  // while fallible ones stay behind.
  auto const rewrite
    = [](ast::expression const& expr) -> Option<ast::expression> {
    if (auto const* field = try_as<ast::root_field>(expr);
        field and field->id.name == "c") {
      return None{};
    }
    return expr;
  };
  auto split = ir::split_filter_by_rewrite(
    parse_chain({"c", R"(x.slice(stride=-1) == "")", "y == 1"}), rewrite);
  CHECK_EQUAL(render_chain(split.independent), render_filter({"y == 1"}));
  CHECK_EQUAL(render_chain(split.dependent),
              render_filter({"c", R"(x.slice(stride=-1) == "")"}));
  // Repeated growth stops at `max_rewritten_predicate_size`.
  auto const guard_step
    = [](ast::expression const& expr) -> Option<ast::expression> {
    return ast::binary_expr{
      ast::binary_expr{ast::constant{false, location::unknown},
                       ast::binary_op::if_, parse(R"(x == "-")")},
      ast::binary_op::else_, expr};
  };
  auto predicate = parse("x != null");
  auto steps = size_t{0};
  while (true) {
    auto step = ir::split_filter_by_rewrite({predicate}, guard_step);
    if (step.independent.empty()) {
      break;
    }
    predicate = std::move(step.independent.front());
    ++steps;
  }
  auto const& reg = test::builtin_registry();
  CHECK_GREATER(steps, size_t{0});
  CHECK_LESS_EQUAL(ir::analyze(predicate, reg).node_count,
                   ir::max_rewritten_predicate_size);
  // A non-growing rewrite is allowed even above the bound.
  auto large = parse("x");
  for (auto i = 0; i < 12; ++i) {
    large = ast::binary_expr{large, ast::binary_op::add, large};
  }
  large = ast::binary_expr{large, ast::binary_op::eq, parse("1")};
  REQUIRE_GREATER(ir::analyze(large, reg).node_count,
                  ir::max_rewritten_predicate_size);
  split = ir::split_filter_by_rewrite({large}, rewrite);
  REQUIRE_EQUAL(split.independent.size(), size_t{1});
  CHECK_EQUAL(ir::analyze(split.independent.front(), reg).node_count,
              ir::analyze(large, reg).node_count);
}

TEST("long chains of replace operators stay within the size bound") {
  auto const optimize = [](std::string_view source) -> ir::OptimizeResult {
    auto dh = collecting_diagnostic_handler{};
    auto const& reg = test::builtin_registry();
    auto provider = session_provider::make(dh, reg);
    auto ctx = base_ctx{dh, reg};
    auto ast = parse_pipeline_with_location_override(source, location::unknown,
                                                     provider.as_session());
    REQUIRE(ast);
    REQUIRE(resolve_entities(*ast, ExternPrelude::empty(), ctx));
    auto compiled = std::move(*ast).compile(compile_ctx::make_root(ctx));
    REQUIRE(compiled);
    REQUIRE(compiled->substitute(substitute_ctx{ctx, nullptr}, true));
    return std::move(*compiled).optimize(
      {.filter = {}, .order = EventOrder::ordered}, {.reg = reg});
  };
  // Distinct `what` values grow `x != null` linearly (+6 nodes per step) and
  // stop at `max_rewritten_predicate_size`.
  auto make_distinct = [](int count) {
    auto source = std::string{};
    for (auto i = 0; i < count; ++i) {
      source += fmt::format("replace x, what=\"s{}\", with=null\n", i);
    }
    source += "where x != null\n";
    return source;
  };
  auto const& reg = test::builtin_registry();
  auto opt = optimize(make_distinct(100));
  REQUIRE_EQUAL(opt.filter.size(), size_t{1});
  CHECK_LESS_EQUAL(ir::analyze(opt.filter.front(), reg).node_count,
                   ir::max_rewritten_predicate_size);
  opt = optimize(make_distinct(750));
  CHECK(opt.filter.empty());
  CHECK_GREATER(opt.replacement.operators.size(), size_t{750});
  // Identical `what` values also stay bounded and stop before blowing up.
  auto identical = std::string{};
  for (auto i = 0; i < 30; ++i) {
    identical += "replace x, what=\"-\", with=null\n";
  }
  identical += "where x != null\n";
  opt = optimize(identical);
  CHECK(opt.filter.empty());
  CHECK_GREATER(opt.replacement.operators.size(), size_t{30});
  // A comparison against an unaffected constant folds to unchanged size across
  // any chain length.
  auto folding = std::string{};
  for (auto i = 0; i < 50; ++i) {
    folding += fmt::format("replace x, what=\"s{}\", with=null\n", i);
  }
  folding += "where x == \"a\"\n";
  opt = optimize(folding);
  REQUIRE_EQUAL(opt.filter.size(), size_t{1});
  CHECK_EQUAL(ir::analyze(opt.filter.front(), reg).node_count, uint32_t{3});
}

TEST("replace guarded conditionals preserve secret and cross-type semantics") {
  auto builder = nova::ArrayBuilder<nova::Data>{};
  builder.null();
  builder.data(std::string_view{"-"});
  builder.data(std::string_view{"a"});
  builder.data(nova::Int{0});
  builder.data(nova::Float{0.0});
  auto bytes
    = std::array<std::byte, 3>{std::byte{'s'}, std::byte{'e'}, std::byte{'c'}};
  builder.data(nova::SecretView{bytes.begin(), bytes.end()});
  auto col = builder.finish();
  auto const length = col.length();
  auto present = nova::storage::BitMap{length, true};
  auto records
    = nova::Array<nova::Record>::make_empty(length).with_field_overwrite(
      "x", {std::move(col), present});
  auto events = nova::Events{std::move(records), present,
                             nova::Events::Meta::make_empty(length)};
  auto const& reg = test::builtin_registry();
  auto const eval_expr = [&](std::string_view source) {
    auto dh = null_diagnostic_handler{};
    auto evaluator
      = nova::Evaluator::make(parse(source), nova::InstantiateCtx{dh, reg});
    REQUIRE(evaluator);
    return evaluator->eval(events, nova::EvalCtx{dh});
  };
  auto const pairs
    = std::array<std::pair<std::string_view, std::string_view>, 8>{
      {{R"((null if x == "-" else x) == "a")", R"(x == "a")"},
       {R"((null if x == "-" else x) != "a")", R"(x != "a")"},
       {R"((null if x == "-" else x) == null)",
        R"(true if x == "-" else x == null)"},
       {R"((null if x == "-" else x) != null)",
        R"(false if x == "-" else x != null)"},
       {R"((null if x == "-" else x) == "-")",
        R"(false if x == "-" else x == "-")"},
       {R"(not ((null if x == "-" else x) == "-"))",
        R"(not (false if x == "-" else x == "-"))"},
       {R"((null if x == 0 else x) == 0.0)",
        R"(false if x == 0 else x == 0.0)"},
       {R"((null if x == 0 else x) == 1.0)", R"(x == 1.0)"}}};
  for (auto [original, rewritten] : pairs) {
    auto const expected = eval_expr(original);
    auto const actual = eval_expr(rewritten);
    REQUIRE_EQUAL(actual.length(), expected.length());
    for (auto row = nova::storage::Index{0}; row < length; ++row) {
      CHECK(nova::equal(actual.get(row), expected.get(row)));
    }
  }
}

TEST("coalescing else does not turn into ternary if-else under rewrite or "
     "simplification") {
  // Replacing `y` in `y else x` with `b if c` would produce `(b if c) else x`,
  // which TQL evaluates as a ternary instead of null-coalescing.
  auto const fields
    = [](ast::field_path const& field) -> Option<ast::expression> {
    if (field.path().size() == 1 and field.path().front().id.name == "y") {
      return parse("b if c");
    }
    return field.inner();
  };
  auto const& reg = test::builtin_registry();
  CHECK(not ast::substitute_refs(parse("(y else x) == 1"), reg, fields));
  // An existing ternary stays a ternary when its branches are substituted.
  auto const sub
    = ast::substitute_refs(parse("(a if y else x) == 1"), reg, fields);
  REQUIRE(sub);
  CHECK_EQUAL(fmt::format("{:?}", *sub), render("(a if (b if c) else x) == 1"));
  // Simplifying the left operand of a coalescing `else` must not expose a bare
  // `if_` directly on the left, which would turn coalescing into a ternary.
  CHECK_EQUAL(simplify("((null else (b? if c)) else x) == 1"),
              render("((null else (b? if c)) else x) == 1"));
  CHECK_EQUAL(simplify("(((b? if c) if true else z) else x) == 1"),
              render("(((b? if c) if true else z) else x) == 1"));
  CHECK_EQUAL(simplify("((z if false else (b? if c)) else x) == 1"),
              render("((z if false else (b? if c)) else x) == 1"));
  CHECK_EQUAL(simplify("((null else (null else (b? if c))) else x) == 1"),
              render("((null else (b? if c)) else x) == 1"));
  CHECK_EQUAL(simplify("(((null else null) else (b? if c)) else x) == 1"),
              render("((null else (b? if c)) else x) == 1"));
  CHECK_EQUAL(simplify("(((1 + 2 if c) if true else z) else x) == 1"),
              render("(((3 if c) if true else z) else x) == 1"));
  CHECK_EQUAL(simplify("(((null else (b? if c)) if true else z) else x) == 1"),
              render("(((b? if c) if true else z) else x) == 1"));
  // Simplifications that do not place a bare `if_` on the left of a coalescing
  // `else` still apply.
  CHECK_EQUAL(simplify("(null else (b? if c)) == 1"), render("(b? if c) == 1"));
  CHECK_EQUAL(simplify("(a else (null else (b? if c))) == 1"),
              render("(a else (b? if c)) == 1"));
  CHECK_EQUAL(simplify("((1 else (b? if c)) else x) == 1"), render("true"));
  CHECK_EQUAL(simplify("(((b? if c) if false else z) else x) == 1"),
              render("(z else x) == 1"));
  CHECK_EQUAL(simplify("((null else y) else x) == 1"),
              render("(y else x) == 1"));
  CHECK_EQUAL(simplify("((null else (b? if c)) if d else x) == 1"),
              render("((b? if c) if d else x) == 1"));
}

TEST("simplifying long coalescing else chains") {
  constexpr auto chain_length = 2000;
  // A long left-associative chain starting with `null else (b? if c)` keeps its
  // coalescing guard intact across all terms.
  auto guarded = std::string{"(null else (b? if c))"};
  for (auto i = 0; i < chain_length; ++i) {
    guarded += fmt::format(" else x{}", i);
  }
  guarded = fmt::format("({}) == 1", guarded);
  CHECK_EQUAL(simplify(guarded), render(guarded));
  // A long left-associative chain with a foldable `null else` prefix still
  // simplifies the prefix without losing any fallback terms.
  auto tail = std::string{"x0"};
  for (auto i = 1; i < chain_length; ++i) {
    tail += fmt::format(" else x{}", i);
  }
  CHECK_EQUAL(simplify(fmt::format("(null else {}) == 1", tail)),
              render(fmt::format("({}) == 1", tail)));
  // A long right-associative chain collapses a nested `null else (b? if c)`
  // on the right operand because it does not sit on the left of an `else`.
  auto const make_field = [](std::string name) {
    return ast::expression{
      ast::root_field{ast::identifier{std::move(name), location::unknown},
                      false},
    };
  };
  auto right_chain = parse("null else (b? if c)");
  auto expected_right = parse("b? if c");
  for (auto i = chain_length - 1; i >= 0; --i) {
    auto name = fmt::format("x{}", i);
    right_chain = ast::binary_expr{make_field(name), ast::binary_op::else_,
                                   std::move(right_chain)};
    expected_right
      = ast::binary_expr{make_field(std::move(name)), ast::binary_op::else_,
                         std::move(expected_right)};
  }
  auto const simplified
    = ir::simplify_predicate(std::move(right_chain), test::builtin_registry());
  CHECK_EQUAL(fmt::format("{:?}", simplified),
              fmt::format("{:?}", expected_right));
}

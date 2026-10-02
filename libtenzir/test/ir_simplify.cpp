//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/diagnostics.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/secret.hpp"
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

/// Renders the simplified predicate for comparison.
auto simplify(std::string_view source) -> std::string {
  return fmt::format("{:?}", ir::simplify_predicate(parse(source)));
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
  return render_chain(ir::simplify_filter(parse_chain(sources)));
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
  return fmt::format("{:?}", ir::simplify_predicate(parse_with_secret(source)));
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
  return render_chain(ir::simplify_filter(std::move(filter)));
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
  // Operations that warn at runtime stay.
  CHECK_EQUAL(simplify("42 == \"42\""), render("42 == \"42\""));
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

TEST("simplify keeps constants that the evaluator rejects") {
  // The evaluator fails the pipeline for a secret constant, even where it
  // evaluates no rows.
  CHECK_EQUAL(simplify_with_secret("s or true"),
              render_with_secret("s or true"));
  CHECK_EQUAL(simplify_with_secret("{a: s} == null or true"),
              render_with_secret("{a: s} == null or true"));
  CHECK_EQUAL(simplify_with_secret("[s] == x and false"),
              render_with_secret("[s] == x and false"));
  CHECK_EQUAL(simplify_with_secret("true if true else s"),
              render_with_secret("true if true else s"));
  CHECK_EQUAL(simplify_with_secret("(s if false) == null"),
              render_with_secret("(s if false) == null"));
  CHECK_EQUAL(simplify_with_secret("(1 else s) == 1"),
              render_with_secret("(1 else s) == 1"));
  // The runtime never evaluates the right operand of a decided `and` or `or`.
  CHECK_EQUAL(simplify_with_secret("true or s"), render("true"));
  CHECK_EQUAL(simplify_with_secret("false and s == x"), render("false"));
  // The secret stays where the runtime evaluates it.
  CHECK_EQUAL(simplify_with_secret("s and true"), render_with_secret("s"));
  CHECK_EQUAL(simplify_with_secret("(null else s) == x"),
              render_with_secret("s == x"));
  CHECK_EQUAL(simplify_filter_with_secret({"s", "x == 1", "false", "s"}),
              render_chain(ir::OptimizeFilter{parse_with_secret("s"),
                                              parse("false"),
                                              parse_with_secret("s")}));
}

TEST("simplify keeps the arguments of invalid calls") {
  // Some arguments are syntax rather than values, and simplifying them could
  // turn a rejected call into a valid one.
  CHECK_EQUAL(simplify("drop_null_fields({a: x, b: 1 + 1}, a if true) == y"),
              render("drop_null_fields({a: x, b: 1 + 1}, a if true) == y"));
  CHECK_EQUAL(simplify("count_if([1 + 1], (v => true) if true) == 1"),
              render("count_if([1 + 1], (v => true) if true) == 1"));
  CHECK_EQUAL(simplify("int(y, base=16 if true else x) == 1"),
              render("int(y, base=16 if true else x) == 1"));
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
  CHECK_EQUAL(simplify("int(int(y, base=16 if true else x).string()) == 1"),
              render("int(int(y, base=16 if true else x).string()) == 1"));
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
  CHECK_EQUAL(ir::expression_size(parse("x")), size_t{1});
  CHECK_EQUAL(ir::expression_size(parse("x == 1")), size_t{3});
  CHECK_EQUAL(ir::expression_size(parse("x.y == [1, 2]")), size_t{6});
}

TEST("substitution stops when the predicate grows beyond the bound") {
  auto const doubling
    = [](ast::field_path const& field) -> Option<ast::expression> {
    return ast::binary_expr{field.inner(), ast::binary_op::add, field.inner()};
  };
  auto predicate = parse("x == 1");
  auto steps = 0;
  while (true) {
    auto split = ir::split_filter_by_substitution({predicate}, doubling);
    if (split.independent.empty()) {
      break;
    }
    predicate = std::move(split.independent.front());
    ++steps;
  }
  // After `n` steps, the predicate has `2^(n + 1) + 1` nodes, and the bound is
  // 4096 nodes.
  CHECK_EQUAL(steps, 10);
  CHECK_EQUAL(ir::expression_size(predicate), size_t{2049});
  // A substitution that does not grow the predicate may exceed the bound.
  auto const rename = [](ast::field_path const&) -> Option<ast::expression> {
    return ast::root_field{ast::identifier{"y", location::unknown}};
  };
  auto large = parse("x");
  for (auto i = 0; i < 12; ++i) {
    large = ast::binary_expr{large, ast::binary_op::add, large};
  }
  large = ast::binary_expr{large, ast::binary_op::eq, parse("1")};
  REQUIRE_GREATER(ir::expression_size(large), size_t{4096});
  auto split = ir::split_filter_by_substitution({large}, rename);
  REQUIRE_EQUAL(split.independent.size(), size_t{1});
  CHECK_EQUAL(ir::expression_size(split.independent.front()),
              ir::expression_size(large));
}

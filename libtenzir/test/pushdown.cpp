//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/diagnostics.hpp"
#include "tenzir/pushdown/translate.hpp"
#include "tenzir/session.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/tql2/parser.hpp"
#include "tenzir/tql2/resolve.hpp"
#include "tenzir/try.hpp"
#include "tenzir/variant_traits.hpp"

#include <fmt/format.h>

#include <array>
#include <limits>
#include <span>
#include <string>
#include <string_view>

using namespace tenzir;
using namespace tenzir::pushdown;

namespace {

/// A dialect close to standard SQL. It spells time literals by their tick
/// count, has no address type, knows `starts_with` only, and, like DuckDB,
/// orders `NaN` above every other value, which it handles by vetoing.
class TestRenderer : public SqlRenderer {
protected:
  auto quote_identifier(std::string_view name) const -> std::string override {
    return fmt::format("\"{}\"", name);
  }

  auto quote_string(std::string_view text) const -> std::string override {
    auto result = std::string{"'"};
    for (auto c : text) {
      if (c == '\'') {
        result += '\'';
      }
      result += c;
    }
    result += '\'';
    return result;
  }

  auto render_double(double x) const -> std::string override {
    return fmt::format("{}", x);
  }

  auto render_time(TimeValue const& x) const -> Option<Fragment> override {
    return sql_call(x.type.native_type,
                    std::array{sql_atom(fmt::to_string(x.ticks))});
  }

  auto render_comparison(Binary const& x, Fragment const& left,
                         Fragment const& right) const
    -> Option<Fragment> override {
    if (x.may_be_nan) {
      return None{};
    }
    return SqlRenderer::render_comparison(x, left, right);
  }

  auto render_call(Operation op, std::span<Fragment const> args) const
    -> Option<Fragment> override {
    if (op != Operation::starts_with) {
      return None{};
    }
    return sql_call("starts_with", args);
  }
};

/// A dialect like `TestRenderer` that guards comparisons that may be `NaN`
/// instead of vetoing them, which turns them into junctions, and spells
/// `contains` as a comparison. It computes arithmetic in the types of the
/// operands and has no way to widen them, so it vetoes all arithmetic.
class GuardingRenderer final : public TestRenderer {
private:
  auto render_comparison(Binary const& x, Fragment const& left,
                         Fragment const& right) const
    -> Option<Fragment> override {
    TRY(auto comparison, SqlRenderer::render_comparison(x, left, right));
    if (not x.may_be_nan) {
      return comparison;
    }
    auto checks = std::vector<Fragment>{};
    for (auto [operand, fragment] :
         {std::pair{&*x.left, &left}, std::pair{&*x.right, &right}}) {
      if (not is<Literal>(*operand)) {
        checks.push_back(sql_call("isnan", std::span{fragment, 1}));
      }
    }
    // IEEE 754 makes `!=` hold for `NaN` and every other comparison fail.
    auto parts = std::vector{std::move(comparison)};
    auto is_neq = x.op == BinaryOp::neq;
    for (auto& check : checks) {
      parts.push_back(is_neq ? std::move(check) : sql_not(check));
    }
    return sql_junction(is_neq ? "OR" : "AND", parts);
  }

  auto render_arithmetic(Binary const&, Fragment const&, Fragment const&) const
    -> Option<Fragment> override {
    return None{};
  }

  auto render_call(Operation op, std::span<Fragment const> args) const
    -> Option<Fragment> override {
    if (op == Operation::contains) {
      return sql_binary(">", sql_call("position", args), sql_atom("0"),
                        Fragment::Kind::predicate);
    }
    return TestRenderer::render_call(op, args);
  }
};

auto make_columns() -> ColumnModel {
  constexpr auto max = std::numeric_limits<int64_t>::max();
  auto result = ColumnModel{};
  result.add({"x"}, {.type = IntType{.bits = 64, .is_signed = true},
                     .nullable = false});
  result.add({"n"}, {.type = IntType{.bits = 32, .is_signed = true},
                     .nullable = true});
  result.add({"i"}, {.type = IntType{.bits = 32, .is_signed = true},
                     .nullable = false});
  result.add({"s"}, {.type = StringType{}, .nullable = true});
  result.add({"r", "a"}, {.type = FloatType{}, .nullable = false});
  result.add({"r", "i"}, {.type = IntType{.bits = 64, .is_signed = false},
                          .nullable = false});
  result.add({"addr"}, {.type = IpType{.v4 = false}, .nullable = false});
  result.add({"t"}, {.type = TimeType{.native_type = "MS",
                                      .unit = 1'000'000,
                                      .lo = -(max / 1'000'000),
                                      .hi = max / 1'000'000,
                                      .guard_lo = true,
                                      .guard_hi = true},
                     .nullable = false});
  return result;
}

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

auto render(Option<Expr> const& expr) -> Option<std::string> {
  if (not expr) {
    return None{};
  }
  return TestRenderer{}.render(*expr);
}

auto translate(std::string_view source) -> Option<std::string> {
  return render(translate_predicate(parse(source), make_columns()));
}

auto translate_guarded(std::string_view source) -> Option<std::string> {
  auto expr = translate_predicate(parse(source), make_columns());
  if (not expr) {
    return None{};
  }
  return GuardingRenderer{}.render(*expr);
}

auto column(std::string name) -> Expr {
  return Column{{std::move(name)}};
}

template <class T>
auto lit(T x) -> Expr {
  return Literal{std::move(x)};
}

auto sub(Expr left, Expr right) -> Expr {
  return Binary{BinaryOp::sub, std::move(left), std::move(right)};
}

} // namespace

TEST("sql renderer spells predicates") {
  CHECK_EQUAL(translate("x > 1"), std::string{"\"x\" > 1"});
  CHECK_EQUAL(translate("r.i <= 1.5"), std::string{"\"r\".\"i\" <= 1.5"});
  CHECK_EQUAL(translate("s == \"it's\""),
              std::string{"(\"s\" IS NOT NULL AND \"s\" = 'it''s')"});
  CHECK_EQUAL(translate("n in [1, 2]"),
              std::string{"(\"n\" IS NOT NULL AND \"n\" IN (1, 2))"});
  CHECK_EQUAL(translate("s.starts_with(\"a\")"),
              std::string{"starts_with(\"s\", 'a')"});
  CHECK_EQUAL(translate("n + 1 == x"),
              std::string{"((\"n\" + 1) IS NOT NULL AND (\"n\" + 1) = \"x\")"});
}

TEST("sql renderer groups fragments by kind and slot") {
  // Every junction is parenthesized, so that it joins with other predicates as
  // is, and keeps the grouping of the source.
  CHECK_EQUAL(translate("x > 0 or n == null"),
              std::string{"(\"x\" > 0 OR \"n\" IS NULL)"});
  CHECK_EQUAL(translate("x > 0 or x < 2 or x == 1"),
              std::string{"((\"x\" > 0 OR \"x\" < 2) OR \"x\" = 1)"});
  CHECK_EQUAL(translate("not (x > 0 and r.i < 1.0)"),
              std::string{"NOT (\"x\" > 0 AND \"r\".\"i\" < 1)"});
  CHECK_EQUAL(translate("not (x > 0)"), std::string{"NOT \"x\" > 0"});
  // Guards share the group of the comparison they guard, and need none as
  // the condition of a conditional.
  CHECK_EQUAL(translate("t > 1970-01-01"),
              std::string{"(\"t\" >= MS(-9223372036854) AND \"t\" <= "
                          "MS(9223372036854) AND \"t\" > MS(0))"});
  CHECK_EQUAL(translate("not (t > 1970-01-01)"),
              std::string{"NOT CASE WHEN \"t\" >= MS(-9223372036854) AND \"t\" "
                          "<= MS(9223372036854) THEN \"t\" > MS(0) ELSE NULL "
                          "END"});
  // Predicates and `NOT` are parenthesized as operands of predicates.
  auto nested = Expr{
    Binary{BinaryOp::eq,
           Expr{Binary{BinaryOp::gt, column("x"), Expr{Literal{int64_t{1}}}}},
           Expr{Literal{true}}}};
  CHECK_EQUAL(TestRenderer{}.render(nested), std::string{"(\"x\" > 1) = true"});
  auto negated = Expr{IsNull{Expr{Not{column("x")}}, false}};
  CHECK_EQUAL(TestRenderer{}.render(negated),
              std::string{"(NOT \"x\") IS NULL"});
  // A call is an atom even where it yields a boolean.
  auto prefix = Expr{IsNull{
    Expr{Call{Operation::starts_with, {column("s"), lit(std::string{"a"})}}},
    false}};
  CHECK_EQUAL(TestRenderer{}.render(prefix),
              std::string{"starts_with(\"s\", 'a') IS NULL"});
}

TEST("sql renderer groups nested arithmetic") {
  auto right = sub(column("x"), sub(column("n"), lit(int64_t{1})));
  CHECK_EQUAL(TestRenderer{}.render(right),
              std::string{"(\"x\" - (\"n\" - 1))"});
  auto left = sub(sub(column("x"), column("n")), lit(int64_t{1}));
  CHECK_EQUAL(TestRenderer{}.render(left),
              std::string{"((\"x\" - \"n\") - 1)"});
  auto compared = Expr{Binary{BinaryOp::gt, std::move(right), lit(int64_t{0})}};
  CHECK_EQUAL(TestRenderer{}.render(compared),
              std::string{"(\"x\" - (\"n\" - 1)) > 0"});
  // Delimited slots need no grouping.
  auto conditional
    = Expr{Conditional{Expr{Binary{BinaryOp::gt, column("x"), lit(int64_t{0})}},
                       sub(column("x"), lit(int64_t{1})), lit(Null{})}};
  CHECK_EQUAL(TestRenderer{}.render(conditional),
              std::string{"CASE WHEN \"x\" > 0 THEN \"x\" - 1 ELSE NULL END"});
}

TEST("sql renderer groups operations spelled as comparisons") {
  CHECK_EQUAL(translate_guarded("x > 0 and \"a\" in s"),
              std::string{"(\"x\" > 0 AND position(\"s\", 'a') > 0)"});
  CHECK_EQUAL(translate_guarded("not (\"a\" in s)"),
              std::string{"NOT position(\"s\", 'a') > 0"});
  auto contains = Expr{IsNull{
    Expr{Call{Operation::contains, {column("s"), lit(std::string{"a"})}}},
    false}};
  CHECK_EQUAL(GuardingRenderer{}.render(contains),
              std::string{"(position(\"s\", 'a') > 0) IS NULL"});
}

TEST("sql renderer groups comparisons spelled as junctions") {
  CHECK_EQUAL(translate_guarded("r.a > 0.5"),
              std::string{"(\"r\".\"a\" > 0.5 AND NOT isnan(\"r\".\"a\"))"});
  CHECK_EQUAL(translate_guarded("x > 0 or r.a > 0.5"),
              std::string{"(\"x\" > 0 OR (\"r\".\"a\" > 0.5 AND NOT "
                          "isnan(\"r\".\"a\")))"});
  CHECK_EQUAL(translate_guarded("not (r.a > 0.5)"),
              std::string{"NOT (\"r\".\"a\" > 0.5 AND NOT "
                          "isnan(\"r\".\"a\"))"});
  CHECK_EQUAL(translate_guarded("not (r.a != 1.5)"),
              std::string{"NOT (\"r\".\"a\" != 1.5 OR isnan(\"r\".\"a\"))"});
  auto compared = Expr{
    IsNull{Expr{Binary{BinaryOp::gt, Expr{Column{{"r", "a"}}}, lit(0.5), true}},
           false}};
  CHECK_EQUAL(GuardingRenderer{}.render(compared),
              std::string{"(\"r\".\"a\" > 0.5 AND NOT isnan(\"r\".\"a\")) "
                          "IS NULL"});
}

TEST("sql renderer vetoes what the dialect cannot spell") {
  CHECK(not translate("addr == ::1"));
  CHECK(not translate("addr in 10.0.0.0/8"));
  CHECK(not translate("\"a\" in s"));
  CHECK(not translate("s.length_bytes() > 1"));
  CHECK(not translate("x > 0 or addr == ::1"));
  CHECK(not TestRenderer{}.render(Expr{In{column("x"), {}}}));
}

TEST("sql renderer lets dialects veto comparisons that may be nan") {
  CHECK(not translate("r.a <= 1.5"));
  CHECK(not translate("r.a == 1"));
  CHECK(not translate("r.a != r.a"));
  CHECK(not translate("r.a + 1.0 > 2.0"));
  CHECK(not translate("x > 0 or r.a > 0.5"));
  // Integers and literals are never `NaN`, and membership compares with
  // literals only.
  CHECK_EQUAL(translate("x > 1.5"), std::string{"\"x\" > 1.5"});
  CHECK_EQUAL(translate("r.i == r.i"),
              std::string{"\"r\".\"i\" = \"r\".\"i\""});
  CHECK_EQUAL(translate("r.a in [1.5, 2.5]"),
              std::string{"\"r\".\"a\" IN (1.5, 2.5)"});
}

TEST("vetoed conjuncts stay local") {
  auto filter = ir::OptimizeFilter{};
  filter.push_back(parse("x > 0 and addr == ::1"));
  filter.push_back(parse("s.ip() in 10.0.0.0/8"));
  filter.push_back(parse("s.starts_with(\"a\")"));
  filter.push_back(parse("r.a > 0.5"));
  auto split = split_filter(std::move(filter), make_columns(), TestRenderer{});
  REQUIRE_EQUAL(split.pushed.size(), size_t{2});
  CHECK_EQUAL(split.pushed[0], std::string{"\"x\" > 0"});
  CHECK_EQUAL(split.pushed[1], std::string{"starts_with(\"s\", 'a')"});
  // The address and float comparisons have exact translations and the subnet
  // test a prefilter, but the renderer can spell none of them.
  REQUIRE_EQUAL(split.remaining.size(), size_t{3});
  auto const* comparison = try_as<ast::binary_expr>(split.remaining[0]);
  REQUIRE(comparison);
  CHECK(comparison->op == ast::binary_op::eq);
  auto const* membership = try_as<ast::binary_expr>(split.remaining[1]);
  REQUIRE(membership);
  CHECK(membership->op == ast::binary_op::in);
  auto const* ordering = try_as<ast::binary_expr>(split.remaining[2]);
  REQUIRE(ordering);
  CHECK(ordering->op == ast::binary_op::gt);
}

TEST("vetoed arithmetic keeps its conjunct local") {
  auto filter = ir::OptimizeFilter{};
  filter.push_back(parse("i + 1 in [2, 3]"));
  filter.push_back(parse("x > 0"));
  filter.push_back(parse("n - 1 == null"));
  // Both translate, with the arithmetic below `IN` and `IS NULL`.
  auto columns = make_columns();
  CHECK_EQUAL(render(translate_predicate(filter[0], columns)),
              std::string{"(\"i\" + 1) IN (2, 3)"});
  CHECK_EQUAL(render(translate_predicate(filter[2], columns)),
              std::string{"(\"n\" - 1) IS NULL"});
  auto split = split_filter(std::move(filter), columns, GuardingRenderer{});
  REQUIRE_EQUAL(split.pushed.size(), size_t{1});
  CHECK_EQUAL(split.pushed[0], std::string{"\"x\" > 0"});
  REQUIRE_EQUAL(split.remaining.size(), size_t{2});
  auto const* membership = try_as<ast::binary_expr>(split.remaining[0]);
  REQUIRE(membership);
  CHECK(membership->op == ast::binary_op::in);
  auto const* null_check = try_as<ast::binary_expr>(split.remaining[1]);
  REQUIRE(null_check);
  CHECK(null_check->op == ast::binary_op::eq);
}

//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/expression.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/time.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/parser.hpp"

#include <string_view>

using namespace tenzir;

TEST("optimize now") {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto s = session{provider};
  const auto lhs = operand{field_extractor{"x"}};
  const auto op = relational_operator::greater;
  const auto test = [&](std::string_view str, bool pos = false) {
    auto expr = parse_expression_with_bad_diagnostics(str, s);
    REQUIRE(expr);
    const auto& [legacy, _] = split_legacy_expression(std::move(expr).unwrap());
    const auto t = time::clock::now() + (pos ? days{100} : -days{100});
    const auto rhs = operand{data{tenzir::time{t}}};
    const auto* p = try_as<predicate>(&legacy.get_data());
    REQUIRE(p);
    CHECK_EQUAL(p->lhs, lhs);
    CHECK_EQUAL(p->op, op);
    CHECK_LESS_EQUAL(p->rhs, rhs);
  };
  test("x > now() - 100d");
  test("x > now() + 100d", true);
  test("x > 100d + now()", true);
  test("now() - 100d < x");
  test("now() + 100d < x", true);
  test("100d + now() < x", true);
}

TEST("split aligns substring in predicate") {
  // Regression test: when a predicate with a literal on the left-hand side is
  // split off for predicate pushdown, the result must be normalized so that the
  // extractor ends up on the left with the operator flipped accordingly.
  // Otherwise pushing the filter into a source that does not re-normalize it
  // (e.g. `subscribe`) inverts the substring check and filters out everything.
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto s = session{provider};
  auto expr
    = parse_expression_with_bad_diagnostics(R"("needle" in haystack)", s);
  REQUIRE(expr);
  const auto& [legacy, remainder]
    = split_legacy_expression(std::move(expr).unwrap());
  CHECK(is_true_literal(remainder));
  const auto* p = try_as<predicate>(&legacy.get_data());
  REQUIRE(p);
  CHECK_EQUAL(p->lhs, operand{field_extractor{"haystack"}});
  CHECK_EQUAL(p->op, relational_operator::ni);
  CHECK_EQUAL(p->rhs, operand{data{std::string{"needle"}}});
}

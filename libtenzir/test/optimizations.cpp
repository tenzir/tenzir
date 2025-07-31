//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/expression.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/time.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/parser.hpp"

#include <string>
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
  // test("-100d + now() < x");
  // test("x > -100d + now()");
}

//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/expression.hpp"
#define SUITE expression

#include "vast/fwd.hpp"

#include "vast/test/test.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/parseable/vast/subnet.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/fmt_integration.hpp"

#include <caf/sum_type.hpp>

using namespace vast;
using namespace std::string_literals;

TEST(parseable / printable - predicate) {
  predicate pred;
  // LHS: schema, RHS: data
  MESSAGE("x.y.z == 42");
  std::string str = "x.y.z == 42";
  CHECK(parsers::predicate(str, pred));
  CHECK(caf::holds_alternative<field_extractor>(pred.lhs));
  CHECK(caf::holds_alternative<data>(pred.rhs));
  CHECK(caf::get<field_extractor>(pred.lhs) == field_extractor{"x.y.z"});
  CHECK(pred.op == relational_operator::equal);
  CHECK(caf::get<data>(pred.rhs) == data{42u});
  CHECK_EQUAL(to_string(pred), str);
  // LHS: data, RHS: schema
  MESSAGE("T.x == Foo");
  str = "T.x == Foo";
  CHECK(parsers::predicate(str, pred));
  CHECK(caf::holds_alternative<field_extractor>(pred.lhs));
  CHECK(caf::holds_alternative<field_extractor>(pred.rhs));
  CHECK(caf::get<field_extractor>(pred.lhs) == field_extractor{"T.x"});
  CHECK(caf::get<field_extractor>(pred.rhs) == field_extractor{"Foo"});
  CHECK(pred.op == relational_operator::equal);
  CHECK_EQUAL(to_string(pred), str);
  // LHS: data, RHS: data
  MESSAGE("42 in [21, 42, 84]");
  str = "42 in [21, 42, 84]";
  CHECK(parsers::predicate(str, pred));
  CHECK(caf::holds_alternative<data>(pred.lhs));
  CHECK(caf::holds_alternative<data>(pred.rhs));
  CHECK(caf::get<data>(pred.lhs) == data{42u});
  CHECK(pred.op == relational_operator::in);
  CHECK(caf::get<data>(pred.rhs) == data{list{21u, 42u, 84u}});
  CHECK_EQUAL(to_string(pred), str);
  // LHS: type, RHS: data
  MESSAGE("#type != \"foo\"");
  str = "#type != \"foo\"";
  CHECK(parsers::predicate(str, pred));
  CHECK(caf::holds_alternative<meta_extractor>(pred.lhs));
  CHECK(caf::holds_alternative<data>(pred.rhs));
  CHECK(caf::get<meta_extractor>(pred.lhs)
        == meta_extractor{meta_extractor::type});
  CHECK(pred.op == relational_operator::not_equal);
  CHECK(caf::get<data>(pred.rhs) == data{"foo"});
  CHECK_EQUAL(to_string(pred), str);
  // LHS: data, RHS: type
  MESSAGE("10.0.0.0/8 ni :addr");
  str = "10.0.0.0/8 ni :addr";
  CHECK(parsers::predicate(str, pred));
  CHECK(caf::holds_alternative<data>(pred.lhs));
  CHECK(caf::holds_alternative<type_extractor>(pred.rhs));
  CHECK(caf::get<data>(pred.lhs) == data{*to<subnet>("10.0.0.0/8")});
  CHECK(pred.op == relational_operator::ni);
  CHECK(caf::get<type_extractor>(pred.rhs) == type_extractor{address_type{}});
  CHECK_EQUAL(to_string(pred), str);
  // LHS: type, RHS: data
  MESSAGE(":real >= -4.8");
  str = ":real >= -4.8";
  CHECK(parsers::predicate(str, pred));
  CHECK(caf::holds_alternative<type_extractor>(pred.lhs));
  CHECK(caf::holds_alternative<data>(pred.rhs));
  CHECK(caf::get<type_extractor>(pred.lhs) == type_extractor{real_type{}});
  CHECK(pred.op == relational_operator::greater_equal);
  CHECK(caf::get<data>(pred.rhs) == data{-4.8});
  CHECK_EQUAL(to_string(pred), str);
  // LHS: data, RHS: typename
  MESSAGE("\"zeek.\" in #type");
  str = "\"zeek.\" in #type";
  CHECK(parsers::predicate(str, pred));
  CHECK(caf::holds_alternative<data>(pred.lhs));
  CHECK(caf::holds_alternative<meta_extractor>(pred.rhs));
  CHECK(pred.op == relational_operator::in);
  CHECK(caf::get<meta_extractor>(pred.rhs)
        == meta_extractor{meta_extractor::type});
  // LHS: schema, RHS: schema
  MESSAGE("x.a_b == y.c_d");
  str = "x.a_b == y.c_d";
  CHECK(parsers::predicate(str, pred));
  CHECK(caf::holds_alternative<field_extractor>(pred.lhs));
  CHECK(caf::holds_alternative<field_extractor>(pred.rhs));
  CHECK(caf::get<field_extractor>(pred.lhs) == field_extractor{"x.a_b"});
  CHECK(pred.op == relational_operator::equal);
  CHECK(caf::get<field_extractor>(pred.rhs) == field_extractor{"y.c_d"});
  CHECK_EQUAL(to_string(pred), str);
  // User defined type name:
  MESSAGE(":foo == -42");
  CHECK(parsers::predicate(":foo == -42"));
}

TEST(parseable - expression) {
  expression expr;
  predicate p1{field_extractor{"x"}, relational_operator::equal, data{42u}};
  predicate p2{type_extractor{real_type{}}, relational_operator::equal,
               data{real{5.3}}};
  predicate p3{field_extractor{"a"}, relational_operator::greater,
               field_extractor{"b"}};
  MESSAGE("conjunction");
  CHECK(parsers::expr("x == 42 && :real == 5.3"s, expr));
  CHECK_EQUAL(expr, expression(conjunction{p1, p2}));
  CHECK(parsers::expr("x == 42 && :real == 5.3 && x == 42"s, expr));
  CHECK_EQUAL(expr, expression(conjunction{p1, p2, p1}));
  CHECK(parsers::expr("x == 42 && ! :real == 5.3 && x == 42"s, expr));
  CHECK_EQUAL(expr, expression(conjunction{p1, negation{p2}, p1}));
  CHECK(parsers::expr("x > 0 && x < 42 && a.b == x.y"s, expr));
  CHECK(parsers::expr(
    ":timestamp > 2018-07-04+12:00:00.0 && :timestamp < 2018-07-04+23:55:04.0"s,
    expr));
  auto x = caf::get_if<conjunction>(&expr);
  REQUIRE(x);
  REQUIRE_EQUAL(x->size(), 2u);
  auto x0 = caf::get_if<predicate>(&x->at(0));
  auto x1 = caf::get_if<predicate>(&x->at(1));
  CHECK(caf::holds_alternative<type_extractor>(x0->lhs));
  CHECK(caf::holds_alternative<type_extractor>(x1->lhs));
  MESSAGE("disjunction");
  CHECK(parsers::expr("x == 42 || :real == 5.3 || x == 42"s, expr));
  CHECK_EQUAL(expr, expression(disjunction{p1, p2, p1}));
  CHECK(parsers::expr("a==b || b==c || c==d"s, expr));
  MESSAGE("negation");
  CHECK(parsers::expr("! x == 42"s, expr));
  CHECK_EQUAL(expr, expression(negation{p1}));
  CHECK(parsers::expr("!(x == 42 || :real == 5.3)"s, expr));
  CHECK_EQUAL(expr, expression(negation{disjunction{p1, p2}}));
  MESSAGE("parentheses");
  CHECK(parsers::expr("(x == 42)"s, expr));
  CHECK_EQUAL(expr, p1);
  CHECK(parsers::expr("((x == 42))"s, expr));
  CHECK_EQUAL(expr, p1);
  CHECK(parsers::expr("x == 42 && (x == 42 || a > b)"s, expr));
  CHECK_EQUAL(expr, expression(conjunction{p1, disjunction{p1, p3}}));
  CHECK(parsers::expr("x == 42 && x == 42 || a > b && x == 42"s, expr));
  expression expected = disjunction{conjunction{p1, p1}, conjunction{p3, p1}};
  CHECK_EQUAL(expr, expected);
  MESSAGE("stray dot regression");
  // This should fail to parse because of the stray dot.
  CHECK(!parsers::expr(
    "#type == \"suricata.http\" && .community_id == \"1:Y3MTSbNCzFAT3I5+i6xzSgrL59k=\""s,
    expr));
}

TEST(parseable - value predicate) {
  expression expr;
  CHECK(parsers::expr("42"s, expr));
  auto pred = caf::get_if<predicate>(&expr);
  REQUIRE(pred != nullptr);
  auto extractor = caf::get_if<type_extractor>(&pred->lhs);
  REQUIRE(extractor != nullptr);
  CHECK(caf::holds_alternative<count_type>(extractor->type));
  CHECK(caf::holds_alternative<data>(pred->rhs));
  CHECK_EQUAL(pred->op, relational_operator::equal);
  CHECK(caf::get<data>(pred->rhs) == data{42u});
}

//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/fwd.hpp"

#include "tenzir/concept/parseable/tenzir/expression.hpp"
#include "tenzir/concept/parseable/tenzir/subnet.hpp"
#include "tenzir/concept/parseable/to.hpp"
#include "tenzir/concept/printable/stream.hpp"
#include "tenzir/concept/printable/tenzir/expression.hpp"
#include "tenzir/concept/printable/to_string.hpp"
#include "tenzir/expression.hpp"
#include "tenzir/test/test.hpp"

#include <caf/sum_type.hpp>

using namespace tenzir;
using namespace std::string_literals;

TEST(parseable / printable - predicate) {
  predicate pred;
  // LHS: schema, RHS: data
  MESSAGE("x.y.z == 42");
  std::string str = "x.y.z == 42";
  CHECK(parsers::predicate(str, pred));
  CHECK(caf::holds_alternative<field_extractor>(pred.lhs));
  CHECK(caf::holds_alternative<data>(pred.rhs));
  CHECK(as<field_extractor>(pred.lhs) == field_extractor{"x.y.z"});
  CHECK(pred.op == relational_operator::equal);
  CHECK(as<data>(pred.rhs) == data{42u});
  CHECK_EQUAL(to_string(pred), str);
  // LHS: data, RHS: schema
  MESSAGE("T.x == Foo");
  str = "T.x == Foo";
  CHECK(parsers::predicate(str, pred));
  CHECK(caf::holds_alternative<field_extractor>(pred.lhs));
  CHECK(caf::holds_alternative<field_extractor>(pred.rhs));
  CHECK(as<field_extractor>(pred.lhs) == field_extractor{"T.x"});
  CHECK(as<field_extractor>(pred.rhs) == field_extractor{"Foo"});
  CHECK(pred.op == relational_operator::equal);
  CHECK_EQUAL(to_string(pred), str);
  // LHS: data, RHS: data
  MESSAGE("42 in [21, 42, 84]");
  str = "42 in [21, 42, 84]";
  CHECK(parsers::predicate(str, pred));
  CHECK(caf::holds_alternative<data>(pred.lhs));
  CHECK(caf::holds_alternative<data>(pred.rhs));
  CHECK(as<data>(pred.lhs) == data{42u});
  CHECK(pred.op == relational_operator::in);
  CHECK((as<data>(pred.rhs) == data{list{21u, 42u, 84u}}));
  CHECK_EQUAL(to_string(pred), str);
  // LHS: type, RHS: data
  MESSAGE("#schema != \"foo\"");
  str = "#schema != \"foo\"";
  CHECK(parsers::predicate(str, pred));
  CHECK(caf::holds_alternative<meta_extractor>(pred.lhs));
  CHECK(caf::holds_alternative<data>(pred.rhs));
  CHECK(as<meta_extractor>(pred.lhs) == meta_extractor{meta_extractor::schema});
  CHECK(pred.op == relational_operator::not_equal);
  CHECK(as<data>(pred.rhs) == data{"foo"});
  CHECK_EQUAL(to_string(pred), str);
  // LHS: data, RHS: type
  MESSAGE("10.0.0.0/8 ni :ip");
  str = "10.0.0.0/8 ni :ip";
  CHECK(parsers::predicate(str, pred));
  CHECK(caf::holds_alternative<data>(pred.lhs));
  CHECK(caf::holds_alternative<type_extractor>(pred.rhs));
  CHECK(as<data>(pred.lhs) == data{*to<subnet>("10.0.0.0/8")});
  CHECK(pred.op == relational_operator::ni);
  CHECK(as<type_extractor>(pred.rhs) == type_extractor{type{ip_type{}}});
  CHECK_EQUAL(to_string(pred), str);
  // LHS: type, RHS: data
  MESSAGE(":double >= -4.8");
  str = ":double >= -4.8";
  CHECK(parsers::predicate(str, pred));
  CHECK(caf::holds_alternative<type_extractor>(pred.lhs));
  CHECK(caf::holds_alternative<data>(pred.rhs));
  CHECK(as<type_extractor>(pred.lhs) == type_extractor{type{double_type{}}});
  CHECK(pred.op == relational_operator::greater_equal);
  CHECK(as<data>(pred.rhs) == data{-4.8});
  CHECK_EQUAL(to_string(pred), str);
  // LHS: data, RHS: typename
  MESSAGE("\"zeek.\" in #schema");
  str = "\"zeek.\" in #schema";
  CHECK(parsers::predicate(str, pred));
  CHECK(caf::holds_alternative<data>(pred.lhs));
  CHECK(caf::holds_alternative<meta_extractor>(pred.rhs));
  CHECK(pred.op == relational_operator::in);
  CHECK(as<meta_extractor>(pred.rhs) == meta_extractor{meta_extractor::schema});
  // LHS: schema, RHS: schema
  MESSAGE("x.a_b == y.c_d");
  str = "x.a_b == y.c_d";
  CHECK(parsers::predicate(str, pred));
  CHECK(caf::holds_alternative<field_extractor>(pred.lhs));
  CHECK(caf::holds_alternative<field_extractor>(pred.rhs));
  CHECK(as<field_extractor>(pred.lhs) == field_extractor{"x.a_b"});
  CHECK(pred.op == relational_operator::equal);
  CHECK(as<field_extractor>(pred.rhs) == field_extractor{"y.c_d"});
  CHECK_EQUAL(to_string(pred), str);
  // User defined type name:
  MESSAGE(":foo == -42");
  CHECK(parsers::predicate(":foo == -42"));
}

TEST(parseable - expression) {
  expression expr;
  predicate p1{field_extractor{"x"}, relational_operator::equal, data{42u}};
  predicate p2{type_extractor{type{double_type{}}}, relational_operator::equal,
               data{double{5.3}}};
  predicate p3{field_extractor{"a"}, relational_operator::greater,
               field_extractor{"b"}};
  MESSAGE("conjunction");
  CHECK(parsers::expr("x == 42 && :double == 5.3"s, expr));
  CHECK_EQUAL(expr, expression(conjunction{p1, p2}));
  CHECK(parsers::expr("x == 42 && :double == 5.3 && x == 42"s, expr));
  CHECK_EQUAL(expr, expression(conjunction{p1, p2, p1}));
  CHECK(parsers::expr("x == 42 && ! :double == 5.3 && x == 42"s, expr));
  CHECK_EQUAL(expr, expression(conjunction{p1, negation{p2}, p1}));
  CHECK(parsers::expr("x > 0 && x < 42 && a.b == x.y"s, expr));
  CHECK(parsers::expr(
    ":timestamp > 2018-07-04+12:00:00.0 && :timestamp < 2018-07-04+23:55:04.0"s,
    expr));
  auto x = try_as<conjunction>(&expr);
  REQUIRE(x);
  REQUIRE_EQUAL(x->size(), 2u);
  auto x0 = try_as<predicate>(&x->at(0));
  auto x1 = try_as<predicate>(&x->at(1));
  CHECK(caf::holds_alternative<type_extractor>(x0->lhs));
  CHECK(caf::holds_alternative<type_extractor>(x1->lhs));
  MESSAGE("disjunction");
  CHECK(parsers::expr("x == 42 || :double == 5.3 || x == 42"s, expr));
  CHECK_EQUAL(expr, expression(disjunction{p1, p2, p1}));
  CHECK(parsers::expr("a==b || b==c || c==d"s, expr));
  MESSAGE("negation");
  CHECK(parsers::expr("! x == 42"s, expr));
  CHECK_EQUAL(expr, expression(negation{p1}));
  CHECK(parsers::expr("!(x == 42 || :double == 5.3)"s, expr));
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
    "#schema == \"suricata.http\" && .community_id == \"1:Y3MTSbNCzFAT3I5+i6xzSgrL59k=\""s,
    expr));
}

TEST(parseable - value predicate) {
  expression expr;
  CHECK(parsers::expr("42"s, expr));
  auto disj = try_as<disjunction>(&expr);
  REQUIRE(disj);
  REQUIRE_EQUAL(disj->size(), 3u);
  {
    auto pred = try_as<predicate>(&(*disj)[0]);
    REQUIRE(pred);
    auto extractor = try_as<type_extractor>(&pred->lhs);
    REQUIRE(extractor);
    CHECK(caf::holds_alternative<int64_type>(extractor->type));
    CHECK(caf::holds_alternative<data>(pred->rhs));
    CHECK_EQUAL(pred->op, relational_operator::equal);
    CHECK(as<data>(pred->rhs) == data{int64_t{42}});
  }
  {
    auto pred = try_as<predicate>(&(*disj)[1]);
    REQUIRE(pred);
    auto extractor = try_as<type_extractor>(&pred->lhs);
    REQUIRE(extractor);
    CHECK(caf::holds_alternative<uint64_type>(extractor->type));
    CHECK(caf::holds_alternative<data>(pred->rhs));
    CHECK_EQUAL(pred->op, relational_operator::equal);
    CHECK(as<data>(pred->rhs) == data{42u});
  }
  {
    auto pred = try_as<predicate>(&(*disj)[2]);
    REQUIRE(pred);
    auto extractor = try_as<type_extractor>(&pred->lhs);
    REQUIRE(extractor);
    CHECK(caf::holds_alternative<double_type>(extractor->type));
    CHECK(caf::holds_alternative<data>(pred->rhs));
    CHECK_EQUAL(pred->op, relational_operator::equal);
    CHECK(as<data>(pred->rhs) == data{42.0});
  }
}

TEST(parseable - field extractor predicate) {
  expression expr;
  CHECK(parsers::expr("foo.bar"s, expr));
  auto pred = try_as<predicate>(&expr);
  REQUIRE(pred);
  auto extractor = try_as<field_extractor>(&pred->lhs);
  REQUIRE(extractor);
  CHECK_EQUAL(extractor->field, "foo.bar");
  CHECK_EQUAL(pred->op, relational_operator::not_equal);
  CHECK_EQUAL(pred->rhs, operand{data{}});
}

TEST(parseable - type extractor predicate) {
  expression expr;
  CHECK(parsers::expr(":ip"s, expr));
  auto pred = try_as<predicate>(&expr);
  REQUIRE(pred);
  auto extractor = try_as<type_extractor>(&pred->lhs);
  REQUIRE(extractor);
  CHECK_EQUAL(extractor->type, ip_type{});
  CHECK_EQUAL(pred->op, relational_operator::not_equal);
  CHECK_EQUAL(pred->rhs, operand{data{}});
}

TEST(parseable - custom type extractor predicate) {
  expression expr;
  CHECK(parsers::expr(":foo.bar"s, expr));
  auto pred = try_as<predicate>(&expr);
  REQUIRE(pred);
  auto extractor = try_as<type_extractor>(&pred->lhs);
  REQUIRE(extractor);
  auto expected = type{"foo.bar", type{}};
  CHECK_EQUAL(extractor->type, expected);
  CHECK_EQUAL(pred->op, relational_operator::not_equal);
  CHECK_EQUAL(pred->rhs, operand{data{}});
}

TEST(parseable - comments in expressions) {
  expression expected_expr;
  CHECK(parsers::expr(
    R"(#schema == "foo" && (foo.bar != [1, 2, 3] || baz != <_, 3.0>))"s,
    expected_expr));
  expression expr;
  CHECK(parsers::expr(
    R"(#schema == "foo" && (foo.bar != [1, 2, 3] /*/*fo* /*/|| baz != <_, 3.0>))"s,
    expr));
  CHECK_EQUAL(expr, expected_expr);
  CHECK(parsers::expr(
    R"(#schema/**/==/******/"foo" && (foo.bar != [1, 2, 3] || baz != <_, 3.0>))"s,
    expr));
  CHECK_EQUAL(expr, expected_expr);
  CHECK(parsers::expr(
    R"(#schema == "foo"/* && x != null */&& (foo.bar != [1, 2, 3] || baz != <_, 3.0>))"s,
    expr));
  // TODO: Comments within list and record literals are not currently allowed
  // because that parser is used in quite a few places that do not parse just an
  // expression or a pipeline.
  // CHECK(parsers::expr(
  //   R"(#schema ==/**/"foo" && (foo.bar != [1, 2,/*0,*/ 3] || baz !=
  //   <_, 3.0>))"s, expr));
  // CHECK_EQUAL(expr, expected_expr);
  // CHECK(parsers::expr(
  //   R"(#schema ==/**/"foo" && (foo.bar != [1, 2,/*0,*/ 3] || baz != </**/_,
  //   /**/3.0>))"s, expr));
  // CHECK_EQUAL(expr, expected_expr);
}

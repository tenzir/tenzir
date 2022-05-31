//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE expression

#include "vast/expression.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/parseable/vast/schema.hpp"
#include "vast/concept/parseable/vast/subnet.hpp"
#include "vast/concept/parseable/vast/time.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/data.hpp"
#include "vast/detail/legacy_deserialize.hpp"
#include "vast/detail/serialize.hpp"
#include "vast/detail/stable_map.hpp"
#include "vast/expression_visitors.hpp"
#include "vast/module.hpp"
#include "vast/test/test.hpp"

#include <caf/test/dsl.hpp>

#include <string>

using caf::get;
using caf::get_if;
using caf::holds_alternative;

using namespace std::string_literals;
using namespace vast;

namespace {

template <class T>
expression to_expr(T&& x) {
  return unbox(to<expression>(std::forward<T>(x)));
}

struct fixture {
  fixture() {
    // expr0 := !(x.y.z <= 42 && #foo == T)
    auto p0 = predicate{extractor{"x.y.z"}, relational_operator::less_equal,
                        data{integer{42}}};
    auto p1 = predicate{selector{selector::field}, relational_operator::equal,
                        data{true}};
    auto conj = conjunction{p0, p1};
    expr0 = negation{conj};
    // expr0 || :real > 4.2
    auto p2 = predicate{type_extractor{type{real_type{}}},
                        relational_operator::greater_equal, data{4.2}};
    expr1 = disjunction{expr0, p2};
  }

  expression expr0;
  expression expr1;
};

} // namespace

FIXTURE_SCOPE(expr_tests, fixture)

TEST(construction) {
  auto n = caf::get_if<negation>(&expr0);
  REQUIRE(n);
  auto c = caf::get_if<conjunction>(&n->expr());
  REQUIRE(c);
  REQUIRE(c->size() == 2);
  auto p0 = caf::get_if<predicate>(&c->at(0));
  REQUIRE(p0);
  CHECK_EQUAL(get<extractor>(p0->lhs).value, "x.y.z");
  CHECK_EQUAL(p0->op, relational_operator::less_equal);
  CHECK_EQUAL(get<data>(p0->rhs), integer{42});
  auto p1 = caf::get_if<predicate>(&c->at(1));
  REQUIRE(p1);
  CHECK_EQUAL(get<selector>(p1->lhs).kind, selector::field);
  CHECK_EQUAL(p1->op, relational_operator::equal);
  CHECK(get<data>(p1->rhs) == data{true});
}

TEST(serialization) {
  expression ex0, ex1;
  std::vector<char> buf;
  CHECK_EQUAL(detail::serialize(buf, expr0, expr1), caf::none);
  CHECK_EQUAL(detail::legacy_deserialize(buf, ex0, ex1), true);
  auto d = caf::get_if<disjunction>(&ex1);
  REQUIRE(d);
  REQUIRE(!d->empty());
  auto n = caf::get_if<negation>(&d->at(0));
  REQUIRE(n);
  auto c = caf::get_if<conjunction>(&n->expr());
  REQUIRE(c);
  REQUIRE_EQUAL(c->size(), 2u);
  auto p = caf::get_if<predicate>(&c->at(1));
  REQUIRE(p);
  CHECK_EQUAL(p->op, relational_operator::equal);
}

TEST(predicate expansion) {
  auto expr = to<expression>("10.0.0.0/8");
  auto normalized
    = to<expression>(":subnet == 10.0.0.0/8 || :addr in 10.0.0.0/8");
  REQUIRE(expr);
  REQUIRE(normalized);
  CHECK_EQUAL(normalize(*expr), *normalized);
}

TEST(avoid overzealous predicate expansion) {
  auto expr = to<expression>(":subnet == 10.0.0.0/8");
  auto normalized = to<expression>(":subnet == 10.0.0.0/8");
  REQUIRE(expr);
  REQUIRE(normalized);
  CHECK_EQUAL(normalize(*expr), *normalized);
}

TEST(normalization) {
  MESSAGE("extractor on LHS");
  auto expr = to<expression>("\"foo\" in bar");
  auto normalized = to<expression>("bar ni \"foo\"");
  REQUIRE(expr);
  REQUIRE(normalized);
  CHECK_EQUAL(normalize(*expr), *normalized);
  // The normalizer must not touch predicates with two extractors, regardless
  // of whether that's actually a valid construct.
  expr = to<expression>(":foo == :bar");
  REQUIRE(expr);
  CHECK_EQUAL(normalize(*expr), *expr);
  MESSAGE("pushing down negations to predicate level");
  expr = to<expression>("! (x > 42 && x < 84)");
  normalized = to<expression>("x <= 42 || x >= 84");
  REQUIRE(expr);
  REQUIRE(normalized);
  CHECK_EQUAL(normalize(*expr), *normalized);
  MESSAGE("removal of negations");
  expr = to<expression>("! x < 42");
  normalized = to<expression>("x >= 42");
  REQUIRE(expr);
  REQUIRE(normalized);
  CHECK_EQUAL(normalize(*expr), *normalized);
  expr = to<expression>("x == 42");
  REQUIRE(expr);
  *expr = negation{expression{negation{std::move(*expr)}}};
  normalized = to<expression>("x == 42");
  REQUIRE(normalized);
  CHECK_EQUAL(normalize(*expr), *normalized);
  *expr = negation{std::move(*expr)};
  normalized = to<expression>("x != 42");
  REQUIRE(expr);
  REQUIRE(normalized);
  CHECK_EQUAL(normalize(*expr), *normalized);
  expr = to<expression>("! (x > -1 && x < +1)");
  normalized = to<expression>("x <= -1 || x >= +1");
  REQUIRE(expr);
  REQUIRE(normalized);
  CHECK_EQUAL(normalize(*expr), *normalized);
  MESSAGE("deduplication");
  expr = to<expression>("x == 42 || x == 42");
  normalized = to<expression>("x == 42");
  REQUIRE(expr);
  REQUIRE(normalized);
  CHECK_EQUAL(normalize(*expr), *normalized);
  expr = to<expression>("x == 42 || 42 == x");
  normalized = to<expression>("x == 42");
  REQUIRE(expr);
  REQUIRE(normalized);
  CHECK_EQUAL(normalize(*expr), *normalized);
  MESSAGE("performing all normalizations in one shot");
  expr = to<expression>("a > 42 && 42 < a && "
                        "! (\"foo\" in bar || ! x == 1337)");
  normalized = to<expression>("a > 42 && bar !ni \"foo\" && x == 1337");
  REQUIRE(expr);
  REQUIRE(normalized);
  CHECK_EQUAL(normalize(*expr), *normalized);
}

TEST(extractors) {
  auto port = type{"port", count_type{}};
  auto subport = type{"subport", port};
  auto s = record_type{
    {"real", real_type{}}, {"bool", bool_type{}}, {"host", address_type{}},
    {"port", port},        {"subport", subport},
  };
  auto r = type{flatten(record_type{{"orig", s}, {"resp", s}})};
  auto sn = unbox(to<subnet>("192.168.0.0/24"));
  {
    auto pred0 = predicate{data_extractor{type{address_type{}}, 2},
                           relational_operator::in, data{sn}};
    auto pred1 = predicate{data_extractor{type{address_type{}}, 7},
                           relational_operator::in, data{sn}};
    auto normalized = disjunction{pred0, pred1};
    MESSAGE("type extractor - distribution");
    auto expr = unbox(to<expression>(":addr in 192.168.0.0/24"));
    auto resolved = caf::visit(type_resolver(r), expr);
    CHECK_EQUAL(resolved, normalized);
    MESSAGE("extractor - distribution");
    expr = unbox(to<expression>("host in 192.168.0.0/24"));
    resolved = unbox(caf::visit(type_resolver(r), expr));
    CHECK_EQUAL(resolved, normalized);
  }
  {
    auto pred0 = predicate{data_extractor{type{address_type{}}, 2},
                           relational_operator::not_in, data{sn}};
    auto pred1 = predicate{data_extractor{type{address_type{}}, 7},
                           relational_operator::not_in, data{sn}};
    auto normalized = conjunction{pred0, pred1};
    MESSAGE("type extractor - distribution with negation");
    auto expr = unbox(to<expression>(":addr !in 192.168.0.0/24"));
    auto resolved = caf::visit(type_resolver(r), expr);
    CHECK_EQUAL(resolved, normalized);
    MESSAGE("extractor - distribution with negation");
    expr = unbox(to<expression>("host !in 192.168.0.0/24"));
    resolved = unbox(caf::visit(type_resolver(r), expr));
    CHECK_EQUAL(resolved, normalized);
  }
  {
    auto pred0 = predicate{data_extractor{port, 3}, relational_operator::equal,
                           data{80u}};
    auto pred1 = predicate{data_extractor{subport, 4},
                           relational_operator::equal, data{80u}};
    auto pred2 = predicate{data_extractor{port, 8}, relational_operator::equal,
                           data{80u}};
    auto pred3 = predicate{data_extractor{subport, 9},
                           relational_operator::equal, data{80u}};
    auto normalized = disjunction{pred0, pred1, pred2, pred3};
    MESSAGE("type extractor - used defined types");
    auto expr = unbox(to<expression>(":port == 80"));
    auto resolved = caf::visit(type_resolver(r), expr);
    CHECK_EQUAL(resolved, normalized);
    expr = unbox(to<expression>(":count == 80"));
    resolved = caf::visit(type_resolver(r), expr);
    CHECK_EQUAL(resolved, normalized);
  }
}

TEST(validation - meta extractor) {
  MESSAGE("#type");
  // The "type" attribute extractor requires a string operand.
  auto expr = to<expression>("#type == \"foo\"");
  REQUIRE(expr);
  CHECK(caf::visit(validator{}, *expr));
  expr = to<expression>("#type == 42");
  REQUIRE(expr);
  CHECK(!caf::visit(validator{}, *expr));
  expr = to<expression>("#type == zeek.conn");
  REQUIRE(expr);
  CHECK(!caf::visit(validator{}, *expr));
  MESSAGE("#field");
  expr = to<expression>("#field == \"id.orig_h\"");
  REQUIRE(expr);
  CHECK(caf::visit(validator{}, *expr));
  expr = to<expression>("#field ~ \"orig\"");
  REQUIRE(expr);
  CHECK(!caf::visit(validator{}, *expr));
  expr = to<expression>("#field == /orig/");
  REQUIRE(expr);
  CHECK(!caf::visit(validator{}, *expr));
  expr = to<expression>("#field ni \"orig\"");
  REQUIRE(expr);
  CHECK(!caf::visit(validator{}, *expr));
  expr = to<expression>("\"orig\" in #field");
  REQUIRE(expr);
  CHECK(!caf::visit(validator{}, *expr));
}

TEST(validation - type extractor) {
  auto expr = to<expression>(":bool == T");
  REQUIRE(expr);
  CHECK(caf::visit(validator{}, *expr));
  expr = to<expression>(":addr in 10.0.0.0/8");
  REQUIRE(expr);
  CHECK(caf::visit(validator{}, *expr));
  expr = to<expression>(":bool > -42");
  REQUIRE(expr);
  CHECK(!caf::visit(validator{}, *expr));
  expr = to<expression>(":timestamp < now");
  REQUIRE(expr);
  CHECK(caf::visit(validator{}, *expr));
  expr = to<expression>(":timestamp < 2017-06-16");
  REQUIRE(expr);
  CHECK(caf::visit(validator{}, *expr));
}

TEST(matcher) {
  auto match = [](const std::string& str, auto&& t) {
    auto expr = to<expression>(str);
    REQUIRE(expr);
    auto resolved = caf::visit(type_resolver(type{t}), *expr);
    REQUIRE(resolved);
    return caf::visit(matcher{type{t}}, *resolved);
  };
  auto r = type{record_type{
    {"x", real_type{}},
    {"y", bool_type{}},
    {"z", address_type{}},
  }};
  CHECK(match(":count == 42 || :real < 4.2", r));
  CHECK(match(":bool == T && :real < 4.2", r));
  MESSAGE("extractors");
  CHECK(match("x < 4.2 || (y == T && z in 10.0.0.0/8)", r));
  CHECK(match("x < 4.2 && (y == F || :bool == F)", r));
  CHECK(!match("x < 4.2 && a == T", r));
  MESSAGE("meta extractors");
  CHECK(!match("#type == \"foo\"", r));
  r = type{"foo", r};
  CHECK(match("#type == \"foo\"", r));
  CHECK(match("#type != \"bar\"", r));
}

TEST(labeler) {
  auto str
    = "(x == 5 && :bool == T) || (foo ~ /foo/ && !(x == 5 || #type ~ /bar/))"s;
  auto expr = to_expr(str);
  // Create a visitor that records all offsets in order.
  detail::stable_map<expression, offset> offset_map;
  auto visitor = labeler{[&](const auto& x, const offset& o) {
    offset_map.emplace(x, o);
  }};
  caf::visit(visitor, expr);
  decltype(offset_map) expected_offset_map{
    {to_expr(str), {0}},
    {to_expr("x == 5 && :bool == T"), {0, 0}},
    {to_expr("x == 5"), {0, 0, 0}},
    {to_expr(":bool == T"), {0, 0, 1}},
    {to_expr("foo ~ /foo/ && !(x == 5 || #type ~ /bar/)"), {0, 1}},
    {to_expr("foo ~ /foo/"), {0, 1, 0}},
    {to_expr("!(x == 5 || #type ~ /bar/)"), {0, 1, 1}},
    {to_expr("x == 5 || #type ~ /bar/"), {0, 1, 1, 0}},
    {to_expr("x == 5"), {0, 1, 1, 0, 0}},
    {to_expr("#type ~ /bar/"), {0, 1, 1, 0, 1}},
  };
  CHECK_EQUAL(offset_map, expected_offset_map);
}

TEST(at) {
  auto str
    = "(x == 5 && :bool == T) || (foo ~ /foo/ && !(x == 5 || #type ~ /bar/))"s;
  auto expr = to_expr(str);
  CHECK_EQUAL(at(expr, {}), nullptr);  // invalid offset
  CHECK_EQUAL(at(expr, {0}), &expr);   // root node
  CHECK_EQUAL(at(expr, {1}), nullptr); // invalid root offset
  CHECK_EQUAL(*at(expr, {0, 0}), to_expr("x == 5 && :bool == T"));
  CHECK_EQUAL(*at(expr, {0, 1, 0}), to_expr("foo ~ /foo/"));
  CHECK_EQUAL(*at(expr, {0, 1, 1, 0, 1}), to_expr("#type ~ /bar/"));
  CHECK_EQUAL(at(expr, {0, 1, 1, 0, 1, 0}), nullptr); // offset too long
}

TEST(resolve) {
  using result_type = std::vector<std::pair<offset, predicate>>;
  auto resolve_pred = [](auto&& x, offset o, type t) -> result_type {
    result_type result;
    auto pred = to<predicate>(x);
    auto resolved = type_resolver{t}(unbox(pred));
    for (auto& pred : caf::visit(predicatizer{}, *resolved))
      result.emplace_back(o, std::move(pred));
    return result;
  };
  auto expr = to_expr("(x == 5 && y == T) || (x == 5 && y == F)"); // tautology
  auto t = type{
    "foo",
    record_type{
      {"x", count_type{}},
      {"y", bool_type{}},
    },
  };
  auto xs = resolve(expr, t);
  decltype(xs) expected;
  auto concat = [](auto&& xs, auto&& ys) {
    auto begin = std::make_move_iterator(ys.begin());
    auto end = std::make_move_iterator(ys.end());
    xs.insert(xs.end(), begin, end);
  };
  // TODO: How should we handle duplicates? Weed them out? --MV
  concat(expected, resolve_pred("x == 5", {0, 0, 0}, t));
  concat(expected, resolve_pred("y == T", {0, 0, 1}, t));
  concat(expected, resolve_pred("x == 5", {0, 1, 0}, t));
  concat(expected, resolve_pred("y == F", {0, 1, 1}, t));
  CHECK_EQUAL(xs, expected);
}

TEST(parse print roundtrip) {
  MESSAGE("simple roundtrip");
  {
    auto str
      = "((x == 5 && :bool == T) || (foo ~ /foo/ && ! (x == 5 || #type ~ /bar/)))"s;
    auto expr = to_expr(str);
    CHECK_EQUAL(str, to_string(expr));
  }
}

FIXTURE_SCOPE_END()

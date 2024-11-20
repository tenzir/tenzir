//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/expression.hpp"

#include "tenzir/concept/parseable/tenzir/expression.hpp"
#include "tenzir/concept/parseable/tenzir/schema.hpp"
#include "tenzir/concept/parseable/tenzir/subnet.hpp"
#include "tenzir/concept/parseable/tenzir/time.hpp"
#include "tenzir/concept/parseable/to.hpp"
#include "tenzir/concept/printable/tenzir/expression.hpp"
#include "tenzir/concept/printable/to_string.hpp"
#include "tenzir/data.hpp"
#include "tenzir/detail/legacy_deserialize.hpp"
#include "tenzir/detail/serialize.hpp"
#include "tenzir/detail/stable_map.hpp"
#include "tenzir/expression_visitors.hpp"
#include "tenzir/module.hpp"
#include "tenzir/test/test.hpp"

#include <caf/test/dsl.hpp>

#include <string>

using caf::get;
using caf::get_if;
using caf::holds_alternative;

using namespace std::string_literals;
using namespace tenzir;

namespace {

template <class T>
expression to_expr(T&& x) {
  return unbox(to<expression>(std::forward<T>(x)));
}

struct fixture {
  fixture() {
    // expr0 := !(x.y.z <= 42 && #schema == "foo")
    auto p0 = predicate{field_extractor{"x.y.z"},
                        relational_operator::less_equal, data{int64_t{42}}};
    auto p1 = predicate{meta_extractor{meta_extractor::schema},
                        relational_operator::equal, data{"foo"}};
    auto conj = conjunction{p0, p1};
    expr0 = negation{conj};
    // expr0 || :double > 4.2
    auto p2 = predicate{type_extractor{type{double_type{}}},
                        relational_operator::greater_equal, data{4.2}};
    expr1 = disjunction{expr0, p2};
  }

  expression expr0;
  expression expr1;
};

} // namespace

FIXTURE_SCOPE(expr_tests, fixture)

TEST(construction) {
  auto n = try_as<negation>(&expr0);
  REQUIRE(n);
  auto c = try_as<conjunction>(&n->expr());
  REQUIRE(c);
  REQUIRE(c->size() == 2);
  auto p0 = try_as<predicate>(&c->at(0));
  REQUIRE(p0);
  CHECK_EQUAL(get<field_extractor>(p0->lhs).field, "x.y.z");
  CHECK_EQUAL(p0->op, relational_operator::less_equal);
  CHECK_EQUAL(get<data>(p0->rhs), int64_t{42});
  auto p1 = try_as<predicate>(&c->at(1));
  REQUIRE(p1);
  CHECK_EQUAL(get<meta_extractor>(p1->lhs).kind, meta_extractor::schema);
  CHECK_EQUAL(p1->op, relational_operator::equal);
  CHECK(get<data>(p1->rhs) == data{"foo"});
}

TEST(serialization) {
  expression ex0, ex1;
  caf::byte_buffer buf;
  CHECK(detail::serialize(buf, expr0, expr1));
  CHECK_EQUAL(detail::legacy_deserialize(buf, ex0, ex1), true);
  auto d = try_as<disjunction>(&ex1);
  REQUIRE(d);
  REQUIRE(!d->empty());
  auto n = try_as<negation>(&d->at(0));
  REQUIRE(n);
  auto c = try_as<conjunction>(&n->expr());
  REQUIRE(c);
  REQUIRE_EQUAL(c->size(), 2u);
  auto p = try_as<predicate>(&c->at(1));
  REQUIRE(p);
  CHECK_EQUAL(p->op, relational_operator::equal);
}

TEST(predicate expansion) {
  auto expr = to<expression>("10.0.0.0/8");
  auto normalized
    = to<expression>(":subnet == 10.0.0.0/8 || :ip in 10.0.0.0/8");
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
  auto port = type{"port", uint64_type{}};
  auto subport = type{"subport", port};
  auto s = record_type{
    {"real", double_type{}}, {"bool", bool_type{}}, {"host", ip_type{}},
    {"port", port},          {"subport", subport},
  };
  auto r = type{flatten(record_type{{"orig", s}, {"resp", s}})};
  auto sn = unbox(to<subnet>("192.168.0.0/24"));
  {
    auto pred0 = predicate{data_extractor{type{ip_type{}}, 2},
                           relational_operator::in, data{sn}};
    auto pred1 = predicate{data_extractor{type{ip_type{}}, 7},
                           relational_operator::in, data{sn}};
    auto normalized = disjunction{pred0, pred1};
    MESSAGE("type extractor - distribution");
    auto expr = unbox(to<expression>(":ip in 192.168.0.0/24"));
    auto resolved = tenzir::match(expr, type_resolver(r));
    CHECK_EQUAL(resolved, normalized);
    MESSAGE("field extractor - distribution");
    expr = unbox(to<expression>("host in 192.168.0.0/24"));
    resolved = unbox(tenzir::match(expr, type_resolver(r)));
    CHECK_EQUAL(resolved, normalized);
  }
  {
    auto pred0 = predicate{data_extractor{type{ip_type{}}, 2},
                           relational_operator::not_in, data{sn}};
    auto pred1 = predicate{data_extractor{type{ip_type{}}, 7},
                           relational_operator::not_in, data{sn}};
    auto normalized = conjunction{pred0, pred1};
    MESSAGE("type extractor - distribution with negation");
    auto expr = unbox(to<expression>(":ip !in 192.168.0.0/24"));
    auto resolved = tenzir::match(expr, type_resolver(r));
    CHECK_EQUAL(resolved, normalized);
    MESSAGE("field extractor - distribution with negation");
    expr = unbox(to<expression>("host !in 192.168.0.0/24"));
    resolved = unbox(tenzir::match(expr, type_resolver(r)));
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
    auto resolved = tenzir::match(expr, type_resolver(r));
    CHECK_EQUAL(resolved, normalized);
    expr = unbox(to<expression>(":uint64 == 80"));
    resolved = tenzir::match(expr, type_resolver(r));
    CHECK_EQUAL(resolved, normalized);
  }
}

TEST(validation - meta extractor) {
  MESSAGE("#schema");
  // The "type" attribute extractor requires a string operand.
  auto expr = to<expression>("#schema == \"foo\"");
  REQUIRE(expr);
  CHECK(tenzir::match(*expr, validator{}));
  expr = to<expression>("#schema == 42");
  REQUIRE(expr);
  CHECK(!tenzir::match(*expr, validator{}));
  expr = to<expression>("#schema == zeek.conn");
  REQUIRE(expr);
  CHECK(!tenzir::match(*expr, validator{}));
}

TEST(validation - type extractor) {
  auto expr = to<expression>(":bool == true");
  REQUIRE(expr);
  CHECK(tenzir::match(*expr, validator{}));
  expr = to<expression>(":ip in 10.0.0.0/8");
  REQUIRE(expr);
  CHECK(tenzir::match(*expr, validator{}));
  expr = to<expression>(":bool > -42");
  REQUIRE(expr);
  CHECK(!tenzir::match(*expr, validator{}));
  expr = to<expression>(":timestamp < now");
  REQUIRE(expr);
  CHECK(tenzir::match(*expr, validator{}));
  expr = to<expression>(":timestamp < 2017-06-16");
  REQUIRE(expr);
  CHECK(tenzir::match(*expr, validator{}));
}

TEST(matcher) {
  auto match = [](const std::string& str, auto&& t) {
    auto expr = to<expression>(str);
    REQUIRE(expr);
    auto resolved = tenzir::match(*expr, type_resolver(type{t}));
    REQUIRE(resolved);
    return tenzir::match(*resolved, matcher{type{t}});
  };
  auto r = type{record_type{
    {"x", double_type{}},
    {"y", bool_type{}},
    {"z", ip_type{}},
  }};
  CHECK(match(":uint64 == 42 || :double < 4.2", r));
  CHECK(match(":bool == true && :double < 4.2", r));
  MESSAGE("field extractors");
  CHECK(match("x < 4.2 || (y == true && z in 10.0.0.0/8)", r));
  CHECK(match("x < 4.2 && (y == false || :bool == false)", r));
  CHECK(!match("x < 4.2 && a == true", r));
  MESSAGE("attribute extractors");
  CHECK(!match("#schema == \"foo\"", r));
  r = type{"foo", r};
  CHECK(match("#schema == \"foo\"", r));
  CHECK(match("#schema != \"bar\"", r));
}

TEST(labeler) {
  auto str
    = "(x == 5 && :bool == true) || (foo == /foo/ && !(x == 5 || #schema == /bar/))"s;
  auto expr = to_expr(str);
  // Create a visitor that records all offsets in order.
  detail::stable_map<expression, offset> offset_map;
  auto visitor = labeler{[&](const auto& x, const offset& o) {
    offset_map.emplace(x, o);
  }};
  tenzir::match(expr, visitor);
  decltype(offset_map) expected_offset_map{
    {to_expr(str), {0}},
    {to_expr("x == 5 && :bool == true"), {0, 0}},
    {to_expr("x == 5"), {0, 0, 0}},
    {to_expr(":bool == true"), {0, 0, 1}},
    {to_expr("foo == /foo/ && !(x == 5 || #schema == /bar/)"), {0, 1}},
    {to_expr("foo == /foo/"), {0, 1, 0}},
    {to_expr("!(x == 5 || #schema == /bar/)"), {0, 1, 1}},
    {to_expr("x == 5 || #schema == /bar/"), {0, 1, 1, 0}},
    {to_expr("x == 5"), {0, 1, 1, 0, 0}},
    {to_expr("#schema == /bar/"), {0, 1, 1, 0, 1}},
  };
  CHECK_EQUAL(offset_map, expected_offset_map);
}

TEST(at) {
  auto str
    = "(x == 5 && :bool == true) || (foo == /foo/ && !(x == 5 || #schema == /bar/))"s;
  auto expr = to_expr(str);
  CHECK_EQUAL(at(expr, {}), nullptr);  // invalid offset
  CHECK_EQUAL(at(expr, {0}), &expr);   // root node
  CHECK_EQUAL(at(expr, {1}), nullptr); // invalid root offset
  CHECK_EQUAL(*at(expr, {0, 0}), to_expr("x == 5 && :bool == true"));
  CHECK_EQUAL(*at(expr, {0, 1, 0}), to_expr("foo == /foo/"));
  CHECK_EQUAL(*at(expr, {0, 1, 1, 0, 1}), to_expr("#schema == /bar/"));
  CHECK_EQUAL(at(expr, {0, 1, 1, 0, 1, 0}), nullptr); // offset too long
}

TEST(resolve) {
  using result_type = std::vector<std::pair<offset, predicate>>;
  auto resolve_pred = [](auto&& x, offset o, type t) -> result_type {
    result_type result;
    auto pred = to<predicate>(x);
    auto resolved = type_resolver{t}(unbox(pred));
    for (auto& pred : tenzir::match(*resolved, predicatizer{})) {
      result.emplace_back(o, std::move(pred));
    }
    return result;
  };
  auto expr
    = to_expr("(x == 5 && y == true) || (x == 5 && y == false)"); // tautology
  auto t = type{
    "foo",
    record_type{
      {"x", uint64_type{}},
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
  concat(expected, resolve_pred("y == true", {0, 0, 1}, t));
  concat(expected, resolve_pred("x == 5", {0, 1, 0}, t));
  concat(expected, resolve_pred("y == false", {0, 1, 1}, t));
  CHECK_EQUAL(xs, expected);
}

TEST(parse print roundtrip) {
  MESSAGE("simple roundtrip");
  {
    auto str
      = "((x == 5 and :bool == true) or (foo == /foo/ and not (x == 5 or #schema == /bar/)))"s;
    auto expr = to_expr(str);
    CHECK_EQUAL(str, to_string(expr));
  }
}

TEST(expression parser composability) {
  auto str = "x == 5 | :bool == true || #schema == /bar/ | +3"s;
  std::vector<expression> result;
  auto p = (parsers::expr % (*parsers::space >> '|' >> *parsers::space))
           >> parsers::eoi;
  REQUIRE(p(str, result));
  REQUIRE_EQUAL(result.size(), 3u);
  CHECK_EQUAL(result[0], to_expr("x == 5"));
  CHECK_EQUAL(result[1], to_expr(":bool == true || #schema == /bar/"));
  CHECK_EQUAL(result[2], to_expr("+3"));
}

FIXTURE_SCOPE_END()

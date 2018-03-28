/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include "vast/event.hpp"
#include "vast/expression.hpp"
#include "vast/expression_visitors.hpp"
#include "vast/load.hpp"
#include "vast/save.hpp"
#include "vast/schema.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/parseable/vast/schema.hpp"
#include "vast/concept/parseable/vast/time.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/expression.hpp"

#define SUITE expression
#include "test.hpp"

using namespace vast;

struct fixture {
  fixture() {
    // expr0 := !(x.y.z <= 42 && &foo == T)
    auto p0 = predicate{key_extractor{{"x", "y", "z"}}, less_equal, data{42}};
    auto p1 = predicate{attribute_extractor{{"foo"}}, equal, data{true}};
    auto conj = conjunction{p0, p1};
    expr0 = negation{conj};
    // expr0 || :real > 4.2
    auto p2 = predicate{type_extractor{real_type{}}, greater_equal, data{4.2}};
    expr1 = disjunction{expr0, p2};
  }

  expression expr0;
  expression expr1;
};

FIXTURE_SCOPE(expr_tests, fixture)

TEST(construction) {
  auto n = get_if<negation>(expr0);
  REQUIRE(n);
  auto c = get_if<conjunction>(n->expr());
  REQUIRE(c);
  REQUIRE(c->size() == 2);
  auto p0 = get_if<predicate>(c->at(0));
  REQUIRE(p0);
  CHECK(get<key_extractor>(p0->lhs).key == key{"x", "y", "z"});
  CHECK_EQUAL(p0->op, less_equal);
  CHECK(get<data>(p0->rhs) == data{42});
  auto p1 = get_if<predicate>(c->at(1));
  REQUIRE(p1);
  CHECK(get<attribute_extractor>(p1->lhs).attr == attribute{"foo"});
  CHECK_EQUAL(p1->op, equal);
  CHECK(get<data>(p1->rhs) == data{true});
}

TEST(serialization) {
  expression ex0, ex1;
  std::vector<char> buf;
  save(buf, expr0, expr1);
  load(buf, ex0, ex1);
  auto d = get_if<disjunction>(ex1);
  REQUIRE(d);
  REQUIRE(!d->empty());
  auto n = get_if<negation>(d->at(0));
  REQUIRE(n);
  auto c = get_if<conjunction>(n->expr());
  REQUIRE(c);
  REQUIRE_EQUAL(c->size(), 2u);
  auto p = get_if<predicate>(c->at(1));
  REQUIRE(p);
  CHECK_EQUAL(p->op, equal);
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
  expr = to<expression>("&foo == &bar");
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
  expr = to<expression>("a > 42 && 42 < a && ! (\"foo\" in bar || ! x == 1337)");
  normalized = to<expression>("a > 42 && bar !ni \"foo\" && x == 1337");
  REQUIRE(expr);
  REQUIRE(normalized);
  CHECK_EQUAL(normalize(*expr), *normalized);
}

TEST(validation - attribute extractor) {
  // The "type" attribute extractor requires a string operand.
  auto expr = to<expression>("&type == \"foo\"");
  REQUIRE(expr);
  CHECK(visit(validator{}, *expr));
  expr = to<expression>("&type == 42");
  REQUIRE(expr);
  CHECK(!visit(validator{}, *expr));
  expr = to<expression>("&type == bro::conn");
  REQUIRE(expr);
  CHECK(!visit(validator{}, *expr));
  // The "time" attribute extractor requires a timestamp operand.
  expr = to<expression>("&time < now");
  REQUIRE(expr);
  CHECK(visit(validator{}, *expr));
  expr = to<expression>("&time < 2017-06-16");
  REQUIRE(expr);
  CHECK(visit(validator{}, *expr));
  expr = to<expression>("&time > -42");
  REQUIRE(expr);
  CHECK(!visit(validator{}, *expr));
  expr = to<expression>("&time > -42 secs");
  REQUIRE(expr);
  CHECK(!visit(validator{}, *expr));
}

TEST(validation - type extractor) {
  auto expr = to<expression>(":port == 443/tcp");
  REQUIRE(expr);
  CHECK(visit(validator{}, *expr));
  expr = to<expression>(":addr in 10.0.0.0/8");
  REQUIRE(expr);
  CHECK(visit(validator{}, *expr));
  expr = to<expression>(":port > -42");
  REQUIRE(expr);
  CHECK(!visit(validator{}, *expr));
}

TEST(matcher) {
  auto match = [](const std::string& str, auto&& t) {
    auto expr = to<expression>(str);
    REQUIRE(expr);
    auto resolved = visit(type_resolver(t), *expr);
    REQUIRE(resolved);
    return visit(matcher{t}, *resolved);
  };
  MESSAGE("type extractors");
  CHECK(match(":real < 4.2", real_type{}));
  CHECK(!match(":int == -42", real_type{}));
  CHECK(!match(":count == 42 && :real < 4.2", real_type{}));
  CHECK(match(":count == 42 || :real < 4.2", real_type{}));
  auto r = record_type{
    {"x", real_type{}},
    {"y", port_type{}},
    {"z", address_type{}}
  };
  CHECK(match(":count == 42 || :real < 4.2", r));
  CHECK(match(":port == 80/tcp && :real < 4.2", r));
  MESSAGE("key extractors");
  CHECK(match("x < 4.2 || (y == 80/tcp && z in 10.0.0.0/8)", r));
  CHECK(match("x < 4.2 && (y == 80/tcp || :bool == F)", r));
  CHECK(!match("x < 4.2 && a == T", r));
  MESSAGE("attribute extractors");
  CHECK(!match("&type == \"foo\"", r));
  r.name("foo");
  CHECK(match("&type == \"foo\"", r));
  CHECK(match("&type != \"bar\"", r));
}

FIXTURE_SCOPE_END()

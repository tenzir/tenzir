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
#include "vast/schema.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"

#define SUITE expression
#include "vast/test/test.hpp"

using namespace vast;

namespace {

struct fixture {
  fixture() {
    foo = record_type{
      {"s1", string_type{}},
      {"d1", real_type{}},
      {"c", count_type{}},
      {"i", integer_type{}},
      {"s2", string_type{}},
      {"d2", real_type{}}
    }.name("foo");
    bar = record_type{
      {"s1", string_type{}},
      {"r", record_type{
      {"b", boolean_type{}},
      {"s", string_type{}}
      }},
    }.name("bar");
    sch.add(foo);
    sch.add(bar);
    e0 = event::make(vector{"babba", 1.337, 42u, 100, "bar", -4.8}, foo);
    e1 = event::make(vector{"yadda", vector{false, "baz"}}, bar);
    MESSAGE("event meta data queries");
    auto tp = to<timestamp>("2014-01-16+05:30:12");
    REQUIRE(tp);
    e.timestamp(*tp);
    auto t = alias_type{}.name("foo"); // nil type, for meta data only
    CHECK(e.type(t));
  }

  schema sch;
  type foo;
  type bar;
  event e;
  event e0;
  event e1;
};

} // namespace <anonymous>

FIXTURE_SCOPE(evaluation_tests, fixture)

TEST(evaluation - attributes) {
  auto ast = to<expression>("#time == 2014-01-16+05:30:12");
  REQUIRE(ast);
  CHECK(caf::visit(event_evaluator{e}, *ast));
  ast = to<expression>("#time == 2015-01-16+05:30:12"); // slight data change
  REQUIRE(ast);
  CHECK(!caf::visit(event_evaluator{e}, *ast));
  ast = to<expression>("#type == \"foo\"");
  REQUIRE(ast);
  CHECK(caf::visit(event_evaluator{e}, *ast));
  ast = to<expression>("! #type == \"bar\"");
  REQUIRE(ast);
  CHECK(caf::visit(event_evaluator{e}, *ast));
  ast = to<expression>("#type != \"foo\"");
  REQUIRE(ast);
  CHECK(!caf::visit(event_evaluator{e}, *ast));
}

TEST(evaluation - types) {
  auto ast = to<expression>(":count == 42");
  REQUIRE(ast);
  CHECK(caf::visit(event_evaluator{e0}, caf::visit(type_pruner{foo}, *ast)));
  CHECK(!caf::visit(event_evaluator{e1}, caf::visit(type_pruner{bar}, *ast)));
  ast = to<expression>(":int != +101");
  REQUIRE(ast);
  CHECK(caf::visit(event_evaluator{e0}, caf::visit(type_pruner{foo}, *ast)));
  CHECK(!caf::visit(event_evaluator{e1}, caf::visit(type_pruner{bar}, *ast)));
  ast = to<expression>(":string ~ /bar/ && :int == +100");
  REQUIRE(ast);
  CHECK(caf::visit(event_evaluator{e0}, caf::visit(type_pruner{foo}, *ast)));
  CHECK(!caf::visit(event_evaluator{e1}, caf::visit(type_pruner{bar}, *ast)));
  ast = to<expression>(":real >= -4.8");
  REQUIRE(ast);
  CHECK(caf::visit(event_evaluator{e0}, caf::visit(type_pruner{foo}, *ast)));
  CHECK(!caf::visit(event_evaluator{e1}, caf::visit(type_pruner{bar}, *ast)));
  ast = to<expression>(
    ":int <= -3 || :int >= +100 && :string !~ /bar/ || :real > 1.0");
  REQUIRE(ast);
  CHECK(caf::visit(event_evaluator{e0}, caf::visit(type_pruner{foo}, *ast)));
  // For the event of type "bar", this expression degenerates to
  // <nil> because it has no numeric types and the first predicate of the
  // conjunction in the middle renders the entire conjunction not viable.
  CHECK(!caf::visit(event_evaluator{e1}, caf::visit(type_pruner{bar}, *ast)));
}

TEST(evaluation - schema) {
  auto ast = to<expression>("s1 == \"babba\" && d1 <= 1337.0");
  REQUIRE(ast);
  auto ast_resolved = caf::visit(type_resolver{foo}, *ast);
  REQUIRE(ast_resolved);
  CHECK(!caf::holds_alternative<caf::none_t>(*ast_resolved));
  CHECK(caf::visit(event_evaluator{e0}, *ast_resolved));
  CHECK(!caf::visit(event_evaluator{e1}, *ast_resolved));
  ast = to<expression>("s1 != \"cheetah\"");
  REQUIRE(ast);
  ast_resolved = caf::visit(type_resolver{foo}, *ast);
  REQUIRE(ast_resolved);
  CHECK(caf::visit(event_evaluator{e0}, *ast_resolved));
  ast_resolved = caf::visit(type_resolver{bar}, *ast);
  REQUIRE(ast_resolved);
  CHECK(caf::visit(event_evaluator{e1}, *ast_resolved));
  ast = to<expression>("d1 > 0.5");
  REQUIRE(ast);
  ast_resolved = caf::visit(type_resolver{foo}, *ast);
  REQUIRE(ast_resolved);
  CHECK(caf::visit(event_evaluator{e0}, *ast_resolved));
  CHECK(!caf::visit(event_evaluator{e1}, *ast_resolved));
  ast = to<expression>("r.b == F");
  REQUIRE(ast);
  ast_resolved = caf::visit(type_resolver{bar}, *ast);
  REQUIRE(ast_resolved);
  CHECK(caf::visit(event_evaluator{e1}, *ast_resolved));
  MESSAGE("error cases");
  // Invalid prefix.
  ast = to<expression>("not.there ~ /nil/");
  REQUIRE(ast);
  ast_resolved = caf::visit(type_resolver{foo}, *ast);
  REQUIRE(ast_resolved);
  CHECK(caf::holds_alternative<caf::none_t>(*ast_resolved));
  // 'q' doesn't exist in 'r'.
  ast = to<expression>("r.q == 80/tcp");
  REQUIRE(ast);
  ast_resolved = caf::visit(type_resolver{bar}, *ast);
  REQUIRE(ast_resolved);
  CHECK(caf::holds_alternative<caf::none_t>(*ast_resolved));
}

FIXTURE_SCOPE_END()

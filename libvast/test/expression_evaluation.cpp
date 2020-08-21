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

#define SUITE expression

#include "vast/test/fixtures/events.hpp"
#include "vast/test/test.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/parseable/vast/time.hpp"
#include "vast/expression.hpp"
#include "vast/expression_visitors.hpp"
#include "vast/ids.hpp"
#include "vast/schema.hpp"
#include "vast/table_slice.hpp"

#include <caf/test/dsl.hpp>

using namespace vast;

namespace {

struct fixture : fixtures::events {
  fixture() {
    // clang-format off
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
        {"b", bool_type{}},
        {"s", string_type{}}
      }},
    }.name("bar");
    // clang-format on
    sch.add(foo);
    sch.add(bar);
    e0 = {{"s1", "babba"}, {"d1", 1.337}, {"c1", 42u},
          {"i", 100},      {"s2", "bar"}, {"d2", -4.8}};
    e1 = {{"s1", "yadda"}, {"r", record{{"b", false}, {"s", "baz"}}}};
  }

  schema sch;
  type foo;
  type bar;
  record e;
  record e0;
  record e1;
};

} // namespace <anonymous>

FIXTURE_SCOPE(evaluation_tests, fixture)

// TODO: convert to table-slice-based evaluation
// TEST(evaluation - attributes) {
//  auto ast = to<expression>("#timestamp == 2014-01-16+05:30:12");
//  REQUIRE(ast);
//  CHECK(caf::visit(event_evaluator{e}, *ast));
//  // slight data change
//  ast = to<expression>("#timestamp == 2015-01-16+05:30:12");
//  REQUIRE(ast);
//  CHECK(!caf::visit(event_evaluator{e}, *ast));
//  ast = to<expression>("#type == \"foo\"");
//  REQUIRE(ast);
//  CHECK(caf::visit(event_evaluator{e}, *ast));
//  ast = to<expression>("! #type == \"bar\"");
//  REQUIRE(ast);
//  CHECK(caf::visit(event_evaluator{e}, *ast));
//  ast = to<expression>("#type != \"foo\"");
//  REQUIRE(ast);
//  CHECK(!caf::visit(event_evaluator{e}, *ast));
//}
//
// TEST(evaluation - types) {
//  auto ast = to<expression>(":count == 42");
//  REQUIRE(ast);
//  CHECK(caf::visit(event_evaluator{e0}, caf::visit(type_pruner{foo}, *ast)));
//  CHECK(!caf::visit(event_evaluator{e1}, caf::visit(type_pruner{bar}, *ast)));
//  ast = to<expression>(":int != +101");
//  REQUIRE(ast);
//  CHECK(caf::visit(event_evaluator{e0}, caf::visit(type_pruner{foo}, *ast)));
//  CHECK(!caf::visit(event_evaluator{e1}, caf::visit(type_pruner{bar}, *ast)));
//  ast = to<expression>(":string ~ /bar/ && :int == +100");
//  REQUIRE(ast);
//  CHECK(caf::visit(event_evaluator{e0}, caf::visit(type_pruner{foo}, *ast)));
//  CHECK(!caf::visit(event_evaluator{e1}, caf::visit(type_pruner{bar}, *ast)));
//  ast = to<expression>(":real >= -4.8");
//  REQUIRE(ast);
//  CHECK(caf::visit(event_evaluator{e0}, caf::visit(type_pruner{foo}, *ast)));
//  CHECK(!caf::visit(event_evaluator{e1}, caf::visit(type_pruner{bar}, *ast)));
//  ast = to<expression>(
//    ":int <= -3 || :int >= +100 && :string !~ /bar/ || :real > 1.0");
//  REQUIRE(ast);
//  CHECK(caf::visit(event_evaluator{e0}, caf::visit(type_pruner{foo}, *ast)));
//  // For the event of type "bar", this expression degenerates to
//  // <nil> because it has no numeric types and the first predicate of the
//  // conjunction in the middle renders the entire conjunction not viable.
//  CHECK(!caf::visit(event_evaluator{e1}, caf::visit(type_pruner{bar}, *ast)));
//}
//
// TEST(evaluation - schema) {
//  auto ast = to<expression>("s1 == \"babba\" && d1 <= 1337.0");
//  REQUIRE(ast);
//  auto ast_resolved = caf::visit(type_resolver{foo}, *ast);
//  REQUIRE(ast_resolved);
//  CHECK(!caf::holds_alternative<caf::none_t>(*ast_resolved));
//  CHECK(caf::visit(event_evaluator{e0}, *ast_resolved));
//  CHECK(!caf::visit(event_evaluator{e1}, *ast_resolved));
//  ast = to<expression>("s1 != \"cheetah\"");
//  REQUIRE(ast);
//  ast_resolved = caf::visit(type_resolver{foo}, *ast);
//  REQUIRE(ast_resolved);
//  CHECK(caf::visit(event_evaluator{e0}, *ast_resolved));
//  ast_resolved = caf::visit(type_resolver{bar}, *ast);
//  REQUIRE(ast_resolved);
//  CHECK(caf::visit(event_evaluator{e1}, *ast_resolved));
//  ast = to<expression>("d1 > 0.5");
//  REQUIRE(ast);
//  ast_resolved = caf::visit(type_resolver{foo}, *ast);
//  REQUIRE(ast_resolved);
//  CHECK(caf::visit(event_evaluator{e0}, *ast_resolved));
//  CHECK(!caf::visit(event_evaluator{e1}, *ast_resolved));
//  ast = to<expression>("r.b == F");
//  REQUIRE(ast);
//  ast_resolved = caf::visit(type_resolver{bar}, *ast);
//  REQUIRE(ast_resolved);
//  CHECK(caf::visit(event_evaluator{e1}, *ast_resolved));
//  MESSAGE("error cases");
//  // Invalid prefix.
//  ast = to<expression>("not.there ~ /nil/");
//  REQUIRE(ast);
//  ast_resolved = caf::visit(type_resolver{foo}, *ast);
//  REQUIRE(ast_resolved);
//  CHECK(caf::holds_alternative<caf::none_t>(*ast_resolved));
//  // 'q' doesn't exist in 'r'.
//  ast = to<expression>("r.q == 80/tcp");
//  REQUIRE(ast);
//  ast_resolved = caf::visit(type_resolver{bar}, *ast);
//  REQUIRE(ast_resolved);
//  CHECK(caf::holds_alternative<caf::none_t>(*ast_resolved));
//}

// TEST(evaluation - table slice rows) {
//  auto& slice = zeek_conn_log[0];
//  auto layout = slice->layout();
//  auto tailored = [&](std::string_view expr) {
//    auto ast = unbox(to<expression>(expr));
//    return unbox(caf::visit(type_resolver{layout}, ast));
//  };
//  // Run some checks on various rows.
//  CHECK(evaluate_at(*slice, 0, tailored("#timestamp < 2009-12-18+00:00:00")));
//  CHECK(evaluate_at(*slice, 0, tailored("orig_h == 192.168.1.102")));
//  CHECK(evaluate_at(*slice, 0, tailored(":addr == 192.168.1.102")));
//  CHECK(evaluate_at(*slice, 1, tailored("orig_h != 192.168.1.102")));
//  CHECK(evaluate_at(*slice, 1, tailored(":addr != 192.168.1.102")));
//  CHECK(evaluate_at(*slice, 1, tailored("orig_h in 192.168.1.0/24")));
//  CHECK(evaluate_at(*slice, 1, tailored("!(orig_h in 192.168.2.0/24)")));
//  CHECK(evaluate_at(*slice, 1,
//                    tailored("orig_h in [192.168.1.102, 192.168.1.103]")));
//}

TEST(evaluation - table slice) {
  auto slice = zeek_conn_log[0];
  slice.unshared().offset(0);
  REQUIRE_EQUAL(slice->rows(), 8u);
  auto layout = slice->layout();
  auto tailored = [&](std::string_view expr) {
    auto ast = unbox(to<expression>(expr));
    return unbox(caf::visit(type_resolver{layout}, ast));
  };
  // Run some checks on various rows.
  CHECK_EQUAL(evaluate(tailored("orig_h == 192.168.1.102"), *slice),
              make_ids({0, 2, 4}, 8));
}

FIXTURE_SCOPE_END()

#include "vast/event.h"
#include "vast/expression.h"
#include "vast/logger.h"
#include "vast/schema.h"
#include "vast/expr/evaluator.h"
#include "vast/expr/resolver.h"
#include "vast/expr/normalize.h"
#include "vast/concept/parseable/to.h"
#include "vast/concept/parseable/vast/expression.h"
#include "vast/concept/parseable/vast/schema.h"
#include "vast/concept/parseable/vast/time.h"
#include "vast/concept/printable/to_string.h"
#include "vast/concept/printable/vast/expression.h"
#include "vast/concept/serializable/vast/expression.h"
#include "vast/concept/serializable/io.h"

#define SUITE expression
#include "test.h"

using namespace vast;

TEST(construction) {
  predicate p0{time_extractor{}, less_equal,
               data{time::point::utc(1983, 8, 12)}};
  predicate p1{event_extractor{}, equal, data{"foo"}};
  conjunction conj{p0, p1};
  expression expr{conj};

  auto c = get<conjunction>(expr);
  REQUIRE(c);
  REQUIRE(c->size() == 2);
  CHECK(is<time_extractor>(get<predicate>(c->at(0))->lhs));
  CHECK(*get<data>(get<predicate>(c->at(1))->rhs) == "foo");
}

TEST(serialization) {
  predicate p0{event_extractor{}, in, data{"foo"}};
  predicate p1{type_extractor{}, equal, data{time::point::utc(1983, 8, 12)}};
  expression expr{disjunction{p0, p1}};

  auto str = to_string(expr);
  std::vector<uint8_t> buf;
  save(buf, expr);
  load(buf, expr);

  CHECK(to_string(expr), str);
}

TEST(event evaluation) {
  std::string str = R"__(
    type foo = record{
      s1: string,
      d1: real,
      c: count,
      i: int,
      s2: string,
      d2: real
    }
    type bar = record { s1: string, r : record { b: bool, s: string } }
  )__";
  auto sch = to<schema>(str);
  REQUIRE(sch);
  auto foo = sch->find_type("foo");
  auto bar = sch->find_type("bar");
  REQUIRE(foo);
  REQUIRE(bar);
  auto e0 = event::make(record{"babba", 1.337, 42u, 100, "bar", -4.8}, *foo);
  auto e1 = event::make(record{"yadda", record{false, "baz"}}, *bar);
  MESSAGE("event meta data queries");
  event e;
  auto tp = to<time::point>("2014-01-16+05:30:12");
  REQUIRE(tp);
  e.timestamp(*tp);
  auto t = type::alias{type{}};
  CHECK(t.name("foo"));
  CHECK(e.type(t));
  auto ast = to<expression>("&time == 2014-01-16+05:30:12");
  REQUIRE(ast);
  CHECK(visit(expr::event_evaluator{e}, *ast));
  ast = to<expression>("&type == \"foo\"");
  REQUIRE(ast);
  CHECK(visit(expr::event_evaluator{e}, *ast));
  ast = to<expression>("! &type == \"bar\"");
  REQUIRE(ast);
  CHECK(visit(expr::event_evaluator{e}, *ast));
  ast = to<expression>("&type != \"foo\"");
  REQUIRE(ast);
  CHECK(!visit(expr::event_evaluator{e}, *ast));
  MESSAGE("type queries");
  ast = to<expression>(":count == 42");
  REQUIRE(ast);
  CHECK(
    visit(expr::event_evaluator{e0}, visit(expr::type_resolver{*foo}, *ast)));
  CHECK(
    !visit(expr::event_evaluator{e1}, visit(expr::type_resolver{*bar}, *ast)));
  ast = to<expression>(":int != +101");
  REQUIRE(ast);
  CHECK(
    visit(expr::event_evaluator{e0}, visit(expr::type_resolver{*foo}, *ast)));
  CHECK(
    !visit(expr::event_evaluator{e1}, visit(expr::type_resolver{*bar}, *ast)));
  ast = to<expression>(":string ~ /bar/ && :int == +100");
  REQUIRE(ast);
  CHECK(
    visit(expr::event_evaluator{e0}, visit(expr::type_resolver{*foo}, *ast)));
  CHECK(
    !visit(expr::event_evaluator{e1}, visit(expr::type_resolver{*bar}, *ast)));
  ast = to<expression>(":real >= -4.8");
  REQUIRE(ast);
  CHECK(
    visit(expr::event_evaluator{e0}, visit(expr::type_resolver{*foo}, *ast)));
  CHECK(
    !visit(expr::event_evaluator{e1}, visit(expr::type_resolver{*bar}, *ast)));
  ast = to<expression>(
    ":int <= -3 || :int >= +100 && :string !~ /bar/ || :real > 1.0");
  REQUIRE(ast);
  CHECK(
    visit(expr::event_evaluator{e0}, visit(expr::type_resolver{*foo}, *ast)));
  // For the event of type "bar", this expression degenerates to
  // <nil> because it has no numeric types and the first predicate of the
  // conjunction in the middle renders the entire conjunction not viable.
  CHECK(
    !visit(expr::event_evaluator{e1}, visit(expr::type_resolver{*bar}, *ast)));
  MESSAGE("schema queries");
  ast = to<expression>("foo.s1 == \"babba\" && d1 <= 1337.0");
  REQUIRE(ast);
  auto schema_resolved = visit(expr::schema_resolver{*foo}, *ast);
  REQUIRE(schema_resolved);
  CHECK(visit(expr::event_evaluator{e0}, *schema_resolved));
  CHECK(!visit(expr::event_evaluator{e1}, *schema_resolved));
  ast = to<expression>("s1 != \"cheetah\"");
  REQUIRE(ast);
  schema_resolved = visit(expr::schema_resolver{*foo}, *ast);
  REQUIRE(schema_resolved);
  CHECK(visit(expr::event_evaluator{e0}, *schema_resolved));
  schema_resolved = visit(expr::schema_resolver{*bar}, *ast);
  REQUIRE(schema_resolved);
  CHECK(visit(expr::event_evaluator{e1}, *schema_resolved));
  ast = to<expression>("d1 > 0.5");
  REQUIRE(ast);
  schema_resolved = visit(expr::schema_resolver{*foo}, *ast);
  REQUIRE(schema_resolved);
  CHECK(visit(expr::event_evaluator{e0}, *schema_resolved));
  CHECK(!visit(expr::event_evaluator{e1}, *schema_resolved));
  ast = to<expression>("r.b == F");
  REQUIRE(ast);
  schema_resolved = visit(expr::schema_resolver{*bar}, *ast);
  REQUIRE(schema_resolved);
  CHECK(visit(expr::event_evaluator{e1}, *schema_resolved));
  MESSAGE("error cases");
  // Invalid prefix.
  ast = to<expression>("not.there ~ /nil/");
  REQUIRE(ast);
  schema_resolved = visit(expr::schema_resolver{*foo}, *ast);
  REQUIRE(schema_resolved);
  CHECK(is<none>(*schema_resolved));
  // 'q' doesn't exist in 'r'.
  ast = to<expression>("r.q == 80/tcp");
  REQUIRE(ast);
  schema_resolved = visit(expr::schema_resolver{*bar}, *ast);
  REQUIRE(schema_resolved);
  CHECK(is<none>(*schema_resolved));
}

TEST(AST normalization) {
  VAST_INFO("ensuring extractor position on LHS");
  auto expr = to<expression>("\"foo\" in bar");
  auto normalized = to<expression>("bar ni \"foo\"");
  REQUIRE(expr);
  REQUIRE(normalized);
  CHECK(expr::normalize(*expr) == *normalized);

  VAST_INFO("pushing down negations to predicate level");
  expr = to<expression>("! (x > 42 && x < 84)");
  normalized = to<expression>("x <= 42 || x >= 84");
  REQUIRE(expr);
  REQUIRE(normalized);
  CHECK(expr::normalize(*expr) == *normalized);

  VAST_INFO("verifying removal of negations");
  expr = to<expression>("! x < 42");
  normalized = to<expression>("x >= 42");
  REQUIRE(expr);
  REQUIRE(normalized);
  CHECK(expr::normalize(*expr) == *normalized);
  expr = to<expression>("x == 42");
  REQUIRE(expr);
  *expr = negation{expression{negation{std::move(*expr)}}};
  normalized = to<expression>("x == 42");
  REQUIRE(normalized);
  CHECK(expr::normalize(*expr) == *normalized);
  *expr = negation{std::move(*expr)};
  normalized = to<expression>("x != 42");
  REQUIRE(expr);
  REQUIRE(normalized);
  CHECK(expr::normalize(*expr) == *normalized);
  expr = to<expression>("! (x > -1 && x < +1)");
  normalized = to<expression>("x <= -1 || x >= +1");
  REQUIRE(expr);
  REQUIRE(normalized);
  CHECK(expr::normalize(*expr) == *normalized);

  VAST_INFO("performing all normalizations in one shot");
  expr = to<expression>("42 < a && ! (\"foo\" in bar || ! x == 1337)");
  normalized = to<expression>("a > 42 && bar !ni \"foo\" && x == 1337");
  REQUIRE(expr);
  REQUIRE(normalized);
  CHECK(expr::normalize(*expr) == *normalized);
}

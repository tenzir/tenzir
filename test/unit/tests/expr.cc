#include "framework/unit.h"

#include "vast/event.h"
#include "vast/expression.h"
#include "vast/schema.h"
#include "vast/expr/evaluator.h"
#include "vast/expr/resolver.h"
#include "vast/expr/normalize.h"
#include "vast/io/serialization.h"

using namespace vast;

SUITE("expression")

TEST("construction")
{
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

TEST("serialization")
{
  predicate p0{event_extractor{}, in, data{"foo"}};
  predicate p1{type_extractor{}, equal, data{time::point::utc(1983, 8, 12)}};
  expression expr{disjunction{p0, p1}};

  auto str = to_string(expr);
  std::vector<uint8_t> buf;
  io::archive(buf, expr);
  io::unarchive(buf, expr);

  CHECK(to_string(expr), str);
}

TEST("parser tests")
{
  // Event tags.
  CHECK(to<expression>("&type == \"foo\""));
  CHECK(to<expression>("&time < now - 5d10m3s"));
  CHECK(to<expression>("&id == 42"));

  // Type queries.
  CHECK(to<expression>(":port < 53/udp"));
  CHECK(to<expression>(":addr == 192.168.0.1 && :port == 80/tcp"));
  CHECK(to<expression>(":string ~ /evil.*/ && :subnet >= 10.0.0.0/8"));
  CHECK(to<expression>(":addr == 1.2.3.4 || :subnet != 10.0.0.0/8"));
  CHECK(to<expression>("! :int == +8 || ! :count < 4"));

  CHECK(to<expression>("\"she\" [+ :string"));
  CHECK(to<expression>(":string +] \"sells\""));
  CHECK(to<expression>("\"sea\" [- :string"));
  CHECK(to<expression>(":string -] \"shells\""));
  CHECK(to<expression>("\"by\" in :string"));
  CHECK(to<expression>("\"the\" !in :string"));
  CHECK(to<expression>(":string ni \"sea\""));
  CHECK(to<expression>(":string !ni \"shore\""));

  // Groups
  CHECK(to<expression>("(:real > 4.2)"));
  CHECK(to<expression>(":real > 4.2 && (:time < now || :port == 53/?)"));
  CHECK(to<expression>("(:real > 4.2 && ! (:time < now || :port == 53/?))"));

  // Invalid type name.
  CHECK(! to<expression>(":foo == -42"));
}

TEST("event evaluation")
{
  std::string str =
    "type foo = record"
    "{"
    "  s1: string,"
    "  d1: real,"
    "  c: count,"
    "  i: int,"
    "  s2: string,"
    "  d2: real"
    "}"
    "type bar = record { s1: string, r : record { b: bool, s: string } }";

  auto sch = to<schema>(str);
  REQUIRE(sch);

  auto foo = sch->find_type("foo");
  auto bar = sch->find_type("bar");
  REQUIRE(foo);
  REQUIRE(bar);

  auto e0 = event::make(record{"babba", 1.337, 42u, 100, "bar", -4.8}, *foo);
  auto e1 = event::make(record{"yadda", record{false, "baz"}}, *bar);

  //
  // Event meta data queries
  //

  event e;
  auto tp = to<time::point>("2014-01-16+05:30:12");
  REQUIRE(tp);
  e.timestamp(*tp);
  auto t = type::alias{type{}};
  CHECK(t.name("foo"));
  CHECK(e.type(t));

  auto ast = to<expression>("&time == 2014-01-16+05:30:12");
  REQUIRE(ast);
  CHECK(visit(expr::evaluator{e}, *ast));

  ast = to<expression>("&type == \"foo\"");
  REQUIRE(ast);
  CHECK(visit(expr::evaluator{e}, *ast));

  ast = to<expression>("! &type == \"bar\"");
  REQUIRE(ast);
  CHECK(visit(expr::evaluator{e}, *ast));

  ast = to<expression>("&type != \"foo\"");
  REQUIRE(ast);
  CHECK(! visit(expr::evaluator{e}, *ast));

  //
  // Type queries
  //

  ast = to<expression>(":count == 42");
  REQUIRE(ast);
  CHECK(visit(expr::evaluator{e0}, visit(expr::type_resolver{*foo}, *ast)));
  CHECK(! visit(expr::evaluator{e1}, visit(expr::type_resolver{*bar}, *ast)));

  ast = to<expression>(":int != +101");
  REQUIRE(ast);
  CHECK(visit(expr::evaluator{e0}, visit(expr::type_resolver{*foo}, *ast)));
  CHECK(! visit(expr::evaluator{e1}, visit(expr::type_resolver{*bar}, *ast)));

  ast = to<expression>(":string ~ /bar/ && :int == +100");
  REQUIRE(ast);
  CHECK(visit(expr::evaluator{e0}, visit(expr::type_resolver{*foo}, *ast)));
  CHECK(! visit(expr::evaluator{e1}, visit(expr::type_resolver{*bar}, *ast)));

  ast = to<expression>(":real >= -4.8");
  REQUIRE(ast);
  CHECK(visit(expr::evaluator{e0}, visit(expr::type_resolver{*foo}, *ast)));
  CHECK(! visit(expr::evaluator{e1}, visit(expr::type_resolver{*bar}, *ast)));

  ast = to<expression>(
      ":int <= -3 || :int >= +100 && :string !~ /bar/ || :real > 1.0");
  REQUIRE(ast);
  CHECK(visit(expr::evaluator{e0}, visit(expr::type_resolver{*foo}, *ast)));

  // For the event of type "bar", this expression degenerates to
  // <nil> because it has no numeric types and the first predicate of the
  // conjunction in the middle renders the entire conjunction not viable.
  CHECK(! visit(expr::evaluator{e1}, visit(expr::type_resolver{*bar}, *ast)));

  //
  // Schema queries
  //

  // FIXME:
  ast = to<expression>("foo.s1 == \"babba\" && d1 <= 1337.0");
  REQUIRE(ast);
  auto schema_resolved = visit(expr::schema_resolver{*foo}, *ast);
  REQUIRE(schema_resolved);
  CHECK(visit(expr::evaluator{e0}, *schema_resolved));
  CHECK(! visit(expr::evaluator{e1}, *schema_resolved));

  ast = to<expression>("s1 != \"cheetah\"");
  REQUIRE(ast);
  schema_resolved = visit(expr::schema_resolver{*foo}, *ast);
  REQUIRE(schema_resolved);
  CHECK(visit(expr::evaluator{e0}, *schema_resolved));
  schema_resolved = visit(expr::schema_resolver{*bar}, *ast);
  REQUIRE(schema_resolved);
  CHECK(visit(expr::evaluator{e1}, *schema_resolved));

  ast = to<expression>("d1 > 0.5");
  REQUIRE(ast);
  schema_resolved = visit(expr::schema_resolver{*foo}, *ast);
  REQUIRE(schema_resolved);
  CHECK(visit(expr::evaluator{e0}, *schema_resolved));
  CHECK(! visit(expr::evaluator{e1}, *schema_resolved));

  ast = to<expression>("r.b == F");
  REQUIRE(ast);
  schema_resolved = visit(expr::schema_resolver{*bar}, *ast);
  REQUIRE(schema_resolved);
  CHECK(visit(expr::evaluator{e1}, *schema_resolved));

  //
  // Error cases
  //

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

TEST("AST normalization")
{
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
  expr = to<expression>("!! x == 42");
  normalized = to<expression>("x == 42");
  REQUIRE(expr);
  REQUIRE(normalized);
  CHECK(expr::normalize(*expr) == *normalized);
  expr = to<expression>("!!! x == 42");
  normalized = to<expression>("x != 42");
  REQUIRE(expr);
  REQUIRE(normalized);
  CHECK(expr::normalize(*expr) == *normalized);
  expr = to<expression>("!! (x == 42 || a == 80/tcp)");
  normalized = to<expression>("(x == 42 || a == 80/tcp)");
  REQUIRE(expr);
  REQUIRE(normalized);
  CHECK(expr::normalize(*expr) == *normalized);
  CHECK(expr::normalize(*expr) == *normalized);
  expr = to<expression>("! (x > -1 && x < +1)");
  normalized = to<expression>("x <= -1 || x >= +1");
  REQUIRE(expr);
  REQUIRE(normalized);
  CHECK(expr::normalize(*expr) == *normalized);

  VAST_INFO("performing all normalizations in one shot");
  expr = to<expression>("42 < a && ! (\"foo\" in bar || !! x == 1337)");
  normalized = to<expression>("a > 42 && bar !ni \"foo\" && x != 1337");
  REQUIRE(expr);
  REQUIRE(normalized);
  CHECK(expr::normalize(*expr) == *normalized);
}

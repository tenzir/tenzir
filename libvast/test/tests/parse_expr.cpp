#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/vast/expression.hpp"

#define SUITE parseable
#include "test.hpp"

using namespace vast;
using namespace std::string_literals;

TEST(predicate) {
  predicate pred;
  // LHS: schema, RHS: data
  CHECK(parsers::predicate("x.y.z == 42", pred));
  CHECK(pred.lhs == schema_extractor{key{"x", "y", "z"}});
  CHECK(pred.op == equal);
  CHECK(pred.rhs == data{42u});
  // LHS: data, RHS: data
  CHECK(parsers::predicate("42 in {21,42,84}", pred));
  CHECK(pred.lhs == data{42u});
  CHECK(pred.op == in);
  CHECK(pred.rhs == data{set{21u, 42u, 84u}});
  // LHS: type, RHS: data
  CHECK(parsers::predicate("&type != \"foo\""s, pred));
  CHECK(pred.lhs == event_extractor{});
  CHECK(pred.op == not_equal);
  CHECK(pred.rhs == data{"foo"});
  // LHS: data, RHS: type
  CHECK(parsers::predicate("10.0.0.0/8 ni :addr", pred));
  CHECK(pred.lhs == data{*to<subnet>("10.0.0.0/8")});
  CHECK(pred.op == ni);
  CHECK(pred.rhs == type_extractor{type::address{}});
  // LHS: type, RHS: data
  CHECK(parsers::predicate(":real >= -4.8", pred));
  CHECK(pred.lhs == type_extractor{type::real{}});
  CHECK(pred.op == greater_equal);
  CHECK(pred.rhs == data{-4.8});
  // LHS: data, RHS: time
  CHECK(parsers::predicate("now > &time", pred));
  CHECK(is<data>(pred.lhs));
  CHECK(pred.op == greater);
  CHECK(pred.rhs == time_extractor{});
  CHECK(parsers::predicate("x == y", pred));
  CHECK(pred.lhs == schema_extractor{key{"x"}});
  CHECK(pred.op == equal);
  CHECK(pred.rhs == schema_extractor{key{"y"}});
  // Invalid type name.
  CHECK(!parsers::predicate(":foo == -42"));
}

TEST(expression) {
  expression expr;
  predicate p1{schema_extractor{key{"x"}}, equal, data{42u}};
  predicate p2{type_extractor{type::port{}}, equal, data{port{53, port::udp}}};
  predicate p3{schema_extractor{key{"a"}}, greater, schema_extractor{key{"b"}}};
  MESSAGE("conjunction");
  CHECK(parsers::expr("x == 42 && :port == 53/udp", expr));
  CHECK(expr == conjunction{p1, p2});
  CHECK(parsers::expr("x == 42 && :port == 53/udp && x == 42", expr));
  CHECK(expr == conjunction{p1, p2, p1});
  CHECK(parsers::expr("x == 42 && ! :port == 53/udp && x == 42", expr));
  CHECK(expr == conjunction{p1, negation{p2}, p1});
  CHECK(parsers::expr("x > 0 && x < 42 && a.b == x.y", expr));
  MESSAGE("disjunction");
  CHECK(parsers::expr("x == 42 || :port == 53/udp || x == 42", expr));
  CHECK(expr == disjunction{p1, p2, p1});
  CHECK(parsers::expr("a==b || b==c || c==d", expr));
  MESSAGE("negation");
  CHECK(parsers::expr("! x == 42", expr));
  CHECK(expr == negation{p1});
  CHECK(parsers::expr("!(x == 42 || :port == 53/udp)", expr));
  CHECK(expr == negation{disjunction{p1, p2}});
  MESSAGE("parentheses");
  CHECK(parsers::expr("(x == 42)", expr));
  CHECK(expr == p1);
  CHECK(parsers::expr("((x == 42))", expr));
  CHECK(expr == p1);
  CHECK(parsers::expr("x == 42 && (x == 42 || a > b)", expr));
  CHECK(expr == conjunction{p1, disjunction{p1, p3}});
  CHECK(parsers::expr("x == 42 && x == 42 || a > b && x == 42", expr));
  CHECK(expr == disjunction{conjunction{p1, p1}, conjunction{p3, p1}});
}

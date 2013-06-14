#include "test.h"
#include "vast/exception.h"
#include "vast/event.h"
#include "vast/expression.h"

using namespace vast;

struct expression_fixture
{
  expression_fixture()
  {
    event e0{"babba", 1.337, 42u, 100, "bar", -4.80};
    e0.name("foo");
    event e1{"yadda", record{false, "baz"}};
    e1.name("bar");
    events.push_back(std::move(e0));
    events.push_back(std::move(e1));
  }

  std::vector<event> events;
};

BOOST_FIXTURE_TEST_SUITE(expression_tests, expression_fixture)

BOOST_AUTO_TEST_CASE(type_queries)
{
  vast::expression expr;
  expr.parse(":count == 42");
  BOOST_CHECK(expr.eval(events[0]));
  BOOST_CHECK(! expr.eval(events[1]));
  expr.parse(":int != +101");
  BOOST_CHECK(expr.eval(events[0]));
  BOOST_CHECK(! expr.eval(events[1]));
  expr.parse(":string ~ /bar/ && :int == +100");
  BOOST_CHECK(expr.eval(events[0]));
  BOOST_CHECK(! expr.eval(events[1]));
  expr.parse(":double >= -4.8");
  BOOST_CHECK(expr.eval(events[0]));
  BOOST_CHECK(! expr.eval(events[1]));
  expr.parse(":int <= -3 || :int >= +100 && :string !~ /bar/ || :double > 1.0");
  BOOST_CHECK(expr.eval(events[0]));
  BOOST_CHECK(! expr.eval(events[1]));
}

BOOST_AUTO_TEST_CASE(event_queries)
{
  vast::schema schema;
  schema.load("event foo(s1: string, d1: double, "
              "c: count, i: int, s2: string, d2: double) "
              "event bar(s1: string, r: record { b: bool, s: string })");

  vast::expression expr;
  expr.parse("foo$s1 == \"babba\"", schema);
  BOOST_CHECK(expr.eval(events[0]));
  expr.parse("foo$d1 > 0.5", schema);
  BOOST_CHECK(expr.eval(events[0]));
  expr.parse("foo$d2 < 0.5", schema);
  BOOST_CHECK(expr.eval(events[0]));
  expr.parse("bar$r$b == F", schema);
  BOOST_CHECK(expr.eval(events[1]));
  expr.parse("bar$r$s == \"baz\"", schema);
  BOOST_CHECK(expr.eval(events[1]));

  BOOST_CHECK_THROW(
      (expr.parse("not$there ~ /nil/", schema)), // invalid event name.
      vast::error::query);

  BOOST_CHECK_THROW(
      (expr.parse("bar$puff ~ /nil/", schema)), // 'puff' is no argument.
      vast::error::schema);

  BOOST_CHECK_THROW(
      (expr.parse("bar$r$q == \"baz\"", schema)), // field 'q' does not exist.
      vast::error::schema);
}

BOOST_AUTO_TEST_CASE(offset_queries)
{
  vast::expression expr;
  event event{42u};

  expr.parse("@0 == 42");
  BOOST_CHECK(expr.eval(event));
  expr.parse("@1 != T");          // Out of bounds.
  BOOST_CHECK(! expr.eval(event));
  expr.parse("@0,3 > 4.2");       // Too deep.
  BOOST_CHECK(! expr.eval(event));

  event = {42u, record{"foo", true, 4.2}};
  expr.parse("@1,0 ~ /foo/");
  BOOST_CHECK(expr.eval(event));
  expr.parse("@1,1 == T");
  BOOST_CHECK(expr.eval(event));
  expr.parse("@1,2 == 4.2");
  BOOST_CHECK(expr.eval(event));
  expr.parse("@1,2,3 ~ /foo/");   // And again too deep.
  BOOST_CHECK(! expr.eval(event));

  event = {-1337, record{record{true, false}}};
  expr.parse("@1,0,0 == T");
  BOOST_CHECK(expr.eval(event));
  expr.parse("@1,0,1 == F");
  BOOST_CHECK(expr.eval(event));
}

BOOST_AUTO_TEST_SUITE_END()

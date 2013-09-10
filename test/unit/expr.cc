#include "test.h"
#include "vast/convert.h"
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
  auto expr = expression::parse(":count == 42");
  BOOST_CHECK(expr.eval(events[0]));
  BOOST_CHECK(! expr.eval(events[1]));

  expr = expression::parse(":int != +101");
  BOOST_CHECK(expr.eval(events[0]));
  BOOST_CHECK(expr.eval(events[1]));

  expr = expression::parse(":string ~ /bar/ && :int == +100");
  BOOST_CHECK(expr.eval(events[0]));
  BOOST_CHECK(! expr.eval(events[1]));

  expr = expression::parse(":double >= -4.8");
  BOOST_CHECK(expr.eval(events[0]));
  BOOST_CHECK(! expr.eval(events[1]));

  expr = expression::parse(
      ":int <= -3 || :int >= +100 && :string !~ /bar/ || :double > 1.0");
  BOOST_CHECK(expr.eval(events[0]));
  BOOST_CHECK(! expr.eval(events[1]));
}

BOOST_AUTO_TEST_CASE(event_queries)
{
  schema sch;
  sch.load("event foo(s1: string, d1: double, "
              "c: count, i: int, s2: string, d2: double) "
              "event bar(s1: string, r: record { b: bool, s: string })");

  auto expr = expression::parse("foo$s1 == \"babba\"", sch);
  BOOST_CHECK(expr.eval(events[0]));
  expr = expression::parse("foo$d1 > 0.5", sch);
  BOOST_CHECK(expr.eval(events[0]));
  expr = expression::parse("foo$d2 < 0.5", sch);
  BOOST_CHECK(expr.eval(events[0]));
  expr = expression::parse("bar$r$b == F", sch);
  BOOST_CHECK(expr.eval(events[1]));
  expr = expression::parse("bar$r$s == \"baz\"", sch);
  BOOST_CHECK(expr.eval(events[1]));

  BOOST_CHECK_THROW(
      (expression::parse("not$there ~ /nil/", sch)), // invalid event name.
      error::query);

  BOOST_CHECK_THROW(
      (expression::parse("bar$puff ~ /nil/", sch)), // 'puff' is no argument.
      error::schema);

  BOOST_CHECK_THROW(
      (expression::parse("bar$r$q == \"baz\"", sch)), // 'q' doesn't exist.
      error::schema);
}

BOOST_AUTO_TEST_CASE(offset_queries)
{
  event e{42u};

  BOOST_CHECK(expression::parse("@0 == 42").eval(e));
  std::cout << to<std::string>(expression::parse("@1 != T")) << std::endl;
  BOOST_CHECK(! expression::parse("@1 != T").eval(e));    // Out of bounds.
  BOOST_CHECK(! expression::parse("@0,3 > 4.2").eval(e)); // Too deep.

  e = {42u, record{"foo", true, 4.2}};
  BOOST_CHECK(expression::parse("@1,0 ~ /foo/").eval(e));
  BOOST_CHECK(expression::parse("@1,1 == T").eval(e));
  BOOST_CHECK(expression::parse("@1,2 == 4.2").eval(e));
  BOOST_CHECK(! expression::parse("@1,2,3 ~ /foo/").eval(e)); // Too deep.

  e = {-1337, record{record{true, false}}};
  BOOST_CHECK(expression::parse("@1,0,0 == T").eval(e));
  BOOST_CHECK(expression::parse("@1,0,1 == F").eval(e));
}

BOOST_AUTO_TEST_SUITE_END()

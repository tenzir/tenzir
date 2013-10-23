#include "test.h"
#include "vast/exception.h"
#include "vast/event.h"
#include "vast/expression.h"
#include "vast/io/serialization.h"

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

BOOST_AUTO_TEST_CASE(partial_order_test)
{
  BOOST_CHECK_LT(expr::ast{":port == 53/tcp"}, expr::ast{":port == 54/tcp"});
  BOOST_CHECK_LT(expr::ast{":port == 54/tcp"}, expr::ast{":port > 53/tcp"});
}

// TODO: Implement constant folding.
//BOOST_AUTO_TEST_CASE(expressions)
//{
//  auto exprs =
//  {
//    "T",
//    "53/udp",
//    "192.168.0.1 + 127.0.0.1",
//    "(42 - 24) / 2",
//    "-(42 - 24) / 2",
//    "1.2.3.4 ^ 5.6.7.8
//  };
//}

BOOST_AUTO_TEST_CASE(parser_tests)
{
  auto exprs = 
  {
    // Type queries.
    ":port < 53/udp",
    ":set != {T, F}",
    ":addr == 192.168.0.1 && :port == 80/tcp",
    ":string ~ /evil.*/ && :prefix >= 10.0.0.0/8",
    ":addr == 1.2.3.4 || :prefix != 10.0.0.0/8",
    "! :int == +8 || ! :count < 4",

    // Event tags.
    "&name == \"foo\"",
    "&time < now - 5d10m3s",
    "&id == 42",

    // Offsets.
    "@5 in {1, 2, 3}",
    "@10,3 < now - 5d10m3s",
    "@0,3,2 ~ /yikes/",
  };

  for (auto& e : exprs)
    BOOST_CHECK(expr::ast(e));

  BOOST_CHECK(! expr::ast{":foo == -42"});
}

bool bool_eval(expr::ast const& a, event const& e)
{
  return evaluate(a, e).get<bool>();
}

BOOST_AUTO_TEST_CASE(type_queries)
{
  auto ast = expr::ast(":count == 42");
  BOOST_CHECK(bool_eval(ast, events[0]));
  BOOST_CHECK(! bool_eval(ast, events[1]));

  ast = expr::ast(":int != +101");
  BOOST_CHECK(bool_eval(ast, events[0]));
  BOOST_CHECK(bool_eval(ast, events[1]));

  ast = expr::ast(":string ~ /bar/ && :int == +100");
  BOOST_CHECK(bool_eval(ast, events[0]));
  BOOST_CHECK(! bool_eval(ast, events[1]));

  ast = expr::ast(":double >= -4.8");
  BOOST_CHECK(bool_eval(ast, events[0]));
  BOOST_CHECK(! bool_eval(ast, events[1]));

  ast = expr::ast(
      ":int <= -3 || :int >= +100 && :string !~ /bar/ || :double > 1.0");
  BOOST_CHECK(bool_eval(ast, events[0]));
  BOOST_CHECK(! bool_eval(ast, events[1]));
}

BOOST_AUTO_TEST_CASE(event_queries)
{
  schema sch;
  sch.load("event foo(s1: string, d1: double, "
              "c: count, i: int, s2: string, d2: double) "
              "event bar(s1: string, r: record { b: bool, s: string })");

  auto ast = expr::ast("foo$s1 == \"babba\"", sch);
  BOOST_CHECK(bool_eval(ast, events[0]));
  ast = expr::ast("foo$d1 > 0.5", sch);
  BOOST_CHECK(bool_eval(ast, events[0]));
  ast = expr::ast("foo$d2 < 0.5", sch);
  BOOST_CHECK(bool_eval(ast, events[0]));
  ast = expr::ast("bar$r$b == F", sch);
  BOOST_CHECK(bool_eval(ast, events[1]));
  ast = expr::ast("bar$r$s == \"baz\"", sch);
  BOOST_CHECK(bool_eval(ast, events[1]));

  BOOST_CHECK_THROW(
      (expr::ast("not$there ~ /nil/", sch)), // invalid event name.
      error::query);

  BOOST_CHECK_THROW(
      (expr::ast("bar$puff ~ /nil/", sch)), // 'puff' is no argument.
      error::schema);

  BOOST_CHECK_THROW(
      (expr::ast("bar$r$q == \"baz\"", sch)), // 'q' doesn't exist.
      error::schema);
}

BOOST_AUTO_TEST_CASE(offset_queries)
{
  event e{42u};

  BOOST_CHECK(bool_eval(expr::ast{"@0 == 42"}, e));
  BOOST_CHECK(bool_eval(expr::ast{"@1 != T"}, e)); // Out of bounds, yet !=.
  BOOST_CHECK(! bool_eval(expr::ast{"@0,3 > 4.2"}, e)); // Too deep.

  e = {42u, record{"foo", true, 4.2}};
  BOOST_CHECK(bool_eval(expr::ast("@1,0 ~ /foo/"), e));
  BOOST_CHECK(bool_eval(expr::ast("@1,1 == T"), e));
  BOOST_CHECK(bool_eval(expr::ast("@1,2 == 4.2"), e));
  BOOST_CHECK(! bool_eval(expr::ast("@1,2,3 ~ /foo/"), e)); // Too deep.

  e = {-1337, record{record{true, false}}};
  BOOST_CHECK(bool_eval(expr::ast("@1,0,0 == T"), e));
  BOOST_CHECK(bool_eval(expr::ast("@1,0,1 == F"), e));
}

BOOST_AUTO_TEST_CASE(serialization)
{
  std::vector<uint8_t> buf;
  expr::ast a{":int <= -3 || :int >= +100 && :string !~ /bar/"};
  expr::ast b;
  io::archive(buf, a);
  io::unarchive(buf, b);
  BOOST_CHECK(a == b);
  BOOST_CHECK_EQUAL(to_string(a), to_string(b));
}

BOOST_AUTO_TEST_SUITE_END()

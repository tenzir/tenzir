#include "test.h"
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
  auto lhs = expr::ast{":string == \"tcp\""};
  auto rhs = expr::ast{":string != \"http\""};
  BOOST_CHECK_LT(lhs, rhs);

  lhs = expr::ast{":string == \"http\""};
  rhs = expr::ast{":string != \"http\""};
  BOOST_CHECK_LT(lhs, rhs);

  lhs = expr::ast{":port == 53/tcp"};
  rhs = expr::ast{":port == 54/tcp"};
  BOOST_CHECK_LT(lhs, rhs);

  lhs = expr::ast{":port == 54/tcp"};
  rhs = expr::ast{":port > 53/tcp"};
  BOOST_CHECK_LT(lhs, rhs);
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
    // Event tags.
    "&name == \"foo\"",
    "&time < now - 5d10m3s",
    "&id == 42",

    // Offsets.
    // Type queries.
    ":port < 53/udp",
    ":addr == 192.168.0.1 && :port == 80/tcp",
    ":string ~ /evil.*/ && :prefix >= 10.0.0.0/8",
    ":addr == 1.2.3.4 || :prefix != 10.0.0.0/8",
    "! :int == +8 || ! :count < 4",

    "@10,3 < now - 5d10m3s",
    "@0,3,2 ~ /yikes/",

    // Groups
    "(:double > 4.2)",
    ":double > 4.2 && (@4 < now || :port == 53/?)",
    "(:double > 4.2 && (@4 < now || :port == 53/?))"
  };

  for (auto& e : exprs)
    BOOST_CHECK(expr::ast(e));

  BOOST_CHECK(! expr::ast{":foo == -42"});
}

bool bool_eval(expr::ast const& a, event const& e)
{
  return evaluate(a, e).get<bool>();
}

BOOST_AUTO_TEST_CASE(tag_queries)
{
  event e;
  e.name("foo");
  e.timestamp({"2014-01-16+05:30:12"});

  auto ast = expr::ast{"&time == 2014-01-16+05:30:12"};
  BOOST_CHECK_EQUAL(evaluate(ast, e), true);

  ast = expr::ast{"&name == \"foo\""};
  BOOST_CHECK_EQUAL(evaluate(ast, e), true);
  ast = expr::ast{"&name != \"bar\""};
  BOOST_CHECK_EQUAL(evaluate(ast, e), true);
  ast = expr::ast{"&name != \"foo\""};
  BOOST_CHECK_EQUAL(evaluate(ast, e), false);
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
  auto sch = schema::load(
      "event foo(s1: string, d1: double, "
                 "c: count, i: int, s2: string, d2: double) "
      "event bar(s1: string, r: record { b: bool, s: string })");;

  BOOST_REQUIRE(sch);

  auto ast = expr::ast("foo$s1 == \"babba\"", *sch);
  BOOST_CHECK(bool_eval(ast, events[0]));
  ast = expr::ast("foo$d1 > 0.5", *sch);
  BOOST_CHECK(bool_eval(ast, events[0]));
  ast = expr::ast("foo$d2 < 0.5", *sch);
  BOOST_CHECK(bool_eval(ast, events[0]));
  ast = expr::ast("bar$r$b == F", *sch);
  BOOST_CHECK(bool_eval(ast, events[1]));
  ast = expr::ast("bar$r$s == \"baz\"", *sch);
  BOOST_CHECK(bool_eval(ast, events[1]));

  // Invalid event name.
  auto a = expr::ast::parse("not$there ~ /nil/", *sch);
  BOOST_REQUIRE(! a);

  // 'puff' is no argument.
  a = expr::ast::parse("bar$puff ~ /nil/", *sch);
  BOOST_REQUIRE(! a);

  // 'q' doesn't exist.
  a = expr::ast::parse("bar$r$q == \"baz\"", *sch);
  BOOST_REQUIRE(! a);
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

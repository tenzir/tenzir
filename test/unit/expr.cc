#include "test.h"
#include "vast/event.h"
#include "vast/expression.h"
#include "vast/io/serialization.h"

using namespace vast;

struct expression_fixture
{
  expression_fixture()
  {
    std::string str =
      "type foo : record"
      "{"
      "  s1: string,"
      "  d1: double,"
      "  c: count,"
      "  i: int,"
      "  s2: string,"
      "  d2: double"
      "}"
      "type bar : record { s1: string, r : record { b: bool, s: string } }";

    auto f = str.begin();
    auto l = str.end();
    BOOST_REQUIRE(extract(f, l, sch));
    BOOST_REQUIRE(sch.find_type("foo"));
    BOOST_REQUIRE(sch.find_type("bar"));

    event e0{"babba", 1.337, 42u, 100, "bar", -4.80};
    e0.type(sch.find_type("foo"));
    events.push_back(std::move(e0));

    event e1{"yadda", record{false, "baz"}};
    e1.type(sch.find_type("bar"));
    events.push_back(std::move(e1));
  }

  std::vector<event> events;
  schema sch;
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
    // Event tags.
  BOOST_CHECK(expr::ast{"&name == \"foo\""});
  BOOST_CHECK(expr::ast{"&time < now - 5d10m3s"});
  BOOST_CHECK(expr::ast{"&id == 42"});

  // Type queries.
  BOOST_CHECK(expr::ast{":port < 53/udp"});
  BOOST_CHECK(expr::ast{":addr == 192.168.0.1 && :port == 80/tcp"});
  BOOST_CHECK(expr::ast{":string ~ /evil.*/ && :subnet >= 10.0.0.0/8"});
  BOOST_CHECK(expr::ast{":addr == 1.2.3.4 || :subnet != 10.0.0.0/8"});
  BOOST_CHECK(expr::ast{"! :int == +8 || ! :count < 4"});

  BOOST_CHECK(expr::ast{":string [+ \"she\""});
  BOOST_CHECK(expr::ast{":string +] \"sells\""});
  BOOST_CHECK(expr::ast{":string [- \"sea\""});
  BOOST_CHECK(expr::ast{":string -] \"shells\""});
  BOOST_CHECK(expr::ast{":string in \"by\""});
  BOOST_CHECK(expr::ast{":string !in \"the\""});
  BOOST_CHECK(expr::ast{":string ni \"sea\""});
  BOOST_CHECK(expr::ast{":string !ni \"shore\""});

  // Groups
  BOOST_CHECK(expr::ast{"(:double > 4.2)"});
  BOOST_CHECK(expr::ast{":double > 4.2 && (:time < now || :port == 53/?)"});
  BOOST_CHECK(expr::ast{"(:double > 4.2 && (:time < now || :port == 53/?))"});

  // Invalid type name.
  BOOST_CHECK(! expr::ast{":foo == -42"});
}

bool bool_eval(expr::ast const& a, event const& e)
{
  return evaluate(a, e).get<bool>();
}

BOOST_AUTO_TEST_CASE(tag_queries)
{
  event e;
  e.timestamp({"2014-01-16+05:30:12"});
  e.type(type::make<invalid_type>("foo"));

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

BOOST_AUTO_TEST_CASE(schema_queries)
{
  auto plain = expr::ast::parse("foo.s1 == \"babba\"");
  BOOST_REQUIRE(plain);
  auto resolved = plain->resolve(sch);
  BOOST_REQUIRE(resolved);
  BOOST_CHECK(bool_eval(*resolved, events[0]));

  plain = expr::ast::parse("s1 != \"cheetah\"");
  BOOST_REQUIRE(plain);
  resolved = plain->resolve(sch);
  BOOST_REQUIRE(resolved);
  BOOST_CHECK(bool_eval(*resolved, events[0]));

  plain = expr::ast::parse("d1 > 0.5");
  BOOST_REQUIRE(plain);
  resolved = plain->resolve(sch);
  BOOST_REQUIRE(resolved);
  BOOST_CHECK(bool_eval(*resolved, events[0]));

  plain = expr::ast::parse("d2 < 0.5");
  BOOST_REQUIRE(plain);
  resolved = plain->resolve(sch);
  BOOST_REQUIRE(resolved);
  BOOST_CHECK(bool_eval(*resolved, events[0]));

  plain = expr::ast::parse("r.b == F");
  BOOST_REQUIRE(plain);
  resolved = plain->resolve(sch);
  BOOST_REQUIRE(resolved);
  BOOST_CHECK(bool_eval(*resolved, events[1]));

  plain = expr::ast::parse("r.s == \"baz\"");
  BOOST_REQUIRE(plain);
  resolved = plain->resolve(sch);
  BOOST_REQUIRE(resolved);
  BOOST_CHECK(bool_eval(*resolved, events[1]));

  //
  // Error cases
  //

  // Invalid event name.
  auto a = expr::ast::parse("not.there ~ /nil/");
  BOOST_REQUIRE(a);
  BOOST_CHECK(! a->resolve(sch));

  // 'puff' is no argument.
  a = expr::ast::parse("puff ~ /nil/");
  BOOST_REQUIRE(a);
  BOOST_CHECK(! a->resolve(sch));

  // 'q' doesn't exist in 'r'.
  a = expr::ast::parse("r.q == 80/tcp");
  BOOST_REQUIRE(a);
  BOOST_CHECK(! a->resolve(sch));
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

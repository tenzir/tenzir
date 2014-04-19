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
    "foo@10,3 < now - 5d10m3s",
    "foo@0,3,2 ~ /yikes/",

    // Type queries.
    ":port < 53/udp",
    ":addr == 192.168.0.1 && :port == 80/tcp",
    ":string ~ /evil.*/ && :subnet >= 10.0.0.0/8",
    ":addr == 1.2.3.4 || :subnet != 10.0.0.0/8",
    "! :int == +8 || ! :count < 4",

    ":string [+ \"foo\"",
    ":string +] \"foo\"",
    ":string [- \"foo\"",
    ":string -] \"foo\"",
    ":string in \"foo\"",
    ":string !in \"foo\"",
    ":string ni \"foo\"",
    ":string !ni \"foo\"",

    // Groups
    "(:double > 4.2)",
    ":double > 4.2 && (foo@4 < now || :port == 53/?)",
    "(:double > 4.2 && (foo@4 < now || :port == 53/?))"
  };

  for (auto& e : exprs)
    BOOST_CHECK(expr::ast(e));

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

BOOST_AUTO_TEST_CASE(schema_queries)
{
  schema sch;

  event_info ei;
  ei.name = "foo";
  ei.args.emplace_back("s1", type::make<string_type>());
  ei.args.emplace_back("d1", type::make<double_type>());
  ei.args.emplace_back("c", type::make<uint_type>());
  ei.args.emplace_back("i", type::make<int_type>());
  ei.args.emplace_back("s2", type::make<string_type>());
  ei.args.emplace_back("d2", type::make<double_type>());
  sch.add(ei);

  ei.name = "bar";
  ei.args.clear();
  ei.args.emplace_back("s1", type::make<string_type>());
  record_type r;
  r.args.emplace_back("b", type::make<bool_type>());
  r.args.emplace_back("s", type::make<string_type>());
  ei.args.emplace_back("r", type::make<record_type>(std::move(r)));
  sch.add(std::move(ei));

  auto ast = expr::ast("s1 == \"babba\"", sch);
  BOOST_CHECK(bool_eval(ast, events[0]));
  ast = expr::ast("d1 > 0.5", sch);
  BOOST_CHECK(bool_eval(ast, events[0]));
  ast = expr::ast("d2 < 0.5", sch);
  BOOST_CHECK(bool_eval(ast, events[0]));
  ast = expr::ast("r.b == F", sch);
  BOOST_CHECK(bool_eval(ast, events[1]));
  ast = expr::ast("r.s == \"baz\"", sch);
  BOOST_CHECK(bool_eval(ast, events[1]));

  // Invalid event name.
  auto a = expr::ast::parse("not.there ~ /nil/", sch);
  BOOST_REQUIRE(! a);

  // 'puff' is no argument.
  a = expr::ast::parse("puff ~ /nil/", sch);
  BOOST_REQUIRE(! a);

  // 'q' doesn't exist.
  a = expr::ast::parse("r.q == 80/tcp", sch);
  BOOST_REQUIRE(! a);
}

BOOST_AUTO_TEST_CASE(offset_queries)
{
  event e{42u};
  e.name("foo");

  BOOST_CHECK(bool_eval(expr::ast{"foo@0 == 42"}, e));
  BOOST_CHECK(bool_eval(expr::ast{"foo@1 != T"}, e)); // Out of bounds, yet !=.
  BOOST_CHECK(! bool_eval(expr::ast{"foo@0,3 > 4.2"}, e)); // Too deep.

  e = {42u, record{"foo", true, 4.2}};
  e.name("bar");
  BOOST_CHECK(bool_eval(expr::ast("bar@1,0 ~ /foo/"), e));
  BOOST_CHECK(bool_eval(expr::ast("bar@1,1 == T"), e));
  BOOST_CHECK(bool_eval(expr::ast("bar@1,2 == 4.2"), e));
  BOOST_CHECK(! bool_eval(expr::ast("bar@1,2,3 ~ /bar/"), e)); // Too deep.

  e = {-1337, record{record{true, false}}};
  e.name("baz");
  BOOST_CHECK(bool_eval(expr::ast("baz@1,0,0 == T"), e));
  BOOST_CHECK(bool_eval(expr::ast("baz@1,0,1 == F"), e));
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

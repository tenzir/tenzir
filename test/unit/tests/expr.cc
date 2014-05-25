#include "framework/unit.h"

#include "vast/event.h"
#include "vast/expression.h"
#include "vast/parse.h"
#include "vast/io/serialization.h"

using namespace vast;

SUITE("expression")

namespace {

std::vector<event> events;
schema sch;

} // namespace <anonymous>

TEST("schema setup")
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

  auto s = to<schema>(str);
  REQUIRE(s);
  sch = *s;

  REQUIRE(sch.find_type("foo"));
  REQUIRE(sch.find_type("bar"));

  event e0{"babba", 1.337, 42u, 100, "bar", -4.80};
  e0.type(sch.find_type("foo"));
  events.push_back(std::move(e0));

  event e1{"yadda", record{false, "baz"}};
  e1.type(sch.find_type("bar"));
  events.push_back(std::move(e1));
};

TEST("partial order")
{
  auto lhs = to<expr::ast>(":string == \"tcp\"");
  auto rhs = to<expr::ast>(":string != \"http\"");
  REQUIRE(lhs && rhs);
  CHECK(*lhs < *rhs);

  lhs = to<expr::ast>(":string == \"http\"");
  rhs = to<expr::ast>(":string != \"http\"");
  REQUIRE(lhs && rhs);
  CHECK(*lhs < *rhs);

  lhs = to<expr::ast>(":port == 53/tcp");
  rhs = to<expr::ast>(":port == 54/tcp");
  REQUIRE(lhs && rhs);
  CHECK(*lhs < *rhs);

  lhs = to<expr::ast>(":port == 54/tcp");
  rhs = to<expr::ast>(":port > 53/tcp");
  REQUIRE(lhs && rhs);
  CHECK(*lhs < *rhs);
}

// TODO: Implement constant folding.
//TEST("constant folding")
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

TEST("parser tests")
{
    // Event tags.
  CHECK(to<expr::ast>("&name == \"foo\""));
  CHECK(to<expr::ast>("&time < now - 5d10m3s"));
  CHECK(to<expr::ast>("&id == 42"));

  // Type queries.
  CHECK(to<expr::ast>(":port < 53/udp"));
  CHECK(to<expr::ast>(":addr == 192.168.0.1 && :port == 80/tcp"));
  CHECK(to<expr::ast>(":string ~ /evil.*/ && :subnet >= 10.0.0.0/8"));
  CHECK(to<expr::ast>(":addr == 1.2.3.4 || :subnet != 10.0.0.0/8"));
  CHECK(to<expr::ast>("! :int == +8 || ! :count < 4"));

  CHECK(to<expr::ast>(":string [+ \"she\""));
  CHECK(to<expr::ast>(":string +] \"sells\""));
  CHECK(to<expr::ast>(":string [- \"sea\""));
  CHECK(to<expr::ast>(":string -] \"shells\""));
  CHECK(to<expr::ast>(":string in \"by\""));
  CHECK(to<expr::ast>(":string !in \"the\""));
  CHECK(to<expr::ast>(":string ni \"sea\""));
  CHECK(to<expr::ast>(":string !ni \"shore\""));

  // Groups
  CHECK(to<expr::ast>("(:double > 4.2)"));
  CHECK(to<expr::ast>(":double > 4.2 && (:time < now || :port == 53/?)"));
  CHECK(to<expr::ast>("(:double > 4.2 && (:time < now || :port == 53/?))"));

  // Invalid type name.
  CHECK(! to<expr::ast>(":foo == -42"));
}

bool bool_eval(expr::ast const& a, event const& e)
{
  return evaluate(a, e).get<bool>();
}

TEST("tag queries")
{
  event e;
  e.timestamp({"2014-01-16+05:30:12"});
  e.type(type::make<invalid_type>("foo"));

  auto ast = to<expr::ast>("&time == 2014-01-16+05:30:12");
  REQUIRE(ast);
  CHECK(evaluate(*ast, e) == true);

  ast = to<expr::ast>("&name == \"foo\"");
  REQUIRE(ast);
  CHECK(evaluate(*ast, e) == true);

  ast = to<expr::ast>("&name != \"bar\"");
  REQUIRE(ast);
  CHECK(evaluate(*ast, e) == true);

  ast = to<expr::ast>("&name != \"foo\"");
  REQUIRE(ast);
  CHECK(evaluate(*ast, e) == false);
}

TEST("type queries")
{
  auto ast = to<expr::ast>(":count == 42");
  REQUIRE(ast);
  CHECK(bool_eval(*ast, events[0]));
  CHECK(! bool_eval(*ast, events[1]));

  ast = to<expr::ast>(":int != +101");
  REQUIRE(ast);
  CHECK(bool_eval(*ast, events[0]));
  CHECK(bool_eval(*ast, events[1]));

  ast = to<expr::ast>(":string ~ /bar/ && :int == +100");
  REQUIRE(ast);
  CHECK(bool_eval(*ast, events[0]));
  CHECK(! bool_eval(*ast, events[1]));

  ast = to<expr::ast>(":double >= -4.8");
  REQUIRE(ast);
  CHECK(bool_eval(*ast, events[0]));
  CHECK(! bool_eval(*ast, events[1]));

  ast = to<expr::ast>(
      ":int <= -3 || :int >= +100 && :string !~ /bar/ || :double > 1.0");
  REQUIRE(ast);
  CHECK(bool_eval(*ast, events[0]));
  CHECK(! bool_eval(*ast, events[1]));
}

TEST("schema queries")
{
  auto plain = to<expr::ast>("foo.s1 == \"babba\"");
  REQUIRE(plain);
  auto resolved = plain->resolve(sch);
  REQUIRE(resolved);
  CHECK(bool_eval(*resolved, events[0]));

  plain = to<expr::ast>("s1 != \"cheetah\"");
  REQUIRE(plain);
  resolved = plain->resolve(sch);
  REQUIRE(resolved);
  CHECK(bool_eval(*resolved, events[0]));

  plain = to<expr::ast>("d1 > 0.5");
  REQUIRE(plain);
  resolved = plain->resolve(sch);
  REQUIRE(resolved);
  CHECK(bool_eval(*resolved, events[0]));

  plain = to<expr::ast>("d2 < 0.5");
  REQUIRE(plain);
  resolved = plain->resolve(sch);
  REQUIRE(resolved);
  CHECK(bool_eval(*resolved, events[0]));

  plain = to<expr::ast>("r.b == F");
  REQUIRE(plain);
  resolved = plain->resolve(sch);
  REQUIRE(resolved);
  CHECK(bool_eval(*resolved, events[1]));

  plain = to<expr::ast>("r.s == \"baz\"");
  REQUIRE(plain);
  resolved = plain->resolve(sch);
  REQUIRE(resolved);
  CHECK(bool_eval(*resolved, events[1]));

  //
  // Error cases
  //

  // Invalid event name.
  auto a = to<expr::ast>("not.there ~ /nil/");
  REQUIRE(a);
  CHECK(! a->resolve(sch));

  // 'puff' is no argument.
  a = to<expr::ast>("puff ~ /nil/");
  REQUIRE(a);
  CHECK(! a->resolve(sch));

  // 'q' doesn't exist in 'r'.
  a = to<expr::ast>("r.q == 80/tcp");
  REQUIRE(a);
  CHECK(! a->resolve(sch));
}

TEST("serialization")
{
  std::vector<uint8_t> buf;
  REQUIRE(io::archive(buf, sch));

  schema s;
  REQUIRE(io::unarchive(buf, s));
  CHECK(s == sch);
  CHECK(to_string(s) == to_string(sch));
}

#include <boost/test/unit_test.hpp>

#include <ze.h>
#include <vast/exception.h>
#include <vast/expression.h>

std::vector<ze::event> events
{
  {"foo", "babba", 1.337, 42u, 100, "bar", -4.8},
  {"bar", "yadda", ze::record{false, "baz"}}
};

bool test_expression(std::string const& query, ze::event const& event)
{
  vast::expression expr;
  expr.parse(query);
  return expr.eval(event);
}

BOOST_AUTO_TEST_CASE(type_queries)
{
  std::vector<std::string> queries
  {
    ":count == 42",
    ":int != +101",
    ":string ~ /bar/ && :int == +100",
    ":double >= -4.8",
    ":int <= -3 || :int >= +100 && :string !~ /bar/ || :double > 1.0"
  };

  for (auto& q : queries)
    BOOST_CHECK(test_expression(q, events[0]));

  for (auto& q : queries)
    BOOST_CHECK(! test_expression(q, events[1]));
}

BOOST_AUTO_TEST_CASE(event_queries)
{
  std::vector<std::string> true_queries
  {
    ":count == 42 || :string ~ /yad.*/",
    ":count == 42 || :bool == F",
    "f*$not$yet$implemented ~ /vast/ || *$not$there$yet ~ /.*[bd]{2}a/"
  };

  std::vector<std::string> false_queries
  {
    ":string ~ /x/ || :bool == T"
  };

  for (auto& q : true_queries)
    for (auto& e : events)
      BOOST_CHECK(test_expression(q, e));

  for (auto& q : false_queries)
    for (auto& e : events)
      BOOST_CHECK(! test_expression(q, e));
}

BOOST_AUTO_TEST_CASE(offset_queries)
{
  vast::expression expr;

  ze::event event("foo", 42u);
  expr.parse("@0 == 42");
  BOOST_CHECK(expr.eval(event));
  expr.parse("@1 != T");          // Out of bounds.
  BOOST_CHECK(! expr.eval(event));
  expr.parse("@0,3 > 4.2");       // Too deep.
  BOOST_CHECK(! expr.eval(event));

  event = {"foo", 42u, ze::record{"foo", true, 4.2}};
  expr.parse("@1,0 ~ /foo/");
  BOOST_CHECK(expr.eval(event));
  expr.parse("@1,1 == T");
  BOOST_CHECK(expr.eval(event));
  expr.parse("@1,2 == 4.2");
  BOOST_CHECK(expr.eval(event));
  expr.parse("@1,2,3 ~ /foo/");   // And again too deep.
  BOOST_CHECK(! expr.eval(event));

  event = {"foo", ze::record{-1337, ze::record{ze::record{true, false}}}};
  expr.parse("@0,1,0,0 == T");
  std::cout << event << std::endl;
  BOOST_CHECK(expr.eval(event));
  expr.parse("@0,1,0,1 == F");
  BOOST_CHECK(expr.eval(event));
}

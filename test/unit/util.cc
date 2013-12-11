#include "test.h"
#include "vast/util/trial.h"
#include "vast/util/result.h"

using namespace vast;

BOOST_AUTO_TEST_CASE(errors)
{
  error e;
  error shoot{"holy cow"};
  BOOST_CHECK_EQUAL(shoot.msg(), "holy cow");
}

BOOST_AUTO_TEST_CASE(trials)
{
  trial<int> t = 42;
  BOOST_REQUIRE(t);
  BOOST_CHECK_EQUAL(*t, 42);

  trial<int> u = std::move(t);
  BOOST_REQUIRE(u);
  BOOST_CHECK_EQUAL(*u, 42);

  t = error{"whoops"};
  BOOST_CHECK(! t);
}

BOOST_AUTO_TEST_CASE(results)
{
  result<int> t;
  BOOST_REQUIRE(t.empty());
  BOOST_REQUIRE(! t.engaged());
  BOOST_REQUIRE(! t.failed());

  t = 42;
  BOOST_REQUIRE(! t.empty());
  BOOST_REQUIRE(t.engaged());
  BOOST_REQUIRE(! t.failed());

  t = error{"whoops"};
  BOOST_REQUIRE(! t.empty());
  BOOST_REQUIRE(! t.engaged());
  BOOST_REQUIRE(t.failed());

  BOOST_CHECK_EQUAL(t.failure().msg(), "whoops");
}

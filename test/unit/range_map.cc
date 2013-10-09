#include "test.h"

#include "vast/util/range_map.h"

using namespace vast::util;

BOOST_AUTO_TEST_CASE(range_map_tests)
{
  range_map<int, std::string> rm;
  BOOST_CHECK(rm.insert(42, 84, "foo"));
  auto foo = rm.lookup(42);
  BOOST_REQUIRE(foo);
  BOOST_CHECK_EQUAL(*foo, "foo");
  foo = rm.lookup(50);
  BOOST_REQUIRE(foo);
  BOOST_CHECK_EQUAL(*foo, "foo");
  foo = rm.lookup(83);
  BOOST_REQUIRE(foo);
  BOOST_CHECK_EQUAL(*foo, "foo");
  foo = rm.lookup(84);
  BOOST_CHECK(! foo);

  BOOST_CHECK(! rm.insert(42, 84, "bar"));
  BOOST_CHECK(! rm.insert(43, 100, "bar"));
  BOOST_CHECK(! rm.insert(10, 50, "bar"));
  BOOST_CHECK(! rm.insert(10, 85, "bar"));
  BOOST_CHECK(rm.insert(100, 200, "bar"));
  auto bar = rm.lookup(100);
  BOOST_REQUIRE(bar);
  BOOST_CHECK_EQUAL(*bar, "bar");
  bar = rm.lookup(150);
  BOOST_REQUIRE(bar);
  BOOST_CHECK_EQUAL(*bar, "bar");
  bar = rm.lookup(200);
  BOOST_CHECK(! bar);

  BOOST_CHECK(! rm.insert(10, 300, "baz"));
  BOOST_CHECK(! rm.insert(90, 300, "baz"));
  BOOST_CHECK(rm.insert(200, 300, "baz"));
}

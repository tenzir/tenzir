#include "framework/unit.h"

#include "vast/util/range_map.h"

SUITE("util")

using namespace vast::util;

TEST("range_map")
{
  range_map<int, std::string> rm;
  CHECK(rm.insert(42, 84, "foo"));
  auto foo = rm.lookup(42);
  REQUIRE(foo);
  CHECK(*foo == "foo");
  foo = rm.lookup(50);
  REQUIRE(foo);
  CHECK(*foo == "foo");
  foo = rm.lookup(83);
  REQUIRE(foo);
  CHECK(*foo == "foo");
  foo = rm.lookup(84);
  CHECK(! foo);

  CHECK(! rm.insert(42, 84, "bar"));
  CHECK(! rm.insert(43, 100, "bar"));
  CHECK(! rm.insert(10, 50, "bar"));
  CHECK(! rm.insert(10, 85, "bar"));
  CHECK(rm.insert(100, 200, "bar"));
  auto bar = rm.lookup(100);
  REQUIRE(bar);
  CHECK(*bar == "bar");
  bar = rm.lookup(150);
  REQUIRE(bar);
  CHECK(*bar == "bar");
  bar = rm.lookup(200);
  CHECK(! bar);

  CHECK(! rm.insert(10, 300, "baz"));
  CHECK(! rm.insert(90, 300, "baz"));
  CHECK(rm.insert(200, 300, "baz"));

  range_map<size_t, char> rm2;
  CHECK(rm2.insert(50, 99, 'a'));
  CHECK(rm2.insert(1, 50, 'b'));
}

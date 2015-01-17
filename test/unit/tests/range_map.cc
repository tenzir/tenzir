#include "framework/unit.h"

#include "vast/util/range_map.h"

SUITE("util")

using namespace vast::util;

TEST("range_map insertion")
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
  auto t = rm.find(80);
  CHECK(std::get<0>(t) == 42);
  CHECK(std::get<1>(t) == 84);
  REQUIRE(std::get<2>(t));
  CHECK(*std::get<2>(t) == "foo");
}

TEST("range_map injection")
{
  range_map<size_t, char> rm;
  CHECK(rm.inject(50, 60, 'a'));
  CHECK(rm.inject(80, 90, 'b'));
  CHECK(rm.inject(20, 30, 'c'));
  // Contained within intervals
  CHECK(! rm.inject(51, 59, 'a'));
  CHECK(! rm.inject(50, 59, 'a'));
  CHECK(! rm.inject(50, 60, 'a'));
  CHECK(! rm.inject(81, 89, 'b'));
  CHECK(! rm.inject(80, 89, 'b'));
  CHECK(! rm.inject(80, 90, 'b'));
  CHECK(! rm.inject(21, 29, 'c'));
  CHECK(! rm.inject(20, 29, 'c'));
  CHECK(! rm.inject(20, 30, 'c'));
  // Overlapping intervals
  CHECK(! rm.inject(15, 25, 'c'));
  CHECK(! rm.inject(15, 31, 'c'));
  CHECK(! rm.inject(25, 35, 'c'));
  CHECK(! rm.inject(45, 55, 'a'));
  CHECK(! rm.inject(45, 65, 'a'));
  CHECK(! rm.inject(55, 65, 'a'));
  CHECK(! rm.inject(75, 85, 'b'));
  CHECK(! rm.inject(75, 95, 'b'));
  CHECK(! rm.inject(85, 95, 'b'));
  // Wrong values
  CHECK(! rm.inject(0, 21, 'b'));
  CHECK(! rm.inject(25, 33, 'b'));
  CHECK(! rm.inject(25, 55, 'a'));
  CHECK(! rm.inject(45, 55, 'b'));
  CHECK(! rm.inject(85, 95, 'c'));
  // Insertion on the very left.
  CHECK(rm.inject(18, 20, 'c'));
  CHECK(rm.inject(10, 15, 'c'));
  CHECK(rm.inject(15, 18, 'c'));
  auto i = rm.find(15);
  CHECK(std::get<0>(i) == 10);
  CHECK(std::get<1>(i) == 30);
  REQUIRE(std::get<2>(i));
  CHECK(*std::get<2>(i) == 'c');
  // Insertion between left and middle.
  CHECK(rm.inject(48, 50, 'a'));
  CHECK(rm.inject(40, 45, 'a'));
  CHECK(rm.inject(45, 48, 'a'));
  i = rm.find(50);
  CHECK(std::get<0>(i) == 40);
  CHECK(std::get<1>(i) == 60);
  REQUIRE(std::get<2>(i));
  CHECK(*std::get<2>(i) == 'a');
  // Insertion between middle and right.
  CHECK(rm.inject(75, 80, 'b'));
  i = rm.find(80);
  CHECK(std::get<0>(i) == 75);
  CHECK(std::get<1>(i) == 90);
  REQUIRE(std::get<2>(i));
  CHECK(*std::get<2>(i) == 'b');
  // Insertion between on the very right.
  CHECK(rm.inject(90, 92, 'b'));
  CHECK(rm.inject(95, 99, 'b'));
  CHECK(rm.inject(92, 95, 'b'));
  i = rm.find(80);
  CHECK(std::get<0>(i) == 75);
  CHECK(std::get<1>(i) == 99);
}

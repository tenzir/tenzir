/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#define SUITE range_map

#include "vast/test/test.hpp"

#include "vast/detail/range_map.hpp"
#include "vast/load.hpp"
#include "vast/save.hpp"

using namespace vast;
using namespace vast::detail;

TEST(range_map insertion) {
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
  CHECK(!foo);
  CHECK(!rm.insert(42, 84, "bar"));
  CHECK(!rm.insert(43, 100, "bar"));
  CHECK(!rm.insert(10, 50, "bar"));
  CHECK(!rm.insert(10, 85, "bar"));
  CHECK(rm.insert(100, 200, "bar"));
  auto bar = rm.lookup(100);
  REQUIRE(bar);
  CHECK(*bar == "bar");
  bar = rm.lookup(150);
  REQUIRE(bar);
  CHECK(*bar == "bar");
  bar = rm.lookup(200);
  CHECK(!bar);
  CHECK(!rm.insert(10, 300, "baz"));
  CHECK(!rm.insert(90, 300, "baz"));
  CHECK(rm.insert(200, 300, "baz"));
  auto t = rm.find(80);
  CHECK(std::get<0>(t) == 42);
  CHECK(std::get<1>(t) == 84);
  REQUIRE(std::get<2>(t));
  CHECK(*std::get<2>(t) == "foo");
}

TEST(range_map injection) {
  range_map<size_t, char> rm;
  CHECK(rm.inject(50, 60, 'a'));
  CHECK(rm.inject(80, 90, 'b'));
  CHECK(rm.inject(20, 30, 'c'));

  MESSAGE("checking contained intervals");
  CHECK(!rm.inject(51, 59, 'a'));
  CHECK(!rm.inject(50, 59, 'a'));
  CHECK(!rm.inject(50, 60, 'a'));
  CHECK(!rm.inject(81, 89, 'b'));
  CHECK(!rm.inject(80, 89, 'b'));
  CHECK(!rm.inject(80, 90, 'b'));
  CHECK(!rm.inject(21, 29, 'c'));
  CHECK(!rm.inject(20, 29, 'c'));
  CHECK(!rm.inject(20, 30, 'c'));

  MESSAGE("checking overlapping intervals");
  CHECK(!rm.inject(15, 25, 'c'));
  CHECK(!rm.inject(15, 31, 'c'));
  CHECK(!rm.inject(25, 35, 'c'));
  CHECK(!rm.inject(45, 55, 'a'));
  CHECK(!rm.inject(45, 65, 'a'));
  CHECK(!rm.inject(55, 65, 'a'));
  CHECK(!rm.inject(75, 85, 'b'));
  CHECK(!rm.inject(75, 95, 'b'));
  CHECK(!rm.inject(85, 95, 'b'));

  MESSAGE("checking wrong values");
  CHECK(!rm.inject(0, 21, 'b'));
  CHECK(!rm.inject(25, 33, 'b'));
  CHECK(!rm.inject(25, 55, 'a'));
  CHECK(!rm.inject(45, 55, 'b'));
  CHECK(!rm.inject(85, 95, 'c'));

  MESSAGE("inserting on very left");
  CHECK(rm.inject(18, 20, 'c'));
  CHECK(rm.inject(10, 15, 'c'));
  CHECK(rm.inject(15, 18, 'c'));
  auto i = rm.find(15);
  CHECK(std::get<0>(i) == 10);
  CHECK(std::get<1>(i) == 30);
  REQUIRE(std::get<2>(i));
  CHECK(*std::get<2>(i) == 'c');

  MESSAGE("inserting between left and middle");
  CHECK(rm.inject(48, 50, 'a'));
  CHECK(rm.inject(40, 45, 'a'));
  CHECK(rm.inject(45, 48, 'a'));
  i = rm.find(50);
  CHECK(std::get<0>(i) == 40);
  CHECK(std::get<1>(i) == 60);
  REQUIRE(std::get<2>(i));
  CHECK(*std::get<2>(i) == 'a');

  MESSAGE("inserting between middle and right");
  CHECK(rm.inject(75, 80, 'b'));
  i = rm.find(80);
  CHECK(std::get<0>(i) == 75);
  CHECK(std::get<1>(i) == 90);
  REQUIRE(std::get<2>(i));
  CHECK(*std::get<2>(i) == 'b');

  MESSAGE("inserting on very right");
  CHECK(rm.inject(90, 92, 'b'));
  CHECK(rm.inject(95, 99, 'b'));
  CHECK(rm.inject(92, 95, 'b'));
  i = rm.find(80);
  CHECK(std::get<0>(i) == 75);
  CHECK(std::get<1>(i) == 99);
}

TEST(range_map erasure) {
  range_map<size_t, char> rm;
  rm.insert(50, 60, 'a');
  rm.insert(80, 90, 'b');
  rm.insert(20, 30, 'c');
  auto i = rm.lookup(50);
  REQUIRE(i);
  CHECK(*i == 'a');

  MESSAGE("erasing nothing");
  rm.erase(40, 50);
  i = rm.lookup(50);
  REQUIRE(i);
  CHECK(*i == 'a');

  MESSAGE("adjusting left");
  rm.erase(40, 52);
  i = rm.lookup(51);
  CHECK(!i);
  i = rm.lookup(52);
  REQUIRE(i);
  CHECK(*i == 'a');

  MESSAGE("adjusting right");
  rm.erase(58, 70);
  i = rm.lookup(58);
  CHECK(!i);
  i = rm.lookup(57);
  REQUIRE(i);
  CHECK(*i == 'a');

  MESSAGE("erasing middle");
  rm.erase(54, 56);
  i = rm.lookup(53);
  REQUIRE(i);
  CHECK(*i == 'a');
  i = rm.lookup(54);
  CHECK(!i);
  i = rm.lookup(55);
  CHECK(!i);
  i = rm.lookup(56);
  REQUIRE(i);
  CHECK(*i == 'a');

  MESSAGE("erasing multiple entirely");
  rm.erase(45, 65);
  i = rm.lookup(53);
  CHECK(!i);
  i = rm.lookup(56);
  CHECK(!i);
}

TEST(range_map serialization) {
  range_map<size_t, char> x, y;
  x.insert(50, 60, 'a');
  x.insert(80, 90, 'b');
  x.insert(20, 30, 'c');
  std::vector<char> buf;
  CHECK_EQUAL(save(nullptr, buf, x), caf::none);
  CHECK_EQUAL(load(nullptr, buf, y), caf::none);
  REQUIRE_EQUAL(y.size(), 3u);
  auto i = y.lookup(50);
  REQUIRE(i);
  CHECK(*i == 'a');
}

#include "vast/detail/flat_serial_set.hpp"

#define SUITE detail
#include "test.hpp"

using namespace vast;

TEST(flat_serial_set) {
  detail::flat_serial_set<int> set;
  MESSAGE("insert elements");
  CHECK(set.push_back(1));
  CHECK(set.push_back(2));
  CHECK(set.push_back(3));
  MESSAGE("ensure no duplicates");
  CHECK(!set.push_back(2));
  MESSAGE("test membership");
  CHECK_EQUAL(set[0], 1);
  CHECK_EQUAL(set[2], 3);
}

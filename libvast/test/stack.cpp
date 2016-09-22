#include "vast/detail/stack/vector.hpp"

using namespace vast;
using namespace vast::detail;

#define SUITE stack
#include "test.hpp"

TEST(default construction) {
  stack::vector<4, int> v;
  CHECK(v.arena.size() == 4 * sizeof(int));
  CHECK(v.arena.used() == 0);
  v.push_back(42);
  v.push_back(1337);
  v.push_back(4711);
  CHECK(v[0] == 42);
  CHECK(v[1] == 1337);
  CHECK(v[2] == 4711);
}

TEST(construction from initializer list) {
  stack::vector<4, int> v{1, 2, 3};
  REQUIRE(v.size() == 3);
  CHECK(v[0] == 1);
  CHECK(v[1] == 2);
  CHECK(v[2] == 3);
  CHECK(v.arena.used() == 3 * sizeof(int));
}


TEST(copy construction) {
  stack::vector<4, int> v{1, 2, 3};
  stack::vector<4, int> copy{v};
  REQUIRE(copy.size() == 3);
  CHECK(copy[0] == 1);
  CHECK(copy[1] == 2);
  CHECK(copy.arena.used() == 3 * sizeof(int));
}

TEST(move construction) {
  stack::vector<4, int> v{1, 2, 3};
  stack::vector<4, int> move{v};
  REQUIRE(move.size() == 3);
  CHECK(move[0] == 1);
  CHECK(move[1] == 2);
  CHECK(move.arena.used() == 3 * sizeof(int));
}

TEST(copy assignment) {
  stack::vector<4, int> v{1, 2, 3};
  auto copy = v;
  CHECK(copy.size() == 3);
  CHECK(copy[0] == 1);
  CHECK(copy[1] == 2);
}

TEST(move assignment) {
  stack::vector<4, int> v;
  REQUIRE(v.empty());
  v = {4, 5, 6, 7, 8, 9};
  REQUIRE(v.size() == 6);
  CHECK(v[0] == 4);
  CHECK(v[5] == 9);
}

TEST(insertion at end) {
  stack::vector<4, int> v;
  v.insert(v.end(), 42);
  REQUIRE(v.size() == 1);
  CHECK(v.front() == 42);
}

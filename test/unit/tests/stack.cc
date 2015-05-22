#include "vast/util/stack/vector.h"

using namespace vast;

#define SUITE util
#include "test.h"

TEST(stack vector)
{
  MESSAGE("default construction");
  util::stack::vector<4, int> v;
  CHECK(v.arena.size() == 4 * sizeof(int));
  CHECK(v.arena.used() == 0);
  v.push_back(42);
  v.push_back(1337);
  v.push_back(4711);
  CHECK(v[0] == 42);
  CHECK(v[1] == 1337);
  CHECK(v[2] == 4711);

  MESSAGE("construction from initializer list");
  util::stack::vector<4, int> w{1, 2, 3};
  REQUIRE(w.size() == 3);
  CHECK(w[0] == 1);
  CHECK(w[1] == 2);
  CHECK(w[2] == 3);
  CHECK(w.arena.used() == 3 * sizeof(int));
  
  MESSAGE("copy construction");
  util::stack::vector<4, int> copy{w};
  REQUIRE(copy.size() == 3);
  CHECK(copy[0] == 1);
  CHECK(copy[1] == 2);
  CHECK(copy.arena.used() == 3 * sizeof(int));
  
  MESSAGE("move construction");
  util::stack::vector<4, int> move{std::move(copy)};
  REQUIRE(move.size() == 3);
  CHECK(move[0] == 1);
  CHECK(move[1] == 2);
  CHECK(move.arena.used() == 3 * sizeof(int));

  MESSAGE("copy assignment");
  v = w;
  CHECK(v.size() == 3);
  CHECK(v[0] == 1);
  CHECK(v[1] == 2);

  MESSAGE("move assignment");
  v = {};
  CHECK(v.empty());
  v = {4, 5, 6, 7, 8, 9};
  REQUIRE(v.size() == 6);
  CHECK(v[0] == 4);
  CHECK(v[5] == 9);
}

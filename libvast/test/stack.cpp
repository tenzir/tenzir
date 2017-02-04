#include "vast/detail/stack_vector.hpp"

using namespace vast;

#define SUITE stack
#include "test.hpp"

using stack_vector = detail::stack_vector<int, 16>;

TEST(default construction) {
  stack_vector v;
  v.push_back(42);
  v.push_back(1337);
  v.push_back(4711);
  CHECK(v[0] == 42);
  CHECK(v[1] == 1337);
  CHECK(v[2] == 4711);
}

TEST(construction from initializer list) {
  stack_vector v{1, 2, 3};
  REQUIRE(v.size() == 3);
  CHECK(v[0] == 1);
  CHECK(v[1] == 2);
  CHECK(v[2] == 3);
}

TEST(copy construction) {
  stack_vector v{1, 2, 3};
  stack_vector copy{v};
  REQUIRE(copy.size() == 3);
  CHECK(copy[0] == 1);
  CHECK(copy[1] == 2);
}

TEST(move construction) {
  stack_vector v{1, 2, 3};
  stack_vector move{v};
  REQUIRE(move.size() == 3);
  CHECK(move[0] == 1);
  CHECK(move[1] == 2);
}

TEST(copy assignment) {
  stack_vector v{1, 2, 3};
  auto copy = v;
  CHECK(copy.size() == 3);
  CHECK(copy[0] == 1);
  CHECK(copy[1] == 2);
}

TEST(move assignment) {
  stack_vector v{1, 2};
  stack_vector w{3, 4, 5, 6, 7};
  v = std::move(w);
  REQUIRE_EQUAL(v.size(), 5u);
  CHECK_EQUAL(v[0], 3);
  CHECK_EQUAL(v[4], 7);
}

TEST(insertion at end) {
  stack_vector v;
  v.insert(v.end(), 42);
  REQUIRE(v.size() == 1);
  CHECK(v.front() == 42);
}

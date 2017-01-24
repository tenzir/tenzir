#include "vast/detail/steady_set.hpp"

#define SUITE detail
#include "test.hpp"

using namespace vast;

namespace {

using set = detail::steady_set<int>;

struct fixture {
  fixture() {
    xs.insert(1);
    xs.emplace(3);
    xs.insert(2);
  }

  set xs;
};

} // namespace <anonymous>

FIXTURE_SCOPE(steady_set_tests, fixture)

TEST(steady_set membership) {
  CHECK(xs.find(0) == xs.end());
  CHECK(xs.find(1) != xs.end());
  CHECK_EQUAL(xs.count(2), 1u);
}

TEST(steady_set insert) {
  auto i = xs.insert(0);
  CHECK(i.second);
  CHECK_EQUAL(*i.first, 0);
  CHECK_EQUAL(xs.size(), 4u);
}

TEST(steady_set erase) {
  CHECK_EQUAL(xs.erase(0), 0u);
  CHECK_EQUAL(xs.erase(1), 1u);
  auto next = xs.erase(xs.begin());
  REQUIRE(next < xs.end());
  CHECK_EQUAL(*next, 2);
  CHECK_EQUAL(xs.size(), 1u);
}

TEST(steady_set duplicates) {
  auto i = xs.insert(3);
  CHECK(!i.second);
  CHECK_EQUAL(*i.first, 3);
  CHECK_EQUAL(xs.size(), 3u);
}

TEST(steady_set comparison) {
  CHECK(xs == set{1, 3, 2});
  CHECK(xs != set{1, 2, 3});
}

FIXTURE_SCOPE_END()

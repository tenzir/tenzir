#include "framework/unit.h"

#include "vast/util/stack_vector.h"

using namespace vast;

SUITE("util")

TEST("stack container")
{
  auto v = util::stack_vector<int, 4>{1, 2, 3};
  auto c = v;
  CHECK(std::equal(v.begin(), v.end(), c.begin(), c.end()));

  auto m = std::move(v);
  CHECK(std::equal(m.begin(), m.end(), c.begin(), c.end()));
}

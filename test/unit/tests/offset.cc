#include "vast/offset.h"
#include "vast/concept/parseable/to.h"
#include "vast/concept/parseable/vast/offset.h"

#include "test.h"

using namespace vast;

TEST(offset printing)
{
  std::string str;
  offset o{0, 10, 8};
  CHECK(print(o, std::back_inserter(str)));
  CHECK(str == "0,10,8");
}

TEST(offset parsing)
{
  auto o = to<offset>("0,4,8,12");
  CHECK(o);
  CHECK(*o == offset({0,4,8,12}));
}

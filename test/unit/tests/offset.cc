#include "framework/unit.h"

#include "vast/offset.h"

using namespace vast;

SUITE("core")

TEST("offset printing")
{
  std::string str;
  offset o{0, 10, 8};
  CHECK(print(o, std::back_inserter(str)));
  CHECK(str == "0,10,8");
}

TEST("offset parsing")
{
  auto str = std::string{"0,4,8,12"};
  auto lval = str.begin();
  auto o = parse<offset>(lval, str.end());
  CHECK(o);
  CHECK(*o == offset({0,4,8,12}));
}

#include "vast/offset.h"
#include "vast/concept/parseable/to.h"
#include "vast/concept/parseable/vast/offset.h"
#include "vast/concept/printable/to_string.h"
#include "vast/concept/printable/vast/offset.h"

#include "test.h"

using namespace vast;

TEST(offset printing)
{
  auto o = offset{0, 10, 8};
  CHECK(to_string(o) == "0,10,8");
}

TEST(offset parsing)
{
  auto o = to<offset>("0,4,8,12");
  CHECK(o);
  CHECK(*o == offset({0,4,8,12}));
}

#include "test.h"
#include "vast/offset.h"

using namespace vast;

BOOST_AUTO_TEST_CASE(offset_printing)
{
  std::string str;
  offset o{0, 10, 8};
  BOOST_CHECK(print(o, std::back_inserter(str)));
  BOOST_CHECK_EQUAL(str, "0,10,8");
}

BOOST_AUTO_TEST_CASE(offset_parsing)
{
  auto str = std::string{"0,4,8,12"};
  offset o;
  auto f = str.begin();
  auto l = str.end();
  BOOST_CHECK(extract(f, l, o));
  BOOST_CHECK_EQUAL(o, offset({0,4,8,12}));
}

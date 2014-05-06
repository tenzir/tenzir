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
  auto lval = str.begin();
  auto o = parse<offset>(lval, str.end());
  BOOST_CHECK(o);
  BOOST_CHECK_EQUAL(*o, offset({0,4,8,12}));
}

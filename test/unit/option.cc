#include "test.h"
#include "vast/option.h"
#include "vast/io/serialization.h"

using namespace vast;

BOOST_AUTO_TEST_CASE(option_serialization)
{
  option<std::string> o1 = std::string{"foo"};
  decltype(o1) o2;
  std::vector<uint8_t> buf;
  io::archive(buf, o1);
  io::unarchive(buf, o2);
  BOOST_REQUIRE(o1);
  BOOST_REQUIRE(o2);
  BOOST_CHECK_EQUAL(*o2, "foo");
  BOOST_CHECK_EQUAL(*o1, *o2);
}

BOOST_AUTO_TEST_CASE(vector_option_serialization)
{
  std::vector<option<int>> v1, v2;
  v1.emplace_back(42);
  v1.emplace_back(84);
  std::vector<uint8_t> buf;
  io::archive(buf, v1);
  io::unarchive(buf, v2);
  BOOST_REQUIRE_EQUAL(v2.size(), 2);
  BOOST_CHECK_EQUAL(*v2[0], 42);
  BOOST_CHECK_EQUAL(*v2[1], 84);
}

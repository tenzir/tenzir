#include "test.h"
#include "vast/string.h"
#include "vast/util/dictionary.h"

using namespace vast;

BOOST_AUTO_TEST_CASE(map_dictionary)
{
  util::map_dictionary<string, size_t> dict;
  auto i0 = dict.insert("foo");
  auto i1 = dict.insert("bar");
  auto i2 = dict.insert("baz");
  auto i3 = dict.insert("foo");
  BOOST_CHECK(i0 != nullptr);
  BOOST_CHECK(i1 != nullptr);
  BOOST_CHECK(i2 != nullptr);
  BOOST_CHECK(i3 == nullptr);
  BOOST_CHECK_EQUAL(*i0, 0);
  BOOST_CHECK_EQUAL(*i1, 1);
  BOOST_CHECK_EQUAL(*i2, 2);
  
  i0 = dict["foo"];
  i1 = dict["bar"];
  i2 = dict["baz"];
  i3 = dict["qux"];
  BOOST_CHECK(i0 != nullptr);
  BOOST_CHECK(i1 != nullptr);
  BOOST_CHECK(i2 != nullptr);
  BOOST_CHECK(i3 == nullptr);
  BOOST_CHECK_EQUAL(*i0, 0);
  BOOST_CHECK_EQUAL(*i1, 1);
  BOOST_CHECK_EQUAL(*i2, 2);

  auto s0 = dict[0];
  auto s1 = dict[1];
  auto s2 = dict[2];
  auto s3 = dict[3];
  BOOST_CHECK(s0 != nullptr);
  BOOST_CHECK(s1 != nullptr);
  BOOST_CHECK(s2 != nullptr);
  BOOST_CHECK(s3 == nullptr);
  BOOST_CHECK_EQUAL(*s0, "foo");
  BOOST_CHECK_EQUAL(*s1, "bar");
  BOOST_CHECK_EQUAL(*s2, "baz");
}

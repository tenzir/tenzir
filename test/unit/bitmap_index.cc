#include "test.h"
#include "vast/detail/bitmap_index/string.h"
#include "vast/to_string.h"

using namespace vast;

BOOST_AUTO_TEST_CASE(string_bitmap_index)
{
  typedef null_bitstream bitstream_type;
  detail::string_bitmap_index<bitstream_type> sbi;
  bitmap_index<bitstream_type>* bi = &sbi;
  bi->push_back("foo");
  bi->push_back("bar");
  bi->push_back("baz");
  bi->push_back("foo");
  bi->push_back("foo");
  bi->push_back("bar");

  auto foo = bi->lookup("foo", equal);
  auto bar = bi->lookup("bar", equal);
  BOOST_CHECK_EQUAL(to_string(foo.bits(), false), "100110");
  BOOST_CHECK_EQUAL(to_string(bar.bits(), false), "010001");

  auto not_foo = bi->lookup("foo", not_equal);
  BOOST_CHECK_EQUAL(to_string(not_foo.bits(), false), "011001");

  BOOST_CHECK(bi->lookup("qux", equal).empty());
  BOOST_CHECK_THROW(bi->lookup("foo", match), error::index);
}

#include "test.h"
#include "vast/detail/bitmap_index/address.h"
#include "vast/detail/bitmap_index/string.h"
#include "vast/to_string.h"

using namespace vast;

template <typename Bitstream>
std::string stringify(Bitstream const& bs)
{
  return to_string(bs.bits());
}

BOOST_AUTO_TEST_CASE(address_bitmap_index)
{
  typedef null_bitstream bitstream_type;
  detail::address_bitmap_index<bitstream_type> abi;
  bitmap_index<bitstream_type>* bi = &abi;
  bi->push_back(ze::address("192.168.0.1"));
  bi->push_back(ze::address("192.168.0.2"));
  bi->push_back(ze::address("192.168.0.3"));
  bi->push_back(ze::address("192.168.0.1"));
  bi->push_back(ze::address("192.168.0.1"));
  bi->push_back(ze::address("192.168.0.2"));

  ze::address addr("192.168.0.1");
  auto bs = bi->lookup(equal, addr);
  BOOST_REQUIRE(bs);
  BOOST_CHECK_EQUAL(stringify(*bs), "011001");
  auto nbs = bi->lookup(not_equal, addr);
  BOOST_CHECK_EQUAL(stringify(*nbs), "100110");
  BOOST_CHECK(! bi->lookup(equal, ze::address("192.168.0.5")));
  BOOST_CHECK_THROW(bi->lookup(match, ze::address("::")), error::operation);

  bi->push_back(ze::address("192.168.0.128"));
  bi->push_back(ze::address("192.168.0.130"));
  bi->push_back(ze::address("192.168.0.240"));
  bi->push_back(ze::address("192.168.0.127"));

  ze::prefix pfx{"192.168.0.128", 25};
  auto pbs = bi->lookup(in, pfx);
  BOOST_REQUIRE(pbs);
  BOOST_CHECK_EQUAL(stringify(*pbs), "0111000000");
  auto npbs = bi->lookup(not_in, pfx);
  BOOST_REQUIRE(npbs);
  BOOST_CHECK_EQUAL(stringify(*npbs), "1000111111");
  pfx = {"192.168.0.0", 24};
  auto pbs2 = bi->lookup(in, pfx);
  BOOST_REQUIRE(pbs2);
  BOOST_CHECK_EQUAL(stringify(*pbs2), "1111111111");
}

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

  auto foo = bi->lookup(equal, "foo");
  auto bar = bi->lookup(equal, "bar");
  BOOST_REQUIRE(foo);
  BOOST_REQUIRE(bar);
  BOOST_CHECK_EQUAL(to_string((*foo).bits(), false), "100110");
  BOOST_CHECK_EQUAL(to_string((*bar).bits(), false), "010001");

  auto not_foo = bi->lookup(not_equal, "foo");
  BOOST_REQUIRE(not_foo);
  BOOST_CHECK_EQUAL(to_string((*not_foo).bits(), false), "011001");

  BOOST_CHECK(! bi->lookup(equal, "qux"));
  BOOST_CHECK_THROW(bi->lookup(match, "foo"), error::operation);
}

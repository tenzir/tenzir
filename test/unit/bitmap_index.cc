#include "test.h"
#include "vast/detail/bitmap_index/address.h"
#include "vast/detail/bitmap_index/port.h"
#include "vast/detail/bitmap_index/string.h"
#include "vast/to_string.h"

using namespace vast;

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
  BOOST_CHECK_EQUAL(to_string(*bs), "100110");
  auto nbs = bi->lookup(not_equal, addr);
  BOOST_CHECK_EQUAL(to_string(*nbs), "011001");
  BOOST_CHECK(! bi->lookup(equal, ze::address("192.168.0.5")));
  BOOST_CHECK_THROW(bi->lookup(match, ze::address("::")), error::operation);

  bi->push_back(ze::address("192.168.0.128"));
  bi->push_back(ze::address("192.168.0.130"));
  bi->push_back(ze::address("192.168.0.240"));
  bi->push_back(ze::address("192.168.0.127"));

  ze::prefix pfx{"192.168.0.128", 25};
  auto pbs = bi->lookup(in, pfx);
  BOOST_REQUIRE(pbs);
  BOOST_CHECK_EQUAL(to_string(*pbs), "0000001110");
  auto npbs = bi->lookup(not_in, pfx);
  BOOST_REQUIRE(npbs);
  BOOST_CHECK_EQUAL(to_string(*npbs), "1111110001");
  pfx = {"192.168.0.0", 24};
  auto pbs2 = bi->lookup(in, pfx);
  BOOST_REQUIRE(pbs2);
  BOOST_CHECK_EQUAL(to_string(*pbs2), "1111111111");

  BOOST_CHECK_EQUAL(
      bi->to_string(),
      "0000000000000000000000000000000000000000000000000000000000000000"
      "0000000000000000000000000000000011000000101010000000000000000001\n"
      "0000000000000000000000000000000000000000000000000000000000000000"
      "0000000000000000000000000000000011000000101010000000000000000010\n"
      "0000000000000000000000000000000000000000000000000000000000000000"
      "0000000000000000000000000000000011000000101010000000000000000011\n"
      "0000000000000000000000000000000000000000000000000000000000000000"
      "0000000000000000000000000000000011000000101010000000000000000001\n"
      "0000000000000000000000000000000000000000000000000000000000000000"
      "0000000000000000000000000000000011000000101010000000000000000001\n"
      "0000000000000000000000000000000000000000000000000000000000000000"
      "0000000000000000000000000000000011000000101010000000000000000010\n"
      "0000000000000000000000000000000000000000000000000000000000000000"
      "0000000000000000000000000000000011000000101010000000000010000000\n"
      "0000000000000000000000000000000000000000000000000000000000000000"
      "0000000000000000000000000000000011000000101010000000000010000010\n"
      "0000000000000000000000000000000000000000000000000000000000000000"
      "0000000000000000000000000000000011000000101010000000000011110000\n"
      "0000000000000000000000000000000000000000000000000000000000000000"
      "0000000000000000000000000000000011000000101010000000000001111111"
      );
}

BOOST_AUTO_TEST_CASE(port_bitmap_index)
{
  typedef null_bitstream bitstream_type;
  detail::port_bitmap_index<bitstream_type> pbi;
  bitmap_index<bitstream_type>* bi = &pbi;
  bi->push_back(ze::port(80, ze::port::tcp));
  bi->push_back(ze::port(443, ze::port::tcp));
  bi->push_back(ze::port(53, ze::port::udp));
  bi->push_back(ze::port(8, ze::port::icmp));
  bi->push_back(ze::port(31337, ze::port::unknown));
  bi->push_back(ze::port(80, ze::port::tcp));
  bi->push_back(ze::port(8080, ze::port::tcp));

  ze::port http(80, ze::port::tcp);
  auto pbs = bi->lookup(equal, http);
  BOOST_REQUIRE(pbs);
  BOOST_CHECK_EQUAL(to_string(*pbs), "1000010");

  ze::port priv(1024, ze::port::unknown);
  auto pbs2 = bi->lookup(less_equal, priv);
  BOOST_REQUIRE(pbs2);
  BOOST_CHECK_EQUAL(to_string(*pbs2), "1111010");

  BOOST_CHECK_EQUAL(
      bi->to_string(),
      "8\t53\t80\t443\t8080\t31337\n"
      "001111\n"
      "000111\n"
      "011111\n"
      "111111\n"
      "000001\n"
      "001111\n"
      "000011"
      );
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
  BOOST_CHECK_EQUAL(to_string(*foo), "100110");
  BOOST_CHECK_EQUAL(to_string(*bar), "010001");

  auto not_foo = bi->lookup(not_equal, "foo");
  BOOST_REQUIRE(not_foo);
  BOOST_CHECK_EQUAL(to_string(*not_foo), "011001");

  BOOST_CHECK(! bi->lookup(equal, "qux"));
  BOOST_CHECK_THROW(bi->lookup(match, "foo"), error::operation);

  BOOST_CHECK_EQUAL(
      bi->to_string(),
      "2\t1\t0\n"
      "001\n"
      "010\n"
      "100\n"
      "001\n"
      "001\n"
      "010"
      );
}

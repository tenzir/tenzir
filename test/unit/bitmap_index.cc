#include "test.h"
#include "vast/detail/bitmap_index/address.h"
#include "vast/detail/bitmap_index/arithmetic.h"
#include "vast/detail/bitmap_index/port.h"
#include "vast/detail/bitmap_index/string.h"
#include "vast/detail/bitmap_index/time.h"
#include "vast/to_string.h"

using namespace vast;

BOOST_AUTO_TEST_CASE(boolean_bitmap_index)
{
  typedef null_bitstream bitstream_type;
  detail::arithmetic_bitmap_index<ze::bool_type, bitstream_type> bbi;
  bitmap_index<bitstream_type>* bi = &bbi;
  bi->push_back(true);
  bi->push_back(true);
  bi->push_back(false);
  bi->push_back(true);
  bi->push_back(false);
  bi->push_back(false);
  bi->push_back(false);
  bi->push_back(true);

  auto f = bi->lookup(equal, false);
  BOOST_REQUIRE(f);
  BOOST_CHECK_EQUAL(to_string(*f), "00101110");
  auto t = bi->lookup(not_equal, false);
  BOOST_REQUIRE(t);
  BOOST_CHECK_EQUAL(to_string(*t), "11010001");

  BOOST_CHECK_EQUAL(
      bi->to_string(),
      "1\n"
      "1\n"
      "0\n"
      "1\n"
      "0\n"
      "0\n"
      "0\n"
      "1");
}

BOOST_AUTO_TEST_CASE(integral_bitmap_index)
{
  typedef null_bitstream bitstream_type;
  detail::arithmetic_bitmap_index<ze::int_type, bitstream_type> abi;
  bitmap_index<bitstream_type>* bi = &abi;
  bi->push_back(-7);
  bi->push_back(42);
  bi->push_back(10000);
  bi->push_back(4711);
  bi->push_back(31337);
  bi->push_back(42);
  bi->push_back(42);

  auto leet = bi->lookup(equal, 31337);
  BOOST_REQUIRE(leet);
  BOOST_CHECK_EQUAL(to_string(*leet), "0000100");
  auto less_than_leet = bi->lookup(less, 31337);
  BOOST_REQUIRE(less_than_leet);
  BOOST_CHECK_EQUAL(to_string(*less_than_leet), "1111011");
  auto greater_zero = bi->lookup(greater, 0);
  BOOST_REQUIRE(greater_zero);
  BOOST_CHECK_EQUAL(to_string(*greater_zero), "0111111");
}

BOOST_AUTO_TEST_CASE(floating_point_bitmap_index)
{
  typedef null_bitstream bitstream_type;
  detail::arithmetic_bitmap_index<ze::double_type, bitstream_type> abi(-2);
  bitmap_index<bitstream_type>* bi = &abi;
  bi->push_back(-7.8);
  bi->push_back(42.123);
  bi->push_back(10000.0);
  bi->push_back(4711.13510);
  bi->push_back(31337.3131313);
  bi->push_back(42.12258);
  bi->push_back(42.125799);

  auto fourty_two = bi->lookup(equal, 42.12);
  BOOST_REQUIRE(fourty_two);
  BOOST_CHECK_EQUAL(to_string(*fourty_two), "0100010");
  auto g_hun = bi->lookup(greater, 100.000001);
  BOOST_REQUIRE(g_hun);
  BOOST_CHECK_EQUAL(to_string(*g_hun), "0011100");
}

BOOST_AUTO_TEST_CASE(time_bitmap_index)
{
  typedef null_bitstream bitstream_type;
  detail::time_bitmap_index<bitstream_type> trbi(8);  // 0.1 sec resolution
  bitmap_index<bitstream_type>* bi = &trbi;
  bi->push_back(std::chrono::milliseconds(1000));
  bi->push_back(std::chrono::milliseconds(2000));
  bi->push_back(std::chrono::milliseconds(3000));
  bi->push_back(std::chrono::milliseconds(1011));
  bi->push_back(std::chrono::milliseconds(2222));
  bi->push_back(std::chrono::milliseconds(2322));

  auto hun = bi->lookup(equal, std::chrono::milliseconds(1034));
  BOOST_REQUIRE(hun);
  BOOST_CHECK_EQUAL(to_string(*hun), "100100");

  BOOST_CHECK_EQUAL(
      bi->to_string(),
      "10\t20\t22\t23\t30\n"
      "11111\n"
      "01111\n"
      "00001\n"
      "11111\n"
      "00111\n"
      "00011");
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
      "010");
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
      "0000000000000000000000000000000011000000101010000000000001111111");
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
      "000011");
}

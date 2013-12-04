#include "test.h"
#include "vast/bitmap_index.h"
#include "vast/io/serialization.h"
#include "vast/util/convert.h"

using namespace vast;

BOOST_AUTO_TEST_CASE(boolean_bitmap_index)
{
  arithmetic_bitmap_index<null_bitstream, bool_type> bbi, bbi2;
  bitmap_index* bi = &bbi;
  BOOST_REQUIRE(bi->push_back(true));
  BOOST_REQUIRE(bi->push_back(true));
  BOOST_REQUIRE(bi->push_back(false));
  BOOST_REQUIRE(bi->push_back(true));
  BOOST_REQUIRE(bi->push_back(false));
  BOOST_REQUIRE(bi->push_back(false));
  BOOST_REQUIRE(bi->push_back(false));
  BOOST_REQUIRE(bi->push_back(true));

  auto f = bi->lookup(equal, false);
  BOOST_REQUIRE(f);
  BOOST_CHECK_EQUAL(to_string(*f), "00101110");
  auto t = bi->lookup(not_equal, false);
  BOOST_REQUIRE(t);
  BOOST_CHECK_EQUAL(to_string(*t), "11010001");

  auto str =
    "1\n"
    "1\n"
    "0\n"
    "1\n"
    "0\n"
    "0\n"
    "0\n"
    "1\n";

  BOOST_CHECK_EQUAL(to_string(*bi), str);

  std::vector<uint8_t> buf;
  io::archive(buf, bbi);
  io::unarchive(buf, bbi2);
  BOOST_CHECK(bbi == bbi2);
  BOOST_CHECK_EQUAL(to_string(bbi2), str);
}

BOOST_AUTO_TEST_CASE(integral_bitmap_index)
{
  arithmetic_bitmap_index<null_bitstream, int_type> abi;
  bitmap_index* bi = &abi;
  BOOST_REQUIRE(bi->push_back(-7));
  BOOST_REQUIRE(bi->push_back(42));
  BOOST_REQUIRE(bi->push_back(10000));
  BOOST_REQUIRE(bi->push_back(4711));
  BOOST_REQUIRE(bi->push_back(31337));
  BOOST_REQUIRE(bi->push_back(42));
  BOOST_REQUIRE(bi->push_back(42));

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
  arithmetic_bitmap_index<null_bitstream, double_type> abi{-2};
  bitmap_index* bi = &abi;
  BOOST_REQUIRE(bi->push_back(-7.8));
  BOOST_REQUIRE(bi->push_back(42.123));
  BOOST_REQUIRE(bi->push_back(10000.0));
  BOOST_REQUIRE(bi->push_back(4711.13510));
  BOOST_REQUIRE(bi->push_back(31337.3131313));
  BOOST_REQUIRE(bi->push_back(42.12258));
  BOOST_REQUIRE(bi->push_back(42.125799));

  auto fourty_two = bi->lookup(equal, 42.12);
  BOOST_REQUIRE(fourty_two);
  BOOST_CHECK_EQUAL(to_string(*fourty_two), "0100010");
  auto g_hun = bi->lookup(greater, 100.000001);
  BOOST_REQUIRE(g_hun);
  BOOST_CHECK_EQUAL(to_string(*g_hun), "0011100");
}

BOOST_AUTO_TEST_CASE(temporal_bitmap_index)
{
  time_bitmap_index<null_bitstream> trbi{8}, trbi2;  // 0.1 sec resolution
  bitmap_index* bi = &trbi;
  BOOST_REQUIRE(bi->push_back(std::chrono::milliseconds(1000)));
  BOOST_REQUIRE(bi->push_back(std::chrono::milliseconds(2000)));
  BOOST_REQUIRE(bi->push_back(std::chrono::milliseconds(3000)));
  BOOST_REQUIRE(bi->push_back(std::chrono::milliseconds(1011)));
  BOOST_REQUIRE(bi->push_back(std::chrono::milliseconds(2222)));
  BOOST_REQUIRE(bi->push_back(std::chrono::milliseconds(2322)));

  auto hun = bi->lookup(equal, std::chrono::milliseconds(1034));
  BOOST_REQUIRE(hun);
  BOOST_CHECK_EQUAL(to_string(*hun), "100100");

  auto str =
    "10\t20\t22\t23\t30\n"
    "11111\n"
    "01111\n"
    "00001\n"
    "11111\n"
    "00111\n"
    "00011\n";

  BOOST_CHECK_EQUAL(to_string(*bi), str);

  std::vector<uint8_t> buf;
  io::archive(buf, trbi);
  io::unarchive(buf, trbi2);
  BOOST_CHECK(trbi == trbi2);
  BOOST_CHECK_EQUAL(to_string(trbi2), str);
}

BOOST_AUTO_TEST_CASE(strings_bitmap_index)
{
  string_bitmap_index<null_bitstream> sbi, sbi2;
  bitmap_index* bi = &sbi;
  BOOST_REQUIRE(bi->push_back("foo"));
  BOOST_REQUIRE(bi->push_back("bar"));
  BOOST_REQUIRE(bi->push_back("baz"));
  BOOST_REQUIRE(bi->push_back("foo"));
  BOOST_REQUIRE(bi->push_back("foo"));
  BOOST_REQUIRE(bi->push_back("bar"));

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
  BOOST_CHECK_THROW(bi->lookup(match, "foo"), std::runtime_error);

  auto str =
    "2\t1\t0\n"
    "001\n"
    "010\n"
    "100\n"
    "001\n"
    "001\n"
    "010\n";

  BOOST_CHECK_EQUAL(to_string(*bi), str);

  std::vector<uint8_t> buf;
  io::archive(buf, sbi);
  io::unarchive(buf, sbi2);
  BOOST_CHECK(sbi == sbi2);
  BOOST_CHECK_EQUAL(to_string(*sbi2.lookup(equal, "foo")), "100110");
  BOOST_CHECK_EQUAL(to_string(*sbi2.lookup(equal, "bar")), "010001");
}

BOOST_AUTO_TEST_CASE(ip_address_bitmap_index)
{
  address_bitmap_index<null_bitstream> abi, abi2;
  bitmap_index* bi = &abi;
  BOOST_REQUIRE(bi->push_back(address("192.168.0.1")));
  BOOST_REQUIRE(bi->push_back(address("192.168.0.2")));
  BOOST_REQUIRE(bi->push_back(address("192.168.0.3")));
  BOOST_REQUIRE(bi->push_back(address("192.168.0.1")));
  BOOST_REQUIRE(bi->push_back(address("192.168.0.1")));
  BOOST_REQUIRE(bi->push_back(address("192.168.0.2")));

  address addr("192.168.0.1");
  auto bs = bi->lookup(equal, addr);
  BOOST_REQUIRE(bs);
  BOOST_CHECK_EQUAL(to_string(*bs), "100110");
  auto nbs = bi->lookup(not_equal, addr);
  BOOST_CHECK_EQUAL(to_string(*nbs), "011001");
  BOOST_CHECK(! bi->lookup(equal, address{"192.168.0.5"}));
  BOOST_CHECK_THROW(bi->lookup(match, address{"::"}), std::runtime_error);

  bi->push_back(address("192.168.0.128"));
  bi->push_back(address("192.168.0.130"));
  bi->push_back(address("192.168.0.240"));
  bi->push_back(address("192.168.0.127"));

  prefix pfx{"192.168.0.128", 25};
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

  auto str =
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
    "0000000000000000000000000000000011000000101010000000000001111111\n";

  BOOST_CHECK_EQUAL(to_string(*bi), str);

  std::vector<uint8_t> buf;
  io::archive(buf, abi);
  io::unarchive(buf, abi2);
  BOOST_CHECK(abi == abi2);
  BOOST_CHECK_EQUAL(to_string(abi2), str);
}

BOOST_AUTO_TEST_CASE(transport_port_bitmap_index)
{
  port_bitmap_index<null_bitstream> pbi;
  bitmap_index* bi = &pbi;
  bi->push_back(port(80, port::tcp));
  bi->push_back(port(443, port::tcp));
  bi->push_back(port(53, port::udp));
  bi->push_back(port(8, port::icmp));
  bi->push_back(port(31337, port::unknown));
  bi->push_back(port(80, port::tcp));
  bi->push_back(port(8080, port::tcp));

  port http(80, port::tcp);
  auto pbs = bi->lookup(equal, http);
  BOOST_REQUIRE(pbs);
  BOOST_CHECK_EQUAL(to_string(*pbs), "1000010");

  port priv(1024, port::unknown);
  auto pbs2 = bi->lookup(less_equal, priv);
  BOOST_REQUIRE(pbs2);
  BOOST_CHECK_EQUAL(to_string(*pbs2), "1111010");

  BOOST_CHECK_EQUAL(
      to_string(*bi),
      "8\t53\t80\t443\t8080\t31337\n"
      "001111\n"
      "000111\n"
      "011111\n"
      "111111\n"
      "000001\n"
      "001111\n"
      "000011\n");
}

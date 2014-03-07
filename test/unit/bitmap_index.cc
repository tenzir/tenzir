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

  BOOST_CHECK_EQUAL(to_string(*bi->lookup(less, 100.0)), "1100011");
  BOOST_CHECK_EQUAL(to_string(*bi->lookup(less, 43.0)), "1100011");
  BOOST_CHECK_EQUAL(to_string(*bi->lookup(greater_equal, 42.0)), "0111111");
  BOOST_CHECK_EQUAL(to_string(*bi->lookup(not_equal, 4711.14)), "1110111");
}

BOOST_AUTO_TEST_CASE(time_point_bitmap_index)
{
  arithmetic_bitmap_index<null_bitstream, time_point_type> trbi{9}, trbi2;
  bitmap_index* bi = &trbi;
  BOOST_REQUIRE(bi->push_back(time_point{"2014-01-16+05:30:15"}));
  BOOST_REQUIRE(bi->push_back(time_point{"2014-01-16+05:30:12"}));
  BOOST_REQUIRE(bi->push_back(time_point{"2014-01-16+05:30:15"}));
  BOOST_REQUIRE(bi->push_back(time_point{"2014-01-16+05:30:18"}));
  BOOST_REQUIRE(bi->push_back(time_point{"2014-01-16+05:30:15"}));
  BOOST_REQUIRE(bi->push_back(time_point{"2014-01-16+05:30:19"}));

  auto fifteen = bi->lookup(equal, time_point{"2014-01-16+05:30:15"});
  BOOST_CHECK_EQUAL(to_string(*fifteen), "101010");

  auto twenty = bi->lookup(less, time_point{"2014-01-16+05:30:20"});
  BOOST_CHECK_EQUAL(to_string(*twenty), "111111");

  auto eighteen = bi->lookup(greater_equal, time_point{"2014-01-16+05:30:18"});
  BOOST_CHECK_EQUAL(to_string(*eighteen), "000101");

  std::vector<uint8_t> buf;
  io::archive(buf, trbi);
  io::unarchive(buf, trbi2);
  BOOST_CHECK(trbi == trbi2);
}

BOOST_AUTO_TEST_CASE(time_range_bitmap_index)
{
  // A precision of 8 translates into a resolution of 0.1 sec.
  arithmetic_bitmap_index<null_bitstream, time_range_type> trbi{8}, trbi2;
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

  auto twokay = bi->lookup(less_equal, std::chrono::milliseconds(2000));
  BOOST_REQUIRE(twokay);
  BOOST_CHECK_EQUAL(to_string(*twokay), "110100");

  auto twelve = bi->lookup(greater, std::chrono::milliseconds(1200));
  BOOST_REQUIRE(twelve);
  BOOST_CHECK_EQUAL(to_string(*twelve), "011011");

  std::vector<uint8_t> buf;
  io::archive(buf, trbi);
  io::unarchive(buf, trbi2);
  BOOST_CHECK(trbi == trbi2);
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

  bi->push_back(address{"192.168.0.128"});
  bi->push_back(address{"192.168.0.130"});
  bi->push_back(address{"192.168.0.240"});
  bi->push_back(address{"192.168.0.127"});

  auto pfx = prefix{address{"192.168.0.128"}, 25};
  auto pbs = bi->lookup(in, pfx);
  BOOST_REQUIRE(pbs);
  BOOST_CHECK_EQUAL(to_string(*pbs), "0000001110");
  auto npbs = bi->lookup(not_in, pfx);
  BOOST_REQUIRE(npbs);
  BOOST_CHECK_EQUAL(to_string(*npbs), "1111110001");
  pfx = {address{"192.168.0.0"}, 24};
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

  port http{80, port::tcp};
  auto pbs = bi->lookup(equal, http);
  BOOST_REQUIRE(pbs);
  BOOST_CHECK_EQUAL(to_string(*pbs), "1000010");

  port priv{1024, port::unknown};
  pbs = bi->lookup(less_equal, priv);
  BOOST_REQUIRE(pbs);
  BOOST_CHECK_EQUAL(to_string(*pbs), "1111010");

  pbs = bi->lookup(greater, port{2, port::unknown});
  BOOST_REQUIRE(pbs);
  BOOST_CHECK_EQUAL(to_string(*pbs), "1111111");
}

BOOST_AUTO_TEST_CASE(transport_port_bitmap_index_ewah)
{
  bitmap<uint16_t, ewah_bitstream, range_bitslice_coder> bm;
  bm.push_back(80);
  bm.push_back(443);
  bm.push_back(53);
  bm.push_back(8);
  bm.push_back(31337);
  bm.push_back(80);
  bm.push_back(8080);

  ewah_bitstream all_ones;
  all_ones.append(7, true);

  ewah_bitstream greater_eight;
  greater_eight.push_back(1);
  greater_eight.push_back(1);
  greater_eight.push_back(1);
  greater_eight.push_back(0);
  greater_eight.push_back(1);
  greater_eight.push_back(1);
  greater_eight.push_back(1);

  ewah_bitstream greater_eighty;
  greater_eighty.push_back(0);
  greater_eighty.push_back(1);
  greater_eighty.push_back(0);
  greater_eighty.push_back(0);
  greater_eighty.push_back(1);
  greater_eighty.push_back(0);
  greater_eighty.push_back(1);

  BOOST_CHECK_EQUAL(*bm.lookup(greater, 1), all_ones);
  BOOST_CHECK_EQUAL(*bm.lookup(greater, 2), all_ones);
  BOOST_CHECK_EQUAL(*bm.lookup(greater, 3), all_ones);
  BOOST_CHECK_EQUAL(*bm.lookup(greater, 4), all_ones);
  BOOST_CHECK_EQUAL(*bm.lookup(greater, 5), all_ones);
  BOOST_CHECK_EQUAL(*bm.lookup(greater, 6), all_ones);
  BOOST_CHECK_EQUAL(*bm.lookup(greater, 7), all_ones);
  BOOST_CHECK_EQUAL(*bm.lookup(greater, 8), greater_eight);
  BOOST_CHECK_EQUAL(*bm.lookup(greater, 9), greater_eight);
  BOOST_CHECK_EQUAL(*bm.lookup(greater, 10), greater_eight);
  BOOST_CHECK_EQUAL(*bm.lookup(greater, 11), greater_eight);
  BOOST_CHECK_EQUAL(*bm.lookup(greater, 12), greater_eight);
  BOOST_CHECK_EQUAL(*bm.lookup(greater, 13), greater_eight);
  BOOST_CHECK_EQUAL(*bm.lookup(greater, 80), greater_eighty);
}

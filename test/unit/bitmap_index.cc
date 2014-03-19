#include "test.h"
#include "vast/bitmap_index.h"
#include "vast/io/serialization.h"
#include "vast/util/convert.h"

using namespace vast;

BOOST_AUTO_TEST_CASE(polymorphic_bitmap_index)
{
  bitmap_index<null_bitstream> bmi;
  BOOST_REQUIRE(! bmi);

  bmi = string_bitmap_index<null_bitstream>{};
  BOOST_REQUIRE(bmi);

  BOOST_CHECK(bmi.push_back("foo"));
}

BOOST_AUTO_TEST_CASE(boolean_bitmap_index)
{
  arithmetic_bitmap_index<null_bitstream, bool_type> bmi, bmi2;
  BOOST_REQUIRE(bmi.push_back(true));
  BOOST_REQUIRE(bmi.push_back(true));
  BOOST_REQUIRE(bmi.push_back(false));
  BOOST_REQUIRE(bmi.push_back(true));
  BOOST_REQUIRE(bmi.push_back(false));
  BOOST_REQUIRE(bmi.push_back(false));
  BOOST_REQUIRE(bmi.push_back(false));
  BOOST_REQUIRE(bmi.push_back(true));

  auto f = bmi.lookup(equal, false);
  BOOST_REQUIRE(f);
  BOOST_CHECK_EQUAL(to_string(*f), "00101110");
  auto t = bmi.lookup(not_equal, false);
  BOOST_REQUIRE(t);
  BOOST_CHECK_EQUAL(to_string(*t), "11010001");

  std::vector<uint8_t> buf;
  io::archive(buf, bmi);
  io::unarchive(buf, bmi2);
  BOOST_CHECK(bmi == bmi2);
}

BOOST_AUTO_TEST_CASE(integral_bitmap_index)
{
  arithmetic_bitmap_index<null_bitstream, int_type> bmi;
  BOOST_REQUIRE(bmi.push_back(-7));
  BOOST_REQUIRE(bmi.push_back(42));
  BOOST_REQUIRE(bmi.push_back(10000));
  BOOST_REQUIRE(bmi.push_back(4711));
  BOOST_REQUIRE(bmi.push_back(31337));
  BOOST_REQUIRE(bmi.push_back(42));
  BOOST_REQUIRE(bmi.push_back(42));

  auto leet = bmi.lookup(equal, 31337);
  BOOST_REQUIRE(leet);
  BOOST_CHECK_EQUAL(to_string(*leet), "0000100");
  auto less_than_leet = bmi.lookup(less, 31337);
  BOOST_REQUIRE(less_than_leet);
  BOOST_CHECK_EQUAL(to_string(*less_than_leet), "1111011");
  auto greater_zero = bmi.lookup(greater, 0);
  BOOST_REQUIRE(greater_zero);
  BOOST_CHECK_EQUAL(to_string(*greater_zero), "0111111");
}

BOOST_AUTO_TEST_CASE(floating_point_bitmap_index)
{
  arithmetic_bitmap_index<null_bitstream, double_type> bmi{-2};
  BOOST_REQUIRE(bmi.push_back(-7.8));
  BOOST_REQUIRE(bmi.push_back(42.123));
  BOOST_REQUIRE(bmi.push_back(10000.0));
  BOOST_REQUIRE(bmi.push_back(4711.13510));
  BOOST_REQUIRE(bmi.push_back(31337.3131313));
  BOOST_REQUIRE(bmi.push_back(42.12258));
  BOOST_REQUIRE(bmi.push_back(42.125799));

  BOOST_CHECK_EQUAL(to_string(*bmi.lookup(less, 100.0)), "1100011");
  BOOST_CHECK_EQUAL(to_string(*bmi.lookup(less, 43.0)), "1100011");
  BOOST_CHECK_EQUAL(to_string(*bmi.lookup(greater_equal, 42.0)), "0111111");
  BOOST_CHECK_EQUAL(to_string(*bmi.lookup(not_equal, 4711.14)), "1110111");
}

BOOST_AUTO_TEST_CASE(time_point_bitmap_index)
{
  arithmetic_bitmap_index<null_bitstream, time_point_type> bmi{9}, bmi2;
  BOOST_REQUIRE(bmi.push_back(time_point{"2014-01-16+05:30:15"}));
  BOOST_REQUIRE(bmi.push_back(time_point{"2014-01-16+05:30:12"}));
  BOOST_REQUIRE(bmi.push_back(time_point{"2014-01-16+05:30:15"}));
  BOOST_REQUIRE(bmi.push_back(time_point{"2014-01-16+05:30:18"}));
  BOOST_REQUIRE(bmi.push_back(time_point{"2014-01-16+05:30:15"}));
  BOOST_REQUIRE(bmi.push_back(time_point{"2014-01-16+05:30:19"}));

  auto fifteen = bmi.lookup(equal, time_point{"2014-01-16+05:30:15"});
  BOOST_CHECK_EQUAL(to_string(*fifteen), "101010");

  auto twenty = bmi.lookup(less, time_point{"2014-01-16+05:30:20"});
  BOOST_CHECK_EQUAL(to_string(*twenty), "111111");

  auto eighteen = bmi.lookup(greater_equal, time_point{"2014-01-16+05:30:18"});
  BOOST_CHECK_EQUAL(to_string(*eighteen), "000101");

  std::vector<uint8_t> buf;
  io::archive(buf, bmi);
  io::unarchive(buf, bmi2);
  BOOST_CHECK(bmi == bmi2);
}

BOOST_AUTO_TEST_CASE(time_range_bitmap_index)
{
  // A precision of 8 translates into a resolution of 0.1 sec.
  arithmetic_bitmap_index<null_bitstream, time_range_type> bmi{8}, bmi2;
  BOOST_REQUIRE(bmi.push_back(std::chrono::milliseconds(1000)));
  BOOST_REQUIRE(bmi.push_back(std::chrono::milliseconds(2000)));
  BOOST_REQUIRE(bmi.push_back(std::chrono::milliseconds(3000)));
  BOOST_REQUIRE(bmi.push_back(std::chrono::milliseconds(1011)));
  BOOST_REQUIRE(bmi.push_back(std::chrono::milliseconds(2222)));
  BOOST_REQUIRE(bmi.push_back(std::chrono::milliseconds(2322)));

  auto hun = bmi.lookup(equal, std::chrono::milliseconds(1034));
  BOOST_REQUIRE(hun);
  BOOST_CHECK_EQUAL(to_string(*hun), "100100");

  auto twokay = bmi.lookup(less_equal, std::chrono::milliseconds(2000));
  BOOST_REQUIRE(twokay);
  BOOST_CHECK_EQUAL(to_string(*twokay), "110100");

  auto twelve = bmi.lookup(greater, std::chrono::milliseconds(1200));
  BOOST_REQUIRE(twelve);
  BOOST_CHECK_EQUAL(to_string(*twelve), "011011");

  std::vector<uint8_t> buf;
  io::archive(buf, bmi);
  io::unarchive(buf, bmi2);
  BOOST_CHECK(bmi == bmi2);
}

BOOST_AUTO_TEST_CASE(strings_bitmap_index)
{
  string_bitmap_index<null_bitstream> bmi, bmi2;
  BOOST_REQUIRE(bmi.push_back("foo"));
  BOOST_REQUIRE(bmi.push_back("bar"));
  BOOST_REQUIRE(bmi.push_back("baz"));
  BOOST_REQUIRE(bmi.push_back("foo"));
  BOOST_REQUIRE(bmi.push_back("foo"));
  BOOST_REQUIRE(bmi.push_back("bar"));
  BOOST_REQUIRE(bmi.push_back(""));
  BOOST_REQUIRE(bmi.push_back("qux"));
  BOOST_REQUIRE(bmi.push_back("corge"));
  BOOST_REQUIRE(bmi.push_back("bazz"));

  BOOST_CHECK_EQUAL(to_string(*bmi.lookup(equal, "foo")),   "1001100000");
  BOOST_CHECK_EQUAL(to_string(*bmi.lookup(equal, "bar")),   "0100010000");
  BOOST_CHECK_EQUAL(to_string(*bmi.lookup(equal, "baz")),   "0010000000");
  BOOST_CHECK_EQUAL(to_string(*bmi.lookup(equal, "")),      "0000001000");
  BOOST_CHECK_EQUAL(to_string(*bmi.lookup(equal, "qux")),   "0000000100");
  BOOST_CHECK_EQUAL(to_string(*bmi.lookup(equal, "corge")), "0000000010");
  BOOST_CHECK_EQUAL(to_string(*bmi.lookup(equal, "bazz")),  "0000000001");

  BOOST_CHECK_EQUAL(to_string(*bmi.lookup(not_equal, "")),    "1111110111");
  BOOST_CHECK_EQUAL(to_string(*bmi.lookup(not_equal, "foo")), "0110011111");

  BOOST_CHECK_EQUAL(to_string(*bmi.lookup(not_ni, "")), "0000000000");
  BOOST_CHECK_EQUAL(to_string(*bmi.lookup(ni, "")),     "1111111111");
  BOOST_CHECK_EQUAL(to_string(*bmi.lookup(ni, "o")),    "1001100010");
  BOOST_CHECK_EQUAL(to_string(*bmi.lookup(ni, "oo")),   "1001100000");
  BOOST_CHECK_EQUAL(to_string(*bmi.lookup(ni, "z")),    "0010000001");
  BOOST_CHECK_EQUAL(to_string(*bmi.lookup(ni, "zz")),   "0000000001");
  BOOST_CHECK_EQUAL(to_string(*bmi.lookup(ni, "ar")),   "0100010000");
  BOOST_CHECK_EQUAL(to_string(*bmi.lookup(ni, "rge")),  "0000000010");

  auto e = bmi.lookup(match, "foo");
  BOOST_CHECK(! e);

  std::vector<uint8_t> buf;
  io::archive(buf, bmi);
  io::unarchive(buf, bmi2);
  BOOST_CHECK(bmi == bmi2);
  BOOST_CHECK_EQUAL(to_string(*bmi2.lookup(equal, "foo")), "1001100000");
  BOOST_CHECK_EQUAL(to_string(*bmi2.lookup(equal, "bar")), "0100010000");
}

BOOST_AUTO_TEST_CASE(ip_address_bitmap_index)
{
  address_bitmap_index<null_bitstream> bmi, bmi2;
  BOOST_REQUIRE(bmi.push_back(address("192.168.0.1")));
  BOOST_REQUIRE(bmi.push_back(address("192.168.0.2")));
  BOOST_REQUIRE(bmi.push_back(address("192.168.0.3")));
  BOOST_REQUIRE(bmi.push_back(address("192.168.0.1")));
  BOOST_REQUIRE(bmi.push_back(address("192.168.0.1")));
  BOOST_REQUIRE(bmi.push_back(address("192.168.0.2")));

  address addr{"192.168.0.1"};
  auto bs = bmi.lookup(equal, addr);
  BOOST_REQUIRE(bs);
  BOOST_CHECK_EQUAL(to_string(*bs), "100110");
  auto nbs = bmi.lookup(not_equal, addr);
  BOOST_CHECK_EQUAL(to_string(*nbs), "011001");

  addr = address{"192.168.0.5"};
  BOOST_CHECK_EQUAL(to_string(*bmi.lookup(equal, addr)), "000000");
  BOOST_CHECK(! bmi.lookup(match, address{"::"}));

  bmi.push_back(address{"192.168.0.128"});
  bmi.push_back(address{"192.168.0.130"});
  bmi.push_back(address{"192.168.0.240"});
  bmi.push_back(address{"192.168.0.127"});

  auto pfx = prefix{address{"192.168.0.128"}, 25};
  auto pbs = bmi.lookup(in, pfx);
  BOOST_REQUIRE(pbs);
  BOOST_CHECK_EQUAL(to_string(*pbs), "0000001110");
  auto npbs = bmi.lookup(not_in, pfx);
  BOOST_REQUIRE(npbs);
  BOOST_CHECK_EQUAL(to_string(*npbs), "1111110001");
  pfx = {address{"192.168.0.0"}, 24};
  auto pbs2 = bmi.lookup(in, pfx);
  BOOST_REQUIRE(pbs2);
  BOOST_CHECK_EQUAL(to_string(*pbs2), "1111111111");

  std::vector<uint8_t> buf;
  io::archive(buf, bmi);
  io::unarchive(buf, bmi2);
  BOOST_CHECK(bmi == bmi2);
}

BOOST_AUTO_TEST_CASE(transport_port_bitmap_index)
{
  port_bitmap_index<null_bitstream> bmi;
  bmi.push_back(port(80, port::tcp));
  bmi.push_back(port(443, port::tcp));
  bmi.push_back(port(53, port::udp));
  bmi.push_back(port(8, port::icmp));
  bmi.push_back(port(31337, port::unknown));
  bmi.push_back(port(80, port::tcp));
  bmi.push_back(port(8080, port::tcp));

  port http{80, port::tcp};
  auto pbs = bmi.lookup(equal, http);
  BOOST_REQUIRE(pbs);
  BOOST_CHECK_EQUAL(to_string(*pbs), "1000010");

  port priv{1024, port::unknown};
  pbs = bmi.lookup(less_equal, priv);
  BOOST_REQUIRE(pbs);
  BOOST_CHECK_EQUAL(to_string(*pbs), "1111010");

  pbs = bmi.lookup(greater, port{2, port::unknown});
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

BOOST_AUTO_TEST_CASE(container_bitmap_index)
{
  set_bitmap_index<null_bitstream> bmi{string_type};

  set s;
  s.emplace_back("foo");
  s.emplace_back("bar");
  BOOST_CHECK(bmi.push_back(s));

  s.clear();
  s.emplace_back("qux");
  s.emplace_back("foo");
  s.emplace_back("baz");
  s.emplace_back("corge");
  BOOST_CHECK(bmi.push_back(s));

  s.clear();
  s.emplace_back("bar");
  BOOST_CHECK(bmi.push_back(s));

  s.clear();
  BOOST_CHECK(bmi.push_back(s));

  null_bitstream r;
  r.append(2, true);
  r.append(2, false);
  BOOST_CHECK_EQUAL(*bmi.lookup(in, "foo"), r);

  r.clear();
  r.append(4, false);
  BOOST_CHECK_EQUAL(*bmi.lookup(in, "not"), r);
}

#include "framework/unit.h"
#include "vast/bitmap_index.h"
#include "vast/io/serialization.h"
#include "vast/util/convert.h"

using namespace vast;

SUITE("bitmap index")


TEST("polymorphic")
{
  bitmap_index<null_bitstream> bmi;
  REQUIRE(! bmi);

  bmi = string_bitmap_index<null_bitstream>{};
  REQUIRE(bmi);

  CHECK(bmi.push_back("foo"));
}

TEST("boolean")
{
  arithmetic_bitmap_index<null_bitstream, bool_value> bmi, bmi2;
  REQUIRE(bmi.push_back(true));
  REQUIRE(bmi.push_back(true));
  REQUIRE(bmi.push_back(false));
  REQUIRE(bmi.push_back(true));
  REQUIRE(bmi.push_back(false));
  REQUIRE(bmi.push_back(false));
  REQUIRE(bmi.push_back(false));
  REQUIRE(bmi.push_back(true));

  auto f = bmi.lookup(equal, false);
  REQUIRE(f);
  CHECK(to_string(*f) == "00101110");
  auto t = bmi.lookup(not_equal, false);
  REQUIRE(t);
  CHECK(to_string(*t) == "11010001");

  std::vector<uint8_t> buf;
  io::archive(buf, bmi);
  io::unarchive(buf, bmi2);
  CHECK(bmi == bmi2);
}

TEST("integral")
{
  arithmetic_bitmap_index<null_bitstream, int_value> bmi;
  REQUIRE(bmi.push_back(-7));
  REQUIRE(bmi.push_back(42));
  REQUIRE(bmi.push_back(10000));
  REQUIRE(bmi.push_back(4711));
  REQUIRE(bmi.push_back(31337));
  REQUIRE(bmi.push_back(42));
  REQUIRE(bmi.push_back(42));

  auto leet = bmi.lookup(equal, 31337);
  REQUIRE(leet);
  CHECK(to_string(*leet) == "0000100");
  auto less_than_leet = bmi.lookup(less, 31337);
  REQUIRE(less_than_leet);
  CHECK(to_string(*less_than_leet) == "1111011");
  auto greater_zero = bmi.lookup(greater, 0);
  REQUIRE(greater_zero);
  CHECK(to_string(*greater_zero) == "0111111");
}

TEST("floating point")
{
  arithmetic_bitmap_index<null_bitstream, double_value> bmi{-2};
  REQUIRE(bmi.push_back(-7.8));
  REQUIRE(bmi.push_back(42.123));
  REQUIRE(bmi.push_back(10000.0));
  REQUIRE(bmi.push_back(4711.13510));
  REQUIRE(bmi.push_back(31337.3131313));
  REQUIRE(bmi.push_back(42.12258));
  REQUIRE(bmi.push_back(42.125799));

  CHECK(to_string(*bmi.lookup(less, 100.0)) == "1100011");
  CHECK(to_string(*bmi.lookup(less, 43.0)) == "1100011");
  CHECK(to_string(*bmi.lookup(greater_equal, 42.0)) == "0111111");
  CHECK(to_string(*bmi.lookup(not_equal, 4711.14)) == "1110111");
}

TEST("time_point")
{
  arithmetic_bitmap_index<null_bitstream, time_point_value> bmi{9}, bmi2;
  REQUIRE(bmi.push_back(time_point{"2014-01-16+05:30:15"}));
  REQUIRE(bmi.push_back(time_point{"2014-01-16+05:30:12"}));
  REQUIRE(bmi.push_back(time_point{"2014-01-16+05:30:15"}));
  REQUIRE(bmi.push_back(time_point{"2014-01-16+05:30:18"}));
  REQUIRE(bmi.push_back(time_point{"2014-01-16+05:30:15"}));
  REQUIRE(bmi.push_back(time_point{"2014-01-16+05:30:19"}));

  auto fifteen = bmi.lookup(equal, time_point{"2014-01-16+05:30:15"});
  CHECK(to_string(*fifteen) == "101010");

  auto twenty = bmi.lookup(less, time_point{"2014-01-16+05:30:20"});
  CHECK(to_string(*twenty) == "111111");

  auto eighteen = bmi.lookup(greater_equal, time_point{"2014-01-16+05:30:18"});
  CHECK(to_string(*eighteen) == "000101");

  std::vector<uint8_t> buf;
  io::archive(buf, bmi);
  io::unarchive(buf, bmi2);
  CHECK(bmi == bmi2);
}

TEST("time_range")
{
  // A precision of 8 translates into a resolution of 0.1 sec.
  arithmetic_bitmap_index<null_bitstream, time_range_value> bmi{8}, bmi2;
  REQUIRE(bmi.push_back(std::chrono::milliseconds(1000)));
  REQUIRE(bmi.push_back(std::chrono::milliseconds(2000)));
  REQUIRE(bmi.push_back(std::chrono::milliseconds(3000)));
  REQUIRE(bmi.push_back(std::chrono::milliseconds(1011)));
  REQUIRE(bmi.push_back(std::chrono::milliseconds(2222)));
  REQUIRE(bmi.push_back(std::chrono::milliseconds(2322)));

  auto hun = bmi.lookup(equal, std::chrono::milliseconds(1034));
  REQUIRE(hun);
  CHECK(to_string(*hun) == "100100");

  auto twokay = bmi.lookup(less_equal, std::chrono::milliseconds(2000));
  REQUIRE(twokay);
  CHECK(to_string(*twokay) == "110100");

  auto twelve = bmi.lookup(greater, std::chrono::milliseconds(1200));
  REQUIRE(twelve);
  CHECK(to_string(*twelve) == "011011");

  std::vector<uint8_t> buf;
  io::archive(buf, bmi);
  io::unarchive(buf, bmi2);
  CHECK(bmi == bmi2);
}

TEST("string")
{
  string_bitmap_index<null_bitstream> bmi, bmi2;
  REQUIRE(bmi.push_back("foo"));
  REQUIRE(bmi.push_back("bar"));
  REQUIRE(bmi.push_back("baz"));
  REQUIRE(bmi.push_back("foo"));
  REQUIRE(bmi.push_back("foo"));
  REQUIRE(bmi.push_back("bar"));
  REQUIRE(bmi.push_back(""));
  REQUIRE(bmi.push_back("qux"));
  REQUIRE(bmi.push_back("corge"));
  REQUIRE(bmi.push_back("bazz"));

  CHECK(to_string(*bmi.lookup(equal, "foo")) ==   "1001100000");
  CHECK(to_string(*bmi.lookup(equal, "bar")) ==   "0100010000");
  CHECK(to_string(*bmi.lookup(equal, "baz")) ==   "0010000000");
  CHECK(to_string(*bmi.lookup(equal, "")) ==      "0000001000");
  CHECK(to_string(*bmi.lookup(equal, "qux")) ==   "0000000100");
  CHECK(to_string(*bmi.lookup(equal, "corge")) == "0000000010");
  CHECK(to_string(*bmi.lookup(equal, "bazz")) ==  "0000000001");

  CHECK(to_string(*bmi.lookup(not_equal, "")) ==    "1111110111");
  CHECK(to_string(*bmi.lookup(not_equal, "foo")) == "0110011111");

  CHECK(to_string(*bmi.lookup(not_ni, "")) == "0000000000");
  CHECK(to_string(*bmi.lookup(ni, "")) ==     "1111111111");
  CHECK(to_string(*bmi.lookup(ni, "o")) ==    "1001100010");
  CHECK(to_string(*bmi.lookup(ni, "oo")) ==   "1001100000");
  CHECK(to_string(*bmi.lookup(ni, "z")) ==    "0010000001");
  CHECK(to_string(*bmi.lookup(ni, "zz")) ==   "0000000001");
  CHECK(to_string(*bmi.lookup(ni, "ar")) ==   "0100010000");
  CHECK(to_string(*bmi.lookup(ni, "rge")) ==  "0000000010");

  auto e = bmi.lookup(match, "foo");
  CHECK(! e);

  std::vector<uint8_t> buf;
  io::archive(buf, bmi);
  io::unarchive(buf, bmi2);
  CHECK(bmi == bmi2);
  CHECK(to_string(*bmi2.lookup(equal, "foo")) == "1001100000");
  CHECK(to_string(*bmi2.lookup(equal, "bar")) == "0100010000");
}

TEST("IP address")
{
  address_bitmap_index<null_bitstream> bmi, bmi2;
  REQUIRE(bmi.push_back(*address::from_v4("192.168.0.1")));
  REQUIRE(bmi.push_back(*address::from_v4("192.168.0.2")));
  REQUIRE(bmi.push_back(*address::from_v4("192.168.0.3")));
  REQUIRE(bmi.push_back(*address::from_v4("192.168.0.1")));
  REQUIRE(bmi.push_back(*address::from_v4("192.168.0.1")));
  REQUIRE(bmi.push_back(*address::from_v4("192.168.0.2")));

  auto addr = *address::from_v4("192.168.0.1");
  auto bs = bmi.lookup(equal, addr);
  REQUIRE(bs);
  CHECK(to_string(*bs) == "100110");
  auto nbs = bmi.lookup(not_equal, addr);
  CHECK(to_string(*nbs) == "011001");

  addr = *address::from_v4("192.168.0.5");
  CHECK(to_string(*bmi.lookup(equal, addr)) == "000000");
  CHECK(! bmi.lookup(match, *address::from_v6("::")));

  bmi.push_back(*address::from_v4("192.168.0.128"));
  bmi.push_back(*address::from_v4("192.168.0.130"));
  bmi.push_back(*address::from_v4("192.168.0.240"));
  bmi.push_back(*address::from_v4("192.168.0.127"));

  auto pfx = prefix{*address::from_v4("192.168.0.128"), 25};
  auto pbs = bmi.lookup(in, pfx);
  REQUIRE(pbs);
  CHECK(to_string(*pbs) == "0000001110");
  auto npbs = bmi.lookup(not_in, pfx);
  REQUIRE(npbs);
  CHECK(to_string(*npbs) == "1111110001");
  pfx = {*address::from_v4("192.168.0.0"), 24};
  auto pbs2 = bmi.lookup(in, pfx);
  REQUIRE(pbs2);
  CHECK(to_string(*pbs2) == "1111111111");

  std::vector<uint8_t> buf;
  io::archive(buf, bmi);
  io::unarchive(buf, bmi2);
  CHECK(bmi == bmi2);
}

TEST("port (null)")
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
  REQUIRE(pbs);
  CHECK(to_string(*pbs) == "1000010");

  port priv{1024, port::unknown};
  pbs = bmi.lookup(less_equal, priv);
  REQUIRE(pbs);
  CHECK(to_string(*pbs) == "1111010");

  pbs = bmi.lookup(greater, port{2, port::unknown});
  REQUIRE(pbs);
  CHECK(to_string(*pbs) == "1111111");
}

TEST("port (ewah)")
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

  CHECK(*bm.lookup(greater, 1) == all_ones);
  CHECK(*bm.lookup(greater, 2) == all_ones);
  CHECK(*bm.lookup(greater, 3) == all_ones);
  CHECK(*bm.lookup(greater, 4) == all_ones);
  CHECK(*bm.lookup(greater, 5) == all_ones);
  CHECK(*bm.lookup(greater, 6) == all_ones);
  CHECK(*bm.lookup(greater, 7) == all_ones);
  CHECK(*bm.lookup(greater, 8) == greater_eight);
  CHECK(*bm.lookup(greater, 9) == greater_eight);
  CHECK(*bm.lookup(greater, 10) == greater_eight);
  CHECK(*bm.lookup(greater, 11) == greater_eight);
  CHECK(*bm.lookup(greater, 12) == greater_eight);
  CHECK(*bm.lookup(greater, 13) == greater_eight);
  CHECK(*bm.lookup(greater, 80) == greater_eighty);
}

TEST("container")
{
  sequence_bitmap_index<null_bitstream> bmi{string_value};

  set s;
  s.emplace_back("foo");
  s.emplace_back("bar");
  CHECK(bmi.push_back(s));

  s.clear();
  s.emplace_back("qux");
  s.emplace_back("foo");
  s.emplace_back("baz");
  s.emplace_back("corge");
  CHECK(bmi.push_back(s));

  s.clear();
  s.emplace_back("bar");
  CHECK(bmi.push_back(s));

  s.clear();
  CHECK(bmi.push_back(s));

  null_bitstream r;
  r.append(2, true);
  r.append(2, false);
  CHECK(*bmi.lookup(in, "foo") == r);

  r.clear();
  r.append(4, false);
  CHECK(*bmi.lookup(in, "not") == r);

  auto v = to<vector>("you won't believe it", type::make<string_type>(), " ");
  REQUIRE(v);
  CHECK(bmi.push_back(*v));
}

#include "framework/unit.h"
#include "vast/bitmap_index_polymorphic.h"
#include "vast/concept/serializable/bitmap_index_polymorphic.h"
#include "vast/concept/serializable/io.h"
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

  std::vector<uint8_t> buf;
  save(buf, bmi);
  decltype(bmi) bmi2;
  load(buf, bmi2);
  CHECK(bmi == bmi2);
}

TEST("boolean")
{
  arithmetic_bitmap_index<null_bitstream, boolean> bmi;
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
  save(buf, bmi);
  decltype(bmi) bmi2;
  load(buf, bmi2);
  CHECK(bmi == bmi2);
}

TEST("integral")
{
  arithmetic_bitmap_index<null_bitstream, integer> bmi;
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

  std::vector<uint8_t> buf;
  save(buf, bmi);
  decltype(bmi) bmi2;
  load(buf, bmi2);
  CHECK(bmi == bmi2);
}

TEST("floating point with binning")
{
  arithmetic_bitmap_index<null_bitstream, real> bmi;
  bmi.binner(-2);

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

  std::vector<uint8_t> buf;
  save(buf, bmi);
  decltype(bmi) bmi2;
  load(buf, bmi2);
  CHECK(bmi == bmi2);
}

TEST("time_range")
{
  arithmetic_bitmap_index<null_bitstream, time::duration> bmi;

  // A precision of 8 translates into a resolution of 0.1 sec.
  bmi.binner(8);

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
  save(buf, bmi);
  decltype(bmi) bmi2;
  load(buf, bmi2);
  CHECK(bmi == bmi2);
}

TEST("time_point")
{
  arithmetic_bitmap_index<null_bitstream, time::point> bmi;
  bmi.binner(9);

  auto t = to<time::point>("2014-01-16+05:30:15", time::point::format);
  REQUIRE(t);
  REQUIRE(bmi.push_back(*t));
  t = to<time::point>("2014-01-16+05:30:12", time::point::format);
  REQUIRE(t);
  REQUIRE(bmi.push_back(*t));
  t = to<time::point>("2014-01-16+05:30:15", time::point::format);
  REQUIRE(t);
  REQUIRE(bmi.push_back(*t));
  t = to<time::point>("2014-01-16+05:30:18", time::point::format);
  REQUIRE(t);
  REQUIRE(bmi.push_back(*t));
  t = to<time::point>("2014-01-16+05:30:15", time::point::format);
  REQUIRE(t);
  REQUIRE(bmi.push_back(*t));
  t = to<time::point>("2014-01-16+05:30:19", time::point::format);
  REQUIRE(t);
  REQUIRE(bmi.push_back(*t));

  t = to<time::point>("2014-01-16+05:30:15", time::point::format);
  REQUIRE(t);
  auto fifteen = bmi.lookup(equal, *t);
  CHECK(to_string(*fifteen) == "101010");

  t = to<time::point>("2014-01-16+05:30:20", time::point::format);
  REQUIRE(t);
  auto twenty = bmi.lookup(less, *t);
  CHECK(to_string(*twenty) == "111111");

  t = to<time::point>("2014-01-16+05:30:18", time::point::format);
  REQUIRE(t);
  auto eighteen = bmi.lookup(greater_equal, *t);
  CHECK(to_string(*eighteen) == "000101");

  std::vector<uint8_t> buf;
  save(buf, bmi);
  decltype(bmi) bmi2;
  load(buf, bmi2);
  CHECK(bmi == bmi2);
}

TEST("string")
{
  string_bitmap_index<null_bitstream> bmi;
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
  save(buf, bmi);
  decltype(bmi) bmi2;
  load(buf, bmi2);
  CHECK(bmi == bmi2);
  CHECK(to_string(*bmi2.lookup(equal, "foo")) == "1001100000");
  CHECK(to_string(*bmi2.lookup(equal, "bar")) == "0100010000");
}

TEST("IP address")
{
  address_bitmap_index<null_bitstream> bmi;
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

  bs = bmi.lookup(not_equal, addr);
  CHECK(to_string(*bs) == "011001");

  addr = *address::from_v4("192.168.0.5");
  CHECK(to_string(*bmi.lookup(equal, addr)) == "000000");

  CHECK(! bmi.lookup(match, *address::from_v6("::"))); // Invalid operator

  bmi.push_back(*address::from_v4("192.168.0.128"));
  bmi.push_back(*address::from_v4("192.168.0.130"));
  bmi.push_back(*address::from_v4("192.168.0.240"));
  bmi.push_back(*address::from_v4("192.168.0.127"));

  auto sub = subnet{*address::from_v4("192.168.0.128"), 25};
  bs = bmi.lookup(in, sub);
  REQUIRE(bs);
  CHECK(to_string(*bs) == "0000001110");

  bs = bmi.lookup(not_in, sub);
  REQUIRE(bs);
  CHECK(to_string(*bs) == "1111110001");

  sub = {*address::from_v4("192.168.0.0"), 24};
  bs = bmi.lookup(in, sub);
  REQUIRE(bs);
  CHECK(to_string(*bs) == "1111111111");

  std::vector<uint8_t> buf;
  save(buf, bmi);
  decltype(bmi) bmi2;
  load(buf, bmi2);
  CHECK(bmi == bmi2);
}

TEST("subnet")
{
  subnet_bitmap_index<null_bitstream> bmi;

  auto s0 = to<subnet>("192.168.0.0/24");
  auto s1 = to<subnet>("192.168.1.0/24");
  auto s2 = to<subnet>("::/40");
  REQUIRE(s0);
  REQUIRE(s1);
  REQUIRE(s2);

  REQUIRE(bmi.push_back(*s0));
  REQUIRE(bmi.push_back(*s1));
  REQUIRE(bmi.push_back(*s0));
  REQUIRE(bmi.push_back(*s0));
  REQUIRE(bmi.push_back(*s2));
  REQUIRE(bmi.push_back(*s2));

  auto bs = bmi.lookup(equal, *s0);
  REQUIRE(bs);
  CHECK(to_string(*bs) == "101100");

  bs = bmi.lookup(not_equal, *s1);
  REQUIRE(bs);
  CHECK(to_string(*bs) == "101111");

  std::vector<uint8_t> buf;
  save(buf, bmi);
  decltype(bmi) bmi2;
  load(buf, bmi2);
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

  std::vector<uint8_t> buf;
  save(buf, bmi);
  decltype(bmi) bmi2;
  load(buf, bmi2);
  CHECK(bmi == bmi2);
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
  sequence_bitmap_index<null_bitstream> bmi{type::string{}};

  vector v{"foo", "bar"};
  CHECK(bmi.push_back(v));

  v = {"qux", "foo", "baz", "corge"};
  CHECK(bmi.push_back(v));

  v = {"bar"};
  CHECK(bmi.push_back(v));
  CHECK(bmi.push_back(v));

  null_bitstream r;
  r.append(2, true);
  r.append(2, false);
  CHECK(*bmi.lookup(in, "foo") == r);

  r.clear();
  r.push_back(true);
  r.push_back(false);
  r.append(2, true);
  CHECK(*bmi.lookup(in, "bar") == r);

  r.clear();
  r.append(4, false);
  CHECK(*bmi.lookup(in, "not") == r);

  auto strings = to<vector>("[you won't believe it]", type::string{}, " ");
  REQUIRE(strings);
  CHECK(bmi.push_back(*strings));

  std::vector<uint8_t> buf;
  save(buf, bmi);
  decltype(bmi) bmi2;
  load(buf, bmi2);
  CHECK(bmi == bmi2);
}

TEST("offset push-back")
{
  string_bitmap_index<null_bitstream> bmi;
  REQUIRE(bmi.push_back("foo", 2));
  REQUIRE(bmi.push_back(data{"bar"}, 3));
  REQUIRE(bmi.push_back(nil, 5));
  REQUIRE(bmi.push_back("baz", 7));

  auto r = bmi.lookup(equal, "foo");
  REQUIRE(r);
  CHECK(to_string(*r) == "00100000");

  r = bmi.lookup(not_equal, "foo");
  REQUIRE(r);
  CHECK(to_string(*r) == "00010101");

  r = bmi.lookup(ni, "a");
  REQUIRE(r);
  CHECK(to_string(*r) == "00010001");

  r = bmi.lookup(equal, nil);
  REQUIRE(r);
  CHECK(to_string(*r) == "00000100");

  r = bmi.lookup(not_equal, nil);
  REQUIRE(r);
  CHECK(to_string(*r) == "00110001");
}

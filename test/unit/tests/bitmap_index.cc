#include "vast/bitmap_index_polymorphic.h"
#include "vast/concept/serializable/bitmap_index_polymorphic.h"
#include "vast/concept/serializable/io.h"
#include "vast/util/convert.h"

#define SUITE bitmap_index
#include "test.h"

using namespace vast;

TEST(polymorphic)
{
  bitmap_index<null_bitstream> bmi;
  REQUIRE(! bmi);
  bmi = string_bitmap_index<null_bitstream>{};
  REQUIRE(bmi);
  CHECK(bmi.push_back("foo"));

  MESSAGE("serialization");
  std::vector<uint8_t> buf;
  save(buf, bmi);
  decltype(bmi) bmi2;
  load(buf, bmi2);
  CHECK(bmi == bmi2);
}

TEST(boolean)
{
  arithmetic_bitmap_index<null_bitstream, boolean> bmi;
  CHECK(bmi.push_back(true));
  CHECK(bmi.push_back(true));
  CHECK(bmi.push_back(false));
  CHECK(bmi.push_back(true));
  CHECK(bmi.push_back(false));
  CHECK(bmi.push_back(false));
  CHECK(bmi.push_back(false));
  CHECK(bmi.push_back(true));

  auto f = bmi.lookup(equal, false);
  REQUIRE(f);
  CHECK(to_string(*f) == "00101110");
  auto t = bmi.lookup(not_equal, false);
  REQUIRE(t);
  CHECK(to_string(*t) == "11010001");

  MESSAGE("serialization");
  std::vector<uint8_t> buf;
  save(buf, bmi);
  decltype(bmi) bmi2;
  load(buf, bmi2);
  CHECK(bmi == bmi2);
}

TEST(integral)
{
  arithmetic_bitmap_index<null_bitstream, integer> bmi;
  CHECK(bmi.push_back(-7));
  CHECK(bmi.push_back(42));
  CHECK(bmi.push_back(10000));
  CHECK(bmi.push_back(4711));
  CHECK(bmi.push_back(31337));
  CHECK(bmi.push_back(42));
  CHECK(bmi.push_back(42));

  auto leet = bmi.lookup(equal, 31337);
  REQUIRE(leet);
  CHECK(to_string(*leet) == "0000100");
  auto less_than_leet = bmi.lookup(less, 31337);
  REQUIRE(less_than_leet);
  CHECK(to_string(*less_than_leet) == "1111011");
  auto greater_zero = bmi.lookup(greater, 0);
  REQUIRE(greater_zero);
  CHECK(to_string(*greater_zero) == "0111111");

  MESSAGE("serialization");
  std::vector<uint8_t> buf;
  save(buf, bmi);
  decltype(bmi) bmi2;
  load(buf, bmi2);
  CHECK(bmi == bmi2);
}

TEST(floating-point with custom binner)
{
  arithmetic_bitmap_index<null_bitstream, real, precision_binner<6, 2>> bmi;
  CHECK(bmi.push_back(-7.8));
  CHECK(bmi.push_back(42.123));
  CHECK(bmi.push_back(10000.0));
  CHECK(bmi.push_back(4711.13510));
  CHECK(bmi.push_back(31337.3131313));
  CHECK(bmi.push_back(42.12258));
  CHECK(bmi.push_back(42.125799));

  CHECK(to_string(*bmi.lookup(less, 100.0)) == "1100011");
  CHECK(to_string(*bmi.lookup(less, 43.0)) == "1100011");
  CHECK(to_string(*bmi.lookup(greater_equal, 42.0)) == "0111111");
  CHECK(to_string(*bmi.lookup(not_equal, 4711.14)) == "1110111");

  MESSAGE("serialization");
  std::vector<uint8_t> buf;
  save(buf, bmi);
  decltype(bmi) bmi2;
  load(buf, bmi2);
  CHECK(bmi == bmi2);
}

TEST(time duration)
{
  // Default binning gives granularity of seconds.
  arithmetic_bitmap_index<null_bitstream, time::duration> bmi;
  CHECK(bmi.push_back(std::chrono::milliseconds(1000)));
  CHECK(bmi.push_back(std::chrono::milliseconds(2000)));
  CHECK(bmi.push_back(std::chrono::milliseconds(3000)));
  CHECK(bmi.push_back(std::chrono::milliseconds(1011)));
  CHECK(bmi.push_back(std::chrono::milliseconds(2222)));
  CHECK(bmi.push_back(std::chrono::milliseconds(2322)));

  auto hun = bmi.lookup(equal, std::chrono::milliseconds(1034));
  REQUIRE(hun);
  CHECK(to_string(*hun) == "100100");
  auto twokay = bmi.lookup(less_equal, std::chrono::milliseconds(2000));
  REQUIRE(twokay);
  CHECK(to_string(*twokay) == "110111");
  auto twelve = bmi.lookup(greater, std::chrono::milliseconds(1200));
  REQUIRE(twelve);
  CHECK(to_string(*twelve) == "011011");

  MESSAGE("serialization");
  std::vector<uint8_t> buf;
  save(buf, bmi);
  decltype(bmi) bmi2;
  load(buf, bmi2);
  CHECK(bmi == bmi2);
}

TEST(time point)
{
  arithmetic_bitmap_index<null_bitstream, time::point> bmi;

  auto t = to<time::point>("2014-01-16+05:30:15", time::point::format);
  REQUIRE(t);
  CHECK(bmi.push_back(*t));
  t = to<time::point>("2014-01-16+05:30:12", time::point::format);
  REQUIRE(t);
  CHECK(bmi.push_back(*t));
  t = to<time::point>("2014-01-16+05:30:15", time::point::format);
  REQUIRE(t);
  CHECK(bmi.push_back(*t));
  t = to<time::point>("2014-01-16+05:30:18", time::point::format);
  REQUIRE(t);
  CHECK(bmi.push_back(*t));
  t = to<time::point>("2014-01-16+05:30:15", time::point::format);
  REQUIRE(t);
  CHECK(bmi.push_back(*t));
  t = to<time::point>("2014-01-16+05:30:19", time::point::format);
  REQUIRE(t);
  CHECK(bmi.push_back(*t));

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

  MESSAGE("serialization");
  std::vector<uint8_t> buf;
  save(buf, bmi);
  decltype(bmi) bmi2;
  load(buf, bmi2);
  CHECK(bmi == bmi2);
}

TEST(string)
{
  string_bitmap_index<null_bitstream> bmi;
  CHECK(bmi.push_back("foo"));
  CHECK(bmi.push_back("bar"));
  CHECK(bmi.push_back("baz"));
  CHECK(bmi.push_back("foo"));
  CHECK(bmi.push_back("foo"));
  CHECK(bmi.push_back("bar"));
  CHECK(bmi.push_back(""));
  CHECK(bmi.push_back("qux"));
  CHECK(bmi.push_back("corge"));
  CHECK(bmi.push_back("bazz"));

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

  MESSAGE("serialization");
  std::vector<uint8_t> buf;
  save(buf, bmi);
  decltype(bmi) bmi2;
  load(buf, bmi2);
  CHECK(bmi == bmi2);
  CHECK(to_string(*bmi2.lookup(equal, "foo")) == "1001100000");
  CHECK(to_string(*bmi2.lookup(equal, "bar")) == "0100010000");
}

TEST(address)
{
  address_bitmap_index<null_bitstream> bmi;

  CHECK(bmi.push_back(*address::from_v4("192.168.0.1")));
  CHECK(bmi.push_back(*address::from_v4("192.168.0.2")));
  CHECK(bmi.push_back(*address::from_v4("192.168.0.3")));
  CHECK(bmi.push_back(*address::from_v4("192.168.0.1")));
  CHECK(bmi.push_back(*address::from_v4("192.168.0.1")));
  CHECK(bmi.push_back(*address::from_v4("192.168.0.2")));
  CHECK(! bmi.lookup(match, *address::from_v6("::"))); // Invalid operator

  MESSAGE("address equality");
  auto addr = *address::from_v4("192.168.0.1");
  auto bs = bmi.lookup(equal, addr);
  REQUIRE(bs);
  CHECK(to_string(*bs) == "100110");
  bs = bmi.lookup(not_equal, addr);
  CHECK(to_string(*bs) == "011001");
  addr = *address::from_v4("192.168.0.5");
  CHECK(to_string(*bmi.lookup(equal, addr)) == "000000");

  bmi.push_back(*address::from_v4("192.168.0.128"));
  bmi.push_back(*address::from_v4("192.168.0.130"));
  bmi.push_back(*address::from_v4("192.168.0.240"));
  bmi.push_back(*address::from_v4("192.168.0.127"));
  bmi.push_back(*address::from_v4("192.168.0.33"));

  MESSAGE("prefix membership");
  auto sub = subnet{*address::from_v4("192.168.0.128"), 25};
  bs = bmi.lookup(in, sub);
  REQUIRE(bs);
  CHECK(to_string(*bs) == "00000011100");
  bs = bmi.lookup(not_in, sub);
  REQUIRE(bs);
  CHECK(to_string(*bs) == "11111100011");
  sub = {*address::from_v4("192.168.0.0"), 24};
  bs = bmi.lookup(in, sub);
  REQUIRE(bs);
  CHECK(to_string(*bs) == "11111111111");
  sub = {*address::from_v4("192.168.0.0"), 20};
  bs = bmi.lookup(in, sub);
  REQUIRE(bs);
  CHECK(to_string(*bs) == "11111111111");
  sub = {*address::from_v4("192.168.0.64"), 26};
  bs = bmi.lookup(not_in, sub);
  REQUIRE(bs);
  CHECK(to_string(*bs) == "11111111101");

  MESSAGE("serialization");
  std::vector<uint8_t> buf;
  save(buf, bmi);
  decltype(bmi) bmi2;
  load(buf, bmi2);
  CHECK(bmi == bmi2);
}

TEST(subnet)
{
  subnet_bitmap_index<null_bitstream> bmi;

  auto s0 = to<subnet>("192.168.0.0/24");
  auto s1 = to<subnet>("192.168.1.0/24");
  auto s2 = to<subnet>("::/40");
  REQUIRE(s0);
  REQUIRE(s1);
  REQUIRE(s2);

  CHECK(bmi.push_back(*s0));
  CHECK(bmi.push_back(*s1));
  CHECK(bmi.push_back(*s0));
  CHECK(bmi.push_back(*s0));
  CHECK(bmi.push_back(*s2));
  CHECK(bmi.push_back(*s2));

  auto bs = bmi.lookup(equal, *s0);
  REQUIRE(bs);
  CHECK(to_string(*bs) == "101100");

  bs = bmi.lookup(not_equal, *s1);
  REQUIRE(bs);
  CHECK(to_string(*bs) == "101111");

  MESSAGE("serialization");
  std::vector<uint8_t> buf;
  save(buf, bmi);
  decltype(bmi) bmi2;
  load(buf, bmi2);
  CHECK(bmi == bmi2);
}

TEST(port)
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

  MESSAGE("serialization");
  std::vector<uint8_t> buf;
  save(buf, bmi);
  decltype(bmi) bmi2;
  load(buf, bmi2);
  CHECK(bmi == bmi2);
}

TEST(container)
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

  MESSAGE("serialization");
  std::vector<uint8_t> buf;
  save(buf, bmi);
  decltype(bmi) bmi2;
  load(buf, bmi2);
  CHECK(bmi == bmi2);
}

TEST(offset_push_back)
{
  string_bitmap_index<null_bitstream> bmi;
  CHECK(bmi.push_back("foo", 2));
  CHECK(bmi.push_back(data{"bar"}, 3));
  CHECK(bmi.push_back(nil, 5));
  CHECK(bmi.push_back("baz", 7));

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

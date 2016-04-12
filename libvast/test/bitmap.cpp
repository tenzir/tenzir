#include "vast/bitmap.hpp"
#include "vast/bitstream.hpp"
#include "vast/save.hpp"
#include "vast/load.hpp"
#include "vast/concept/serializable/vast/bitmap.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/bitstream.hpp"

#define SUITE bitmap
#include "test.hpp"

using namespace vast;

namespace {

// Prints doubles as IEEE 754 and with our custom offset binary encoding.
std::string dump(uint64_t x) {
  std::string result;
  for (size_t i = 0; i < 64; ++i) {
    if (i == 1 || i == 12)
      result += ' ';
    result += ((x >> (64 - i - 1)) & 1) ? '1' : '0';
  }
  return result;
}

std::string dump(double x) {
  return dump(detail::order(x));
}

} // namespace <anonymous>

TEST(bitwise total ordering (integral)) {
  using detail::order;

  MESSAGE("unsigned identities");
  CHECK(order(0u) == 0);
  CHECK(order(4u) == 4);

  MESSAGE("signed permutation");
  int32_t i = -4;
  CHECK(order(i) == 2147483644);
  i = 4;
  CHECK(order(i) == 2147483652);
}

TEST(bitwise total ordering (floating point)) {
  using detail::order;
  std::string d;
  MESSAGE("permutation");
  CHECK(dump(-0.0) == dump(0.0)); // No signed zero.
  d = "0 11111111111 1111111111111111111111111111111111111111111111111111";
  CHECK(dump(0.0) == d);

  MESSAGE("total ordering");
  CHECK(order(-1111.2) < order(-10.0));
  CHECK(order(-10.0) < order(-2.0));
  CHECK(order(-2.4) < order(-2.2));
  CHECK(order(-1.0) < order(-0.1));
  CHECK(order(-0.001) < order(-0.0));
  CHECK(order(-0.0) == order(0.0)); // no signed zero
  CHECK(order(0.0) < order(0.001));
  CHECK(order(0.001) < order(0.1));
  CHECK(order(0.1) < order(1.0));
  CHECK(order(1.0) < order(2.0));
  CHECK(order(2.0) < order(2.2));
  CHECK(order(2.0) < order(2.4));
  CHECK(order(2.4) < order(10.0));
  CHECK(order(10.0) < order(1111.2));
}

TEST(singleton-coder) {
  singleton_coder<null_bitstream> c;
  REQUIRE(c.encode(true));
  REQUIRE(c.encode(false));
  REQUIRE(c.encode(false));
  REQUIRE(c.encode(true));
  REQUIRE(c.encode(false));
  CHECK(to_string(c.decode(equal, true)) == "10010");
  CHECK(to_string(c.decode(not_equal, false)) == "10010");
  CHECK(to_string(c.decode(not_equal, true)) == "01101");
}

TEST(equality-coder) {
  equality_coder<null_bitstream> c{10};
  CHECK(c.encode(8));
  CHECK(c.encode(9));
  CHECK(c.encode(0));
  CHECK(c.encode(1));
  CHECK(c.encode(4));
  CHECK(to_string(c.decode(less,          0)) == "00000");
  CHECK(to_string(c.decode(less,          4)) == "00110");
  CHECK(to_string(c.decode(less,          9)) == "10111");
  CHECK(to_string(c.decode(less_equal,    0)) == "00100");
  CHECK(to_string(c.decode(less_equal,    4)) == "00111");
  CHECK(to_string(c.decode(less_equal,    9)) == "11111");
  CHECK(to_string(c.decode(equal,         0)) == "00100");
  CHECK(to_string(c.decode(equal,         3)) == "00000");
  CHECK(to_string(c.decode(equal,         9)) == "01000");
  CHECK(to_string(c.decode(not_equal,     0)) == "11011");
  CHECK(to_string(c.decode(not_equal,     3)) == "11111");
  CHECK(to_string(c.decode(not_equal,     9)) == "10111");
  CHECK(to_string(c.decode(greater_equal, 0)) == "11111");
  CHECK(to_string(c.decode(greater_equal, 8)) == "11000");
  CHECK(to_string(c.decode(greater_equal, 9)) == "01000");
  CHECK(to_string(c.decode(greater,       0)) == "11011");
  CHECK(to_string(c.decode(greater,       8)) == "01000");
  CHECK(to_string(c.decode(greater,       9)) == "00000");
}

TEST(range-coder) {
  range_coder<null_bitstream> c{8};
  CHECK(c.encode(4));
  CHECK(c.encode(7));
  CHECK(c.encode(4));
  CHECK(c.encode(3, 5));
  CHECK(c.encode(3));
  CHECK(c.encode(0));
  CHECK(c.encode(1));
  CHECK(to_string(c.decode(less, 4)) == "00011111111");
  CHECK(to_string(c.decode(equal, 3)) == "00011111100");
  CHECK(to_string(c.decode(greater_equal, 3)) == "11111111100");
}

TEST(bitslice-coder) {
  bitslice_coder<null_bitstream> c{6};
  CHECK(c.encode(4));
  CHECK(c.encode(5));
  CHECK(c.encode(2));
  CHECK(c.encode(3));
  CHECK(c.encode(0));
  CHECK(c.encode(1));
  CHECK(to_string(c.decode(equal, 0)) == "000010");
  CHECK(to_string(c.decode(equal, 1)) == "000001");
  CHECK(to_string(c.decode(equal, 2)) == "001000");
  CHECK(to_string(c.decode(equal, 3)) == "000100");
  CHECK(to_string(c.decode(equal, 4)) == "100000");
  CHECK(to_string(c.decode(equal, 5)) == "010000");
  CHECK(to_string(c.decode(in,    0)) == "000000");
  CHECK(to_string(c.decode(in,    1)) == "010101");
  CHECK(to_string(c.decode(in,    2)) == "001100");
  CHECK(to_string(c.decode(in,    3)) == "000100");
  CHECK(to_string(c.decode(in,    4)) == "110000");
  CHECK(to_string(c.decode(in,    5)) == "010000");
}

TEST(bitslice-coder 2) {
  bitslice_coder<null_bitstream> c{8};
  CHECK(c.encode(0));
  CHECK(c.encode(1));
  CHECK(c.encode(3));
  CHECK(c.encode(9));
  CHECK(c.encode(10));
  CHECK(c.encode(77));
  CHECK(c.encode(99));
  CHECK(c.encode(100));
  CHECK(c.encode(128));
  CHECK(to_string(c.decode(equal, 0)) == "100000000");
  CHECK(to_string(c.decode(equal, 1))  == "010000000");
  CHECK(to_string(c.decode(equal, 3))   == "001000000");
  CHECK(to_string(c.decode(equal, 9))    == "000100000");
  CHECK(to_string(c.decode(equal, 10))   == "000010000");
  CHECK(to_string(c.decode(equal, 77))   == "000001000");
  CHECK(to_string(c.decode(equal, 99))   == "000000100");
  CHECK(to_string(c.decode(equal, 100))  == "000000010");
  CHECK(to_string(c.decode(equal, 128))  == "000000001");
  CHECK(to_string(c.decode(less_equal, 0)) == "100000000");
  CHECK(to_string(c.decode(greater, 0)) == "011111111");
  CHECK(to_string(c.decode(less, 1))  == "100000000");
  CHECK(to_string(c.decode(less_equal, 1))  == "110000000");
  CHECK(to_string(c.decode(greater_equal, 3))   == "001111111");
  CHECK(to_string(c.decode(less, 128))  == "111111110");
}

TEST(base) {
  using b4 = base<3, 4, 10, 42>;
  CHECK(!b4::uniform);
  REQUIRE(b4::components == b4::values.size());
  REQUIRE(b4::components == 4);
  CHECK(b4::values[0] == 3);
  CHECK(b4::values[1] == 4);
  CHECK(b4::values[2] == 10);
  CHECK(b4::values[3] == 42);
}

TEST(base uniform) {
  using u = uniform_base<42, 10>;
  auto is42 = [](auto x) { return x == 42; };
  CHECK(std::all_of(u::values.begin(), u::values.end(), is42));
  CHECK(u::uniform);

  CHECK(make_uniform_base<2, int8_t>::components == 8);
  CHECK(make_uniform_base<2, int16_t>::components == 16);
  CHECK(make_uniform_base<2, int32_t>::components == 32);
  CHECK(make_uniform_base<2, int64_t>::components == 64);
  CHECK(make_uniform_base<10, int8_t>::components == 3);
  CHECK(make_uniform_base<10, int16_t>::components == 5);
  CHECK(make_uniform_base<10, int32_t>::components == 10);
  CHECK(make_uniform_base<10, int64_t>::components == 20);
}

TEST(base singleton) {
  using s = make_singleton_base<int8_t>;
  CHECK(s::components == 1);
  CHECK(s::values[0] == 256);
}

TEST(value decomposition) {
  using detail::compose;
  using detail::decompose;

  auto d0 = decompose(259, base<10, 10, 10>::values);
  auto c0 = compose(d0, base<10, 10, 10>::values);
  CHECK(d0[0] == 9);
  CHECK(d0[1] == 5);
  CHECK(d0[2] == 2);
  CHECK(c0 == 259);

  auto d1 = decompose(54, base<13, 13>::values);
  auto c1 = compose(d1, base<13, 13>::values);
  CHECK(d1[0] == 2);
  CHECK(d1[1] == 4);
  CHECK(c1 == 54);

  auto d2 = decompose(42, base<10, 10>::values);
  auto c2 = compose(d2, base<13, 13>::values);
  CHECK(c2 == 54);

  MESSAGE("heterogeneous base");
  auto d3 = decompose(312, base<10, 10, 10>::values);
  auto c3 = compose(d3, base<3, 2, 5>::values);
  CHECK(c3 == 23);

  MESSAGE("out of range");
  auto x = decompose(42, base<42>::values);
  CHECK(x[0] == 0);

  MESSAGE("wrap around");
  x = decompose(43, base<42>::values);
  CHECK(x[0] == 1);
}

TEST(boolean bitmap) {
  bitmap<bool, singleton_coder<null_bitstream>> m;
  m.push_back(true);
  m.push_back(false);
  m.push_back(false);
  m.push_back(true);
  m.push_back(false);

  CHECK(to_string(m.lookup(equal,     true))  == "10010");
  CHECK(to_string(m.lookup(equal,     false)) == "01101");
  CHECK(to_string(m.lookup(not_equal, false)) == "10010");
  CHECK(to_string(m.lookup(not_equal, true))  == "01101");
}

TEST(equality-coded bitmap) {
  using coder_type = 
    multi_level_coder<base<10, 10>, equality_coder<null_bitstream>>;
  bitmap<unsigned, coder_type> m;
  m.push_back(42);
  m.push_back(84);
  m.push_back(42);
  m.push_back(21);
  m.push_back(30);

  CHECK(to_string(m.lookup(equal,     21)) == "00010");
  CHECK(to_string(m.lookup(equal,     30)) == "00001");
  CHECK(to_string(m.lookup(equal,     42)) == "10100");
  CHECK(to_string(m.lookup(equal,     84)) == "01000");
  CHECK(to_string(m.lookup(equal,     13)) == "00000");
  CHECK(to_string(m.lookup(not_equal, 21)) == "11101");
  CHECK(to_string(m.lookup(not_equal, 30)) == "11110");
  CHECK(to_string(m.lookup(not_equal, 42)) == "01011");
  CHECK(to_string(m.lookup(not_equal, 84)) == "10111");
  CHECK(to_string(m.lookup(not_equal, 13)) == "11111");

  // Increase size artificially.
  CHECK(m.stretch(5));
  CHECK(m.size() == 10);
}

TEST(bitmap serialization) {
  using coder = 
    multi_level_coder<uniform_base<2, 8>, equality_coder<null_bitstream>>;
  using bitmap_type = bitmap<int8_t, coder>;
  bitmap_type bm, bm2;
  bm.push_back(52);
  bm.push_back(84);
  bm.push_back(100);
  bm.push_back(-42);
  bm.push_back(-100);

  std::vector<char> buf;
  save(buf, bm);
  load(buf, bm2);
  CHECK(bm == bm2);
}

TEST(range-coded bitmap) {
  using coder_type =
    multi_level_coder<uniform_base<10, 3>, range_coder<null_bitstream>>;
  bitmap<uint8_t, coder_type> m;

  m.push_back(0);
  m.push_back(6);
  m.push_back(9);
  m.push_back(10);
  m.push_back(77);
  m.push_back(99);
  m.push_back(100);
  m.push_back(255);
  m.push_back(254);

  CHECK(to_string(m.lookup(less,          0))     == "000000000");
  CHECK(to_string(m.lookup(less,          8))     == "110000000");
  CHECK(to_string(m.lookup(less,          9))     == "110000000");
  CHECK(to_string(m.lookup(less,          10))    == "111000000");
  CHECK(to_string(m.lookup(less,          100))   == "111111000");
  CHECK(to_string(m.lookup(less,          254))   == "111111100");
  CHECK(to_string(m.lookup(less,          255))   == "111111101");
  CHECK(to_string(m.lookup(less_equal,    0))     == "100000000");
  CHECK(to_string(m.lookup(less_equal,    8))     == "110000000");
  CHECK(to_string(m.lookup(less_equal,    9))     == "111000000");
  CHECK(to_string(m.lookup(less_equal,    10))    == "111100000");
  CHECK(to_string(m.lookup(less_equal,    100))   == "111111100");
  CHECK(to_string(m.lookup(less_equal,    254))   == "111111101");
  CHECK(to_string(m.lookup(less_equal,    255))   == "111111111");
  CHECK(to_string(m.lookup(greater,       0))     == "011111111");
  CHECK(to_string(m.lookup(greater,       8))     == "001111111");
  CHECK(to_string(m.lookup(greater,       9))     == "000111111");
  CHECK(to_string(m.lookup(greater,       10))    == "000011111");
  CHECK(to_string(m.lookup(greater,       100))   == "000000011");
  CHECK(to_string(m.lookup(greater,       254))   == "000000010");
  CHECK(to_string(m.lookup(greater,       255))   == "000000000");
  CHECK(to_string(m.lookup(greater_equal, 0))     == "111111111");
  CHECK(to_string(m.lookup(greater_equal, 8))     == "001111111");
  CHECK(to_string(m.lookup(greater_equal, 9))     == "001111111");
  CHECK(to_string(m.lookup(greater_equal, 10))    == "000111111");
  CHECK(to_string(m.lookup(greater_equal, 100))   == "000000111");
  CHECK(to_string(m.lookup(greater_equal, 254))   == "000000011");
  CHECK(to_string(m.lookup(greater_equal, 255))   == "000000010");
  CHECK(to_string(m.lookup(equal,         0))     == "100000000");
  CHECK(to_string(m.lookup(equal,         6))     == "010000000");
  CHECK(to_string(m.lookup(equal,         8))     == "000000000");
  CHECK(to_string(m.lookup(equal,         9))     == "001000000");
  CHECK(to_string(m.lookup(equal,         10))    == "000100000");
  CHECK(to_string(m.lookup(equal,         77))    == "000010000");
  CHECK(to_string(m.lookup(equal,         100))   == "000000100");
  CHECK(to_string(m.lookup(equal,         254))   == "000000001");
  CHECK(to_string(m.lookup(equal,         255))   == "000000010");
  CHECK(to_string(m.lookup(not_equal,     0))     == "011111111");
  CHECK(to_string(m.lookup(not_equal,     6))     == "101111111");
  CHECK(to_string(m.lookup(not_equal,     8))     == "111111111");
  CHECK(to_string(m.lookup(not_equal,     9))     == "110111111");
  CHECK(to_string(m.lookup(not_equal,     10))    == "111011111");
  CHECK(to_string(m.lookup(not_equal,     100))   == "111111011");
  CHECK(to_string(m.lookup(not_equal,     254))   == "111111110");
  CHECK(to_string(m.lookup(not_equal,     255))   == "111111101");

  m = {};
  for (size_t i = 0; i < 256; ++i)
    m.push_back(i);
  CHECK(m.size() == 256);
  auto str = std::string(256, '0');
  for (size_t i = 0; i < 256; ++i) {
    str[i] = '1';
    CHECK(to_string(m.lookup(less_equal, i)) == str);
  }
}

TEST(range-coded bitmap 2) {
  using coder_type =
    multi_level_coder<uniform_base<2, 8>, range_coder<null_bitstream>>;
  bitmap<int8_t, coder_type> bm;
  bm.push_back(42);
  bm.push_back(84);
  bm.push_back(42);
  bm.push_back(21);
  bm.push_back(30);

  CHECK(to_string(bm.lookup(not_equal, 13)) == "11111");
  CHECK(to_string(bm.lookup(not_equal, 42)) == "01011");
  CHECK(to_string(bm.lookup(equal, 21)) == "00010");
  CHECK(to_string(bm.lookup(equal, 30)) == "00001");
  CHECK(to_string(bm.lookup(equal, 42)) == "10100");
  CHECK(to_string(bm.lookup(equal, 84)) == "01000");
  CHECK(to_string(bm.lookup(less_equal, 21)) == "00010");
  CHECK(to_string(bm.lookup(less_equal, 30)) == "00011");
  CHECK(to_string(bm.lookup(less_equal, 42)) == "10111");
  CHECK(to_string(bm.lookup(less_equal, 84)) == "11111");
  CHECK(to_string(bm.lookup(less_equal, 25)) == "00010");
  CHECK(to_string(bm.lookup(less_equal, 80)) == "10111");
  CHECK(to_string(bm.lookup(not_equal, 30)) == "11110");
  CHECK(to_string(bm.lookup(greater, 42)) == "01000");
  CHECK(to_string(bm.lookup(greater, 13)) == "11111");
  CHECK(to_string(bm.lookup(greater, 84)) == "00000");
  CHECK(to_string(bm.lookup(less, 42)) == "00011");
  CHECK(to_string(bm.lookup(less, 84)) == "10111");
  CHECK(to_string(bm.lookup(greater_equal, 84)) == "01000");
  CHECK(to_string(bm.lookup(greater_equal, -42)) == "11111");
  CHECK(to_string(bm.lookup(greater_equal, 22)) == "11101");
}

TEST(range-coded bitmap 3) {
  using coder_type =
    multi_level_coder<uniform_base<9, 7>, range_coder<null_bitstream>>;
  bitmap<uint16_t, coder_type> bm;
  bm.push_back(80);
  bm.push_back(443);
  bm.push_back(53);
  bm.push_back(8);
  bm.push_back(31337);
  bm.push_back(80);
  bm.push_back(8080);

  null_bitstream all_zeros;
  all_zeros.append(7, false);
  null_bitstream all_ones;
  all_ones.append(7, true);

  null_bitstream greater_eight;
  CHECK(greater_eight.push_back(1));
  CHECK(greater_eight.push_back(1));
  CHECK(greater_eight.push_back(1));
  CHECK(greater_eight.push_back(0));
  CHECK(greater_eight.push_back(1));
  CHECK(greater_eight.push_back(1));
  CHECK(greater_eight.push_back(1));

  null_bitstream greater_eighty;
  CHECK(greater_eighty.push_back(0));
  CHECK(greater_eighty.push_back(1));
  CHECK(greater_eighty.push_back(0));
  CHECK(greater_eighty.push_back(0));
  CHECK(greater_eighty.push_back(1));
  CHECK(greater_eighty.push_back(0));
  CHECK(greater_eighty.push_back(1));

  CHECK(bm.lookup(greater, 1) == all_ones);
  CHECK(bm.lookup(greater, 2) == all_ones);
  CHECK(bm.lookup(greater, 3) == all_ones);
  CHECK(bm.lookup(greater, 4) == all_ones);
  CHECK(bm.lookup(greater, 5) == all_ones);
  CHECK(bm.lookup(greater, 6) == all_ones);
  CHECK(bm.lookup(greater, 7) == all_ones);
  CHECK(bm.lookup(greater, 8) == greater_eight);
  CHECK(bm.lookup(greater, 9) == greater_eight);
  CHECK(bm.lookup(greater, 10) == greater_eight);
  CHECK(bm.lookup(greater, 11) == greater_eight);
  CHECK(bm.lookup(greater, 12) == greater_eight);
  CHECK(bm.lookup(greater, 13) == greater_eight);
  CHECK(bm.lookup(greater, 80) == greater_eighty);
  CHECK(bm.lookup(greater, 80) == greater_eighty);
  CHECK(bm.lookup(greater, 31337) == all_zeros);
  CHECK(bm.lookup(greater, 31338) == all_zeros);
}

TEST(bitslice-coded bitmap) {
  bitmap<int16_t, bitslice_coder<null_bitstream>> bm{8};
  bm.push_back(0);
  bm.push_back(1);
  bm.push_back(1);
  bm.push_back(2);
  bm.push_back(3);
  bm.push_back(2);
  bm.push_back(2);

  CHECK(to_string(bm.lookup(equal, 0))   == "1000000");
  CHECK(to_string(bm.lookup(equal, 1))   == "0110000");
  CHECK(to_string(bm.lookup(equal, 2))   == "0001011");
  CHECK(to_string(bm.lookup(equal, 3))   == "0000100");
  CHECK(to_string(bm.lookup(equal, -42)) == "0000000");
  CHECK(to_string(bm.lookup(equal, 4))   == "0000000");

  CHECK(to_string(bm.lookup(not_equal, -42)) == "1111111");
  CHECK(to_string(bm.lookup(not_equal, 0))   == "0111111");
  CHECK(to_string(bm.lookup(not_equal, 1))   == "1001111");
  CHECK(to_string(bm.lookup(not_equal, 2))   == "1110100");
  CHECK(to_string(bm.lookup(not_equal, 3))   == "1111011");
}

namespace {

template <typename Coder>
auto append_test() {
  using coder_type = multi_level_coder<uniform_base<10, 6>, Coder>;
  bitmap<uint16_t, coder_type> bm1, bm2;
  bm1.push_back(43);
  bm1.push_back(42);
  bm1.push_back(42);
  bm1.push_back(1337);

  bm2.push_back(4711);
  bm2.push_back(123);
  bm2.push_back(1337);
  bm2.push_back(456);

  CHECK(to_string(bm1.lookup(equal, 42)) ==   "0110");
  CHECK(to_string(bm1.lookup(equal, 1337)) == "0001");
  bm1.append(bm2);
  REQUIRE(bm1.size() == 8);
  CHECK(to_string(bm1.lookup(equal, 42)) ==   "01100000");
  CHECK(to_string(bm1.lookup(equal, 123)) ==  "00000100");
  CHECK(to_string(bm1.lookup(equal, 1337)) == "00010010");
  CHECK(to_string(bm1.lookup(equal, 456)) ==  "00000001");

  bm2.append(bm1);
  REQUIRE(bm2.size() == 12);
  CHECK(to_string(bm2.lookup(equal, 42)) ==   "000001100000");
  CHECK(to_string(bm2.lookup(equal, 1337)) == "001000010010");
  CHECK(to_string(bm2.lookup(equal, 456)) ==  "000100000001");

  return bm2;
}

} // namespace <anonymous>

TEST(equality-coder append) {
  append_test<equality_coder<null_bitstream>>();
}

TEST(range-coder append) {
  auto bm = append_test<range_coder<null_bitstream>>();
  CHECK(to_string(bm.lookup(greater_equal, 42)) == "111111111111");
  CHECK(to_string(bm.lookup(less_equal, 10))    == "000000000000");
  CHECK(to_string(bm.lookup(less_equal, 100))   == "000011100000");
  CHECK(to_string(bm.lookup(greater, 1000))     == "101000011010");
}

TEST(bitslice-coder append) {
  append_test<bitslice_coder<null_bitstream>>();
}

TEST(multi_push_back) {
  bitmap<uint8_t, range_coder<null_bitstream>> bm{20};
  bm.push_back(7, 4);
  bm.push_back(3, 6);

  CHECK(bm.size() == 10);
  CHECK(to_string(bm.lookup(less, 10)) == "1111111111");
  CHECK(to_string(bm.lookup(equal, 7)) == "1111000000");
  CHECK(to_string(bm.lookup(equal, 3)) == "0000111111");
}


TEST(precision-binner fractional) {
  using binner = precision_binner<2, 3>;
  using coder_type =
    multi_level_coder<uniform_base<2, 64>, range_coder<null_bitstream>>;
  bitmap<double, coder_type, binner> bm;
  bm.push_back(42.001);
  bm.push_back(42.002);
  bm.push_back(43.0014);
  bm.push_back(43.0013);
  bm.push_back(43.0005);
  bm.push_back(43.0015);
  CHECK(to_string(bm.lookup(equal, 42.001)) == "100000");
  CHECK(to_string(bm.lookup(equal, 42.002)) == "010000");
  CHECK(to_string(bm.lookup(equal, 43.001)) == "001110");
  CHECK(to_string(bm.lookup(equal, 43.002)) == "000001");
}

TEST(decimal binner with integers) {
  using binner = decimal_binner<2>;
  bitmap<uint16_t, equality_coder<null_bitstream>, binner> bm{400};
  bm.push_back(183);
  bm.push_back(215);
  bm.push_back(350);
  bm.push_back(253);
  bm.push_back(101);
  CHECK(to_string(bm.lookup(equal, 100)) == "10001");
  CHECK(to_string(bm.lookup(equal, 200)) == "01010");
  CHECK(to_string(bm.lookup(equal, 300)) == "00100");
}

TEST(decimal binner with floating-point) {
  using binner = decimal_binner<1>;
  using coder_type =
    multi_level_coder<uniform_base<2, 64>, range_coder<null_bitstream>>;
  bitmap<double, coder_type, binner> bm;
  bm.push_back(42.123);
  bm.push_back(53.9);
  bm.push_back(41.02014);
  bm.push_back(44.91234543);
  bm.push_back(39.5);
  bm.push_back(49.5);
  CHECK(to_string(bm.lookup(equal, 40.0)) == "101110");
  CHECK(to_string(bm.lookup(equal, 50.0)) == "010001");
}

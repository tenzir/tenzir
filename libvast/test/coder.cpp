/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#define SUITE coder

#include "test.hpp"
#include "fixtures/actor_system.hpp"

#include "vast/base.hpp"
#include "vast/coder.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/bitmap.hpp"
#include "vast/concept/printable/vast/coder.hpp"
#include "vast/detail/order.hpp"
#include "vast/load.hpp"
#include "vast/null_bitmap.hpp"
#include "vast/save.hpp"

using namespace vast;

#define CHECK_DECODE(op, val, res)                                             \
  CHECK_EQUAL(to_string(c.decode(op, val)), res)

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

template <class Coder, class... Ts>
void fill(Coder& c, Ts... xs) {
  (c.encode(xs), ...);
}

} // namespace <anonymous>

FIXTURE_SCOPE(coder_tests, fixtures::deterministic_actor_system)

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
  singleton_coder<null_bitmap> c;
  fill(c, true, false, false, true, false);
  CHECK_DECODE(equal, true, "10010");
  CHECK_DECODE(not_equal, true, "01101");
  CHECK_DECODE(not_equal, false, "10010");
}

TEST(equality-coder) {
  equality_coder<null_bitmap> c{10};
  fill(c, 8, 9, 0, 1, 4);
  CHECK_DECODE(less, 0, "00000");
  CHECK_DECODE(less, 1, "00100");
  CHECK_DECODE(less, 2, "00110");
  CHECK_DECODE(less, 3, "00110");
  CHECK_DECODE(less, 4, "00110");
  CHECK_DECODE(less, 5, "00111");
  CHECK_DECODE(less, 6, "00111");
  CHECK_DECODE(less, 7, "00111");
  CHECK_DECODE(less, 8, "00111");
  CHECK_DECODE(less, 9, "10111");
  CHECK_DECODE(less_equal, 0, "00100");
  CHECK_DECODE(less_equal, 1, "00110");
  CHECK_DECODE(less_equal, 2, "00110");
  CHECK_DECODE(less_equal, 3, "00110");
  CHECK_DECODE(less_equal, 4, "00111");
  CHECK_DECODE(less_equal, 5, "00111");
  CHECK_DECODE(less_equal, 6, "00111");
  CHECK_DECODE(less_equal, 7, "00111");
  CHECK_DECODE(less_equal, 8, "10111");
  CHECK_DECODE(less_equal, 9, "11111");
  CHECK_DECODE(equal, 0, "00100");
  CHECK_DECODE(equal, 1, "00010");
  CHECK_DECODE(equal, 2, "00000");
  CHECK_DECODE(equal, 3, "00000");
  CHECK_DECODE(equal, 4, "00001");
  CHECK_DECODE(equal, 5, "00000");
  CHECK_DECODE(equal, 6, "00000");
  CHECK_DECODE(equal, 7, "00000");
  CHECK_DECODE(equal, 8, "10000");
  CHECK_DECODE(equal, 9, "01000");
  CHECK_DECODE(not_equal, 0, "11011");
  CHECK_DECODE(not_equal, 1, "11101");
  CHECK_DECODE(not_equal, 2, "11111");
  CHECK_DECODE(not_equal, 3, "11111");
  CHECK_DECODE(not_equal, 4, "11110");
  CHECK_DECODE(not_equal, 5, "11111");
  CHECK_DECODE(not_equal, 6, "11111");
  CHECK_DECODE(not_equal, 7, "11111");
  CHECK_DECODE(not_equal, 8, "01111");
  CHECK_DECODE(not_equal, 9, "10111");
  CHECK_DECODE(greater, 0, "11011");
  CHECK_DECODE(greater, 1, "11001");
  CHECK_DECODE(greater, 2, "11001");
  CHECK_DECODE(greater, 3, "11001");
  CHECK_DECODE(greater, 4, "11000");
  CHECK_DECODE(greater, 5, "11000");
  CHECK_DECODE(greater, 6, "11000");
  CHECK_DECODE(greater, 7, "11000");
  CHECK_DECODE(greater, 8, "01000");
  CHECK_DECODE(greater, 9, "00000");
  CHECK_DECODE(greater_equal, 0, "11111");
  CHECK_DECODE(greater_equal, 1, "11011");
  CHECK_DECODE(greater_equal, 2, "11001");
  CHECK_DECODE(greater_equal, 3, "11001");
  CHECK_DECODE(greater_equal, 4, "11001");
  CHECK_DECODE(greater_equal, 5, "11000");
  CHECK_DECODE(greater_equal, 6, "11000");
  CHECK_DECODE(greater_equal, 7, "11000");
  CHECK_DECODE(greater_equal, 8, "11000");
  CHECK_DECODE(greater_equal, 9, "01000");
}

TEST(range-coder) {
  range_coder<null_bitmap> c{8};
  fill(c, 4, 7, 4, 3, 3, 3, 3, 3, 3, 0, 1);
  CHECK_DECODE(less, 0, "00000000000");
  CHECK_DECODE(less, 1, "00000000010");
  CHECK_DECODE(less, 2, "00000000011");
  CHECK_DECODE(less, 3, "00000000011");
  CHECK_DECODE(less, 4, "00011111111");
  CHECK_DECODE(less, 5, "10111111111");
  CHECK_DECODE(less, 6, "10111111111");
  CHECK_DECODE(less, 7, "10111111111");
  CHECK_DECODE(less_equal, 0, "00000000010");
  CHECK_DECODE(less_equal, 1, "00000000011");
  CHECK_DECODE(less_equal, 2, "00000000011");
  CHECK_DECODE(less_equal, 3, "00011111111");
  CHECK_DECODE(less_equal, 4, "10111111111");
  CHECK_DECODE(less_equal, 5, "10111111111");
  CHECK_DECODE(less_equal, 6, "10111111111");
  CHECK_DECODE(less_equal, 7, "11111111111");
  CHECK_DECODE(equal, 0, "00000000010");
  CHECK_DECODE(equal, 1, "00000000001");
  CHECK_DECODE(equal, 2, "00000000000");
  CHECK_DECODE(equal, 3, "00011111100");
  CHECK_DECODE(equal, 4, "10100000000");
  CHECK_DECODE(equal, 5, "00000000000");
  CHECK_DECODE(equal, 6, "00000000000");
  CHECK_DECODE(equal, 7, "01000000000");
  CHECK_DECODE(not_equal, 0, "11111111101");
  CHECK_DECODE(not_equal, 1, "11111111110");
  CHECK_DECODE(not_equal, 2, "11111111111");
  CHECK_DECODE(not_equal, 3, "11100000011");
  CHECK_DECODE(not_equal, 4, "01011111111");
  CHECK_DECODE(not_equal, 5, "11111111111");
  CHECK_DECODE(not_equal, 6, "11111111111");
  CHECK_DECODE(not_equal, 7, "10111111111");
  CHECK_DECODE(greater, 0, "11111111101");
  CHECK_DECODE(greater, 1, "11111111100");
  CHECK_DECODE(greater, 2, "11111111100");
  CHECK_DECODE(greater, 3, "11100000000");
  CHECK_DECODE(greater, 4, "01000000000");
  CHECK_DECODE(greater, 5, "01000000000");
  CHECK_DECODE(greater, 6, "01000000000");
  CHECK_DECODE(greater, 7, "00000000000");
  CHECK_DECODE(greater_equal, 0, "11111111111");
  CHECK_DECODE(greater_equal, 1, "11111111101");
  CHECK_DECODE(greater_equal, 2, "11111111100");
  CHECK_DECODE(greater_equal, 3, "11111111100");
  CHECK_DECODE(greater_equal, 4, "11100000000");
  CHECK_DECODE(greater_equal, 5, "01000000000");
  CHECK_DECODE(greater_equal, 6, "01000000000");
  CHECK_DECODE(greater_equal, 7, "01000000000");
}

TEST(bitslice-coder) {
  bitslice_coder<null_bitmap> c{6};
  fill(c, 4, 5, 2, 3, 0, 1);
  CHECK_DECODE(equal, 0, "000010");
  CHECK_DECODE(equal, 1, "000001");
  CHECK_DECODE(equal, 2, "001000");
  CHECK_DECODE(equal, 3, "000100");
  CHECK_DECODE(equal, 4, "100000");
  CHECK_DECODE(equal, 5, "010000");
  CHECK_DECODE(in,    0, "000000");
  CHECK_DECODE(in,    1, "010101");
  CHECK_DECODE(in,    2, "001100");
  CHECK_DECODE(in,    3, "000100");
  CHECK_DECODE(in,    4, "110000");
  CHECK_DECODE(in,    5, "010000");
}

TEST(bitslice-coder 2) {
  bitslice_coder<null_bitmap> c{8};
  fill(c, 0, 1, 3, 9, 10, 77, 99, 100, 128);
  CHECK_DECODE(less,   0, "000000000");
  CHECK_DECODE(less,   1, "100000000");
  CHECK_DECODE(less,   2, "110000000");
  CHECK_DECODE(less,   3, "110000000");
  CHECK_DECODE(less,   4, "111000000");
  CHECK_DECODE(less,   9, "111000000");
  CHECK_DECODE(less,  10, "111100000");
  CHECK_DECODE(less,  11, "111110000");
  CHECK_DECODE(less,  76, "111110000");
  CHECK_DECODE(less,  77, "111110000");
  CHECK_DECODE(less,  78, "111111000");
  CHECK_DECODE(less,  98, "111111000");
  CHECK_DECODE(less,  99, "111111000");
  CHECK_DECODE(less, 100, "111111100");
  CHECK_DECODE(less, 101, "111111110");
  CHECK_DECODE(less, 127, "111111110");
  CHECK_DECODE(less, 128, "111111110");
  CHECK_DECODE(less_equal,   0, "100000000");
  CHECK_DECODE(less_equal,   1, "110000000");
  CHECK_DECODE(less_equal,   2, "110000000");
  CHECK_DECODE(less_equal,   3, "111000000");
  CHECK_DECODE(less_equal,   4, "111000000");
  CHECK_DECODE(less_equal,   9, "111100000");
  CHECK_DECODE(less_equal,  10, "111110000");
  CHECK_DECODE(less_equal,  11, "111110000");
  CHECK_DECODE(less_equal,  76, "111110000");
  CHECK_DECODE(less_equal,  77, "111111000");
  CHECK_DECODE(less_equal,  78, "111111000");
  CHECK_DECODE(less_equal,  98, "111111000");
  CHECK_DECODE(less_equal,  99, "111111100");
  CHECK_DECODE(less_equal, 100, "111111110");
  CHECK_DECODE(less_equal, 101, "111111110");
  CHECK_DECODE(less_equal, 127, "111111110");
  CHECK_DECODE(less_equal, 128, "111111111");
  CHECK_DECODE(equal,   0, "100000000");
  CHECK_DECODE(equal,   1, "010000000");
  CHECK_DECODE(equal,   2, "000000000");
  CHECK_DECODE(equal,   3, "001000000");
  CHECK_DECODE(equal,   4, "000000000");
  CHECK_DECODE(equal,   9, "000100000");
  CHECK_DECODE(equal,  10, "000010000");
  CHECK_DECODE(equal,  11, "000000000");
  CHECK_DECODE(equal,  76, "000000000");
  CHECK_DECODE(equal,  77, "000001000");
  CHECK_DECODE(equal,  78, "000000000");
  CHECK_DECODE(equal,  98, "000000000");
  CHECK_DECODE(equal,  99, "000000100");
  CHECK_DECODE(equal, 100, "000000010");
  CHECK_DECODE(equal, 101, "000000000");
  CHECK_DECODE(equal, 127, "000000000");
  CHECK_DECODE(equal, 128, "000000001");
  CHECK_DECODE(not_equal,   0, "011111111");
  CHECK_DECODE(not_equal,   1, "101111111");
  CHECK_DECODE(not_equal,   2, "111111111");
  CHECK_DECODE(not_equal,   3, "110111111");
  CHECK_DECODE(not_equal,   4, "111111111");
  CHECK_DECODE(not_equal,   9, "111011111");
  CHECK_DECODE(not_equal,  10, "111101111");
  CHECK_DECODE(not_equal,  11, "111111111");
  CHECK_DECODE(not_equal,  76, "111111111");
  CHECK_DECODE(not_equal,  77, "111110111");
  CHECK_DECODE(not_equal,  78, "111111111");
  CHECK_DECODE(not_equal,  98, "111111111");
  CHECK_DECODE(not_equal,  99, "111111011");
  CHECK_DECODE(not_equal, 100, "111111101");
  CHECK_DECODE(not_equal, 101, "111111111");
  CHECK_DECODE(not_equal, 127, "111111111");
  CHECK_DECODE(not_equal, 128, "111111110");
  CHECK_DECODE(greater,   0, "011111111");
  CHECK_DECODE(greater,   1, "001111111");
  CHECK_DECODE(greater,   2, "001111111");
  CHECK_DECODE(greater,   3, "000111111");
  CHECK_DECODE(greater,   4, "000111111");
  CHECK_DECODE(greater,   9, "000011111");
  CHECK_DECODE(greater,  10, "000001111");
  CHECK_DECODE(greater,  11, "000001111");
  CHECK_DECODE(greater,  76, "000001111");
  CHECK_DECODE(greater,  77, "000000111");
  CHECK_DECODE(greater,  78, "000000111");
  CHECK_DECODE(greater,  98, "000000111");
  CHECK_DECODE(greater,  99, "000000011");
  CHECK_DECODE(greater, 100, "000000001");
  CHECK_DECODE(greater, 101, "000000001");
  CHECK_DECODE(greater, 127, "000000001");
  CHECK_DECODE(greater, 128, "000000000");
  CHECK_DECODE(greater_equal,   0, "111111111");
  CHECK_DECODE(greater_equal,   1, "011111111");
  CHECK_DECODE(greater_equal,   2, "001111111");
  CHECK_DECODE(greater_equal,   3, "001111111");
  CHECK_DECODE(greater_equal,   4, "000111111");
  CHECK_DECODE(greater_equal,   9, "000111111");
  CHECK_DECODE(greater_equal,  10, "000011111");
  CHECK_DECODE(greater_equal,  11, "000001111");
  CHECK_DECODE(greater_equal,  76, "000001111");
  CHECK_DECODE(greater_equal,  77, "000001111");
  CHECK_DECODE(greater_equal,  78, "000000111");
  CHECK_DECODE(greater_equal,  98, "000000111");
  CHECK_DECODE(greater_equal,  99, "000000111");
  CHECK_DECODE(greater_equal, 100, "000000011");
  CHECK_DECODE(greater_equal, 101, "000000001");
  CHECK_DECODE(greater_equal, 127, "000000001");
  CHECK_DECODE(greater_equal, 128, "000000001");
}

TEST(uniform bases) {
  auto u = base::uniform(42, 10);
  auto is42 = [](auto x) { return x == 42; };
  CHECK(std::all_of(u.begin(), u.end(), is42));
  CHECK_EQUAL(u.size(), 10u);
  CHECK_EQUAL(base::uniform<8>(2).size(), 8u);
  CHECK_EQUAL(base::uniform<16>(2).size(), 16u);
  CHECK_EQUAL(base::uniform<32>(2).size(), 32u);
  CHECK_EQUAL(base::uniform<64>(2).size(), 64u);
  CHECK_EQUAL(base::uniform<8>(10).size(), 3u);
  CHECK_EQUAL(base::uniform<16>(10).size(), 5u);
  CHECK_EQUAL(base::uniform<32>(10).size(), 10u);
  CHECK_EQUAL(base::uniform<64>(10).size(), 20u);
}

TEST(value decomposition) {
  MESSAGE("base <10, 10, 10>");
  auto b1 = base{10, 10, 10};
  auto xs = base::vector_type(3);
  b1.decompose(259, xs);
  CHECK(xs[0] == 9);
  CHECK(xs[1] == 5);
  CHECK(xs[2] == 2);
  auto x = b1.compose<int>(xs);
  CHECK(x == 259);
  MESSAGE("base <13, 13>");
  auto b2 = base{13, 13};
  xs.resize(2);
  b2.decompose(54, xs);
  CHECK(xs[0] == 2);
  CHECK(xs[1] == 4);
  x = b2.compose<int>(xs);
  CHECK(x == 54);
  xs = {2, 4};
  x = b2.compose<int>(xs);
  CHECK(x == 54);
  MESSAGE("heterogeneous base");
  xs.resize(3);
  b1.decompose(312, xs);
  auto b3 = base{3, 2, 5};
  x = b3.compose<int>(xs);
  CHECK_EQUAL(x, 23);
}

TEST(multi-level equality coder) {
  auto c = multi_level_coder<equality_coder<null_bitmap>>{base{10, 10}};
  fill(c, 42, 84, 42, 21, 30);
  CHECK_DECODE(equal, 20, "00000");
  CHECK_DECODE(equal, 21, "00010");
  CHECK_DECODE(equal, 22, "00000");
  CHECK_DECODE(equal, 29, "00000");
  CHECK_DECODE(equal, 30, "00001");
  CHECK_DECODE(equal, 31, "00000");
  CHECK_DECODE(equal, 41, "00000");
  CHECK_DECODE(equal, 42, "10100");
  CHECK_DECODE(equal, 43, "00000");
  CHECK_DECODE(equal, 83, "00000");
  CHECK_DECODE(equal, 84, "01000");
  CHECK_DECODE(equal, 85, "00000");
  CHECK_DECODE(not_equal, 20, "11111");
  CHECK_DECODE(not_equal, 21, "11101");
  CHECK_DECODE(not_equal, 22, "11111");
  CHECK_DECODE(not_equal, 29, "11111");
  CHECK_DECODE(not_equal, 30, "11110");
  CHECK_DECODE(not_equal, 31, "11111");
  CHECK_DECODE(not_equal, 41, "11111");
  CHECK_DECODE(not_equal, 42, "01011");
  CHECK_DECODE(not_equal, 43, "11111");
  CHECK_DECODE(not_equal, 83, "11111");
  CHECK_DECODE(not_equal, 84, "10111");
  CHECK_DECODE(not_equal, 85, "11111");
}

TEST(multi-level range coder) {
  using coder_type = multi_level_coder<range_coder<null_bitmap>>;
  auto c = coder_type{base::uniform(10, 3)};
  fill(c, 0, 6, 9, 10, 77, 99, 100, 255, 254);
  CHECK_DECODE(less, 0, "000000000");
  CHECK_DECODE(less,   8, "110000000");
  CHECK_DECODE(less,   9, "110000000");
  CHECK_DECODE(less,  10, "111000000");
  CHECK_DECODE(less, 100, "111111000");
  CHECK_DECODE(less, 254, "111111100");
  CHECK_DECODE(less, 255, "111111101");
  CHECK_DECODE(less_equal,   0, "100000000");
  CHECK_DECODE(less_equal,   8, "110000000");
  CHECK_DECODE(less_equal,   9, "111000000");
  CHECK_DECODE(less_equal,  10, "111100000");
  CHECK_DECODE(less_equal, 100, "111111100");
  CHECK_DECODE(less_equal, 254, "111111101");
  CHECK_DECODE(less_equal, 255, "111111111");
  CHECK_DECODE(greater,   0, "011111111");
  CHECK_DECODE(greater,   8, "001111111");
  CHECK_DECODE(greater,   9, "000111111");
  CHECK_DECODE(greater,  10, "000011111");
  CHECK_DECODE(greater, 100, "000000011");
  CHECK_DECODE(greater, 254, "000000010");
  CHECK_DECODE(greater, 255, "000000000");
  CHECK_DECODE(greater_equal,   0, "111111111");
  CHECK_DECODE(greater_equal,   8, "001111111");
  CHECK_DECODE(greater_equal,   9, "001111111");
  CHECK_DECODE(greater_equal,  10, "000111111");
  CHECK_DECODE(greater_equal, 100, "000000111");
  CHECK_DECODE(greater_equal, 254, "000000011");
  CHECK_DECODE(greater_equal, 255, "000000010");
  CHECK_DECODE(equal,   0, "100000000");
  CHECK_DECODE(equal,   6, "010000000");
  CHECK_DECODE(equal,   8, "000000000");
  CHECK_DECODE(equal,   9, "001000000");
  CHECK_DECODE(equal,  10, "000100000");
  CHECK_DECODE(equal,  77, "000010000");
  CHECK_DECODE(equal, 100, "000000100");
  CHECK_DECODE(equal, 254, "000000001");
  CHECK_DECODE(equal, 255, "000000010");
  CHECK_DECODE(not_equal,   0, "011111111");
  CHECK_DECODE(not_equal,   6, "101111111");
  CHECK_DECODE(not_equal,   8, "111111111");
  CHECK_DECODE(not_equal,   9, "110111111");
  CHECK_DECODE(not_equal,  10, "111011111");
  CHECK_DECODE(not_equal, 100, "111111011");
  CHECK_DECODE(not_equal, 254, "111111110");
  CHECK_DECODE(not_equal, 255, "111111101");
  c = coder_type{base::uniform(9, 3)};
  for (auto i = 0u; i < 256; ++i)
    c.encode(i);
  CHECK_EQUAL(c.size(), 256u);
  auto str = std::string(256, '0');
  for (auto i = 0u; i < 256; ++i) {
    str[i] = '1';
    CHECK_EQUAL(to_string(c.decode(less_equal, i)), str);
  }
}

TEST(serialization range coder) {
  range_coder<null_bitmap> x{100}, c;
  fill(x, 42, 84, 42, 21, 30);
  std::string buf;
  CHECK_EQUAL(save(sys, buf, x), caf::none);
  CHECK_EQUAL(load(sys, buf, c), caf::none);
  CHECK_EQUAL(x, c);
  CHECK_DECODE(equal,     21, "00010");
  CHECK_DECODE(equal,     30, "00001");
  CHECK_DECODE(equal,     42, "10100");
  CHECK_DECODE(equal,     84, "01000");
  CHECK_DECODE(equal,     13, "00000");
  CHECK_DECODE(not_equal, 21, "11101");
  CHECK_DECODE(not_equal, 30, "11110");
  CHECK_DECODE(not_equal, 42, "01011");
  CHECK_DECODE(not_equal, 84, "10111");
  CHECK_DECODE(not_equal, 13, "11111");
  CHECK_DECODE(greater, 21, "11101");
  CHECK_DECODE(greater, 30, "11100");
  CHECK_DECODE(greater, 42, "01000");
  CHECK_DECODE(greater, 84, "00000");
  CHECK_DECODE(greater, 13, "11111");
}

TEST(serialization multi-level coder) {
  using coder_type = multi_level_coder<equality_coder<null_bitmap>>;
  auto x = coder_type{base{10, 10}};
  fill(x, 42, 84, 42, 21, 30);
  std::string buf;
  CHECK_EQUAL(save(sys, buf, x), caf::none);
  auto c = coder_type{};
  CHECK_EQUAL(load(sys, buf, c), caf::none);
  CHECK_EQUAL(x, c);
  CHECK_DECODE(equal,     21, "00010");
  CHECK_DECODE(equal,     30, "00001");
  CHECK_DECODE(equal,     42, "10100");
  CHECK_DECODE(equal,     84, "01000");
  CHECK_DECODE(equal,     13, "00000");
  CHECK_DECODE(not_equal, 21, "11101");
  CHECK_DECODE(not_equal, 30, "11110");
  CHECK_DECODE(not_equal, 42, "01011");
  CHECK_DECODE(not_equal, 84, "10111");
  CHECK_DECODE(not_equal, 13, "11111");
}

TEST(printable) {
  equality_coder<null_bitmap> c{5};
  fill(c, 1, 2, 1, 0, 4);
  auto expected = "0\t0001\n"
                  "1\t101\n"
                  "2\t01\n"
                  "3\t\n"
                  "4\t00001";
  CHECK_EQUAL(to_string(c), expected);
}

FIXTURE_SCOPE_END()

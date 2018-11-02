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
  c.encode(true);
  c.encode(false);
  c.encode(false);
  c.encode(true);
  c.encode(false);
  CHECK_EQUAL(to_string(c.decode(equal, true)), "10010");
  CHECK_EQUAL(to_string(c.decode(not_equal, true)), "01101");
  CHECK_EQUAL(to_string(c.decode(not_equal, false)), "10010");
  // Skipped entries come out as 1s.
  CHECK(c.skip(3));
  c.encode(true, 5);
  CHECK_EQUAL(to_string(c.decode(equal, true)), "1001000011111");
}

TEST(equality-coder) {
  equality_coder<null_bitmap> c{10};
  c.encode(8);
  c.encode(9);
  c.encode(0);
  c.encode(1);
  c.encode(4);
  CHECK_EQUAL(to_string(c.decode(less,          0)), "00000");
  CHECK_EQUAL(to_string(c.decode(less,          4)), "00110");
  CHECK_EQUAL(to_string(c.decode(less,          9)), "10111");
  CHECK_EQUAL(to_string(c.decode(less_equal,    0)), "00100");
  CHECK_EQUAL(to_string(c.decode(less_equal,    4)), "00111");
  CHECK_EQUAL(to_string(c.decode(less_equal,    9)), "11111");
  CHECK_EQUAL(to_string(c.decode(equal,         0)), "00100");
  CHECK_EQUAL(to_string(c.decode(equal,         3)), "00000");
  CHECK_EQUAL(to_string(c.decode(equal,         9)), "01000");
  CHECK_EQUAL(to_string(c.decode(not_equal,     0)), "11011");
  CHECK_EQUAL(to_string(c.decode(not_equal,     3)), "11111");
  CHECK_EQUAL(to_string(c.decode(not_equal,     9)), "10111");
  CHECK_EQUAL(to_string(c.decode(greater_equal, 0)), "11111");
  CHECK_EQUAL(to_string(c.decode(greater_equal, 8)), "11000");
  CHECK_EQUAL(to_string(c.decode(greater_equal, 9)), "01000");
  CHECK_EQUAL(to_string(c.decode(greater,       0)), "11011");
  CHECK_EQUAL(to_string(c.decode(greater,       8)), "01000");
  CHECK_EQUAL(to_string(c.decode(greater,       9)), "00000");
  // Skipped entries come out as 0s.
  CHECK(c.skip(2));
  c.encode(7, 3);
  CHECK_EQUAL(to_string(c.decode(equal,         7)), "0000000111");
}

#define CHECK_DECODE(op, val, res)                                             \
  CHECK_EQUAL(to_string(c.decode(op, val)), res)

TEST(range-coder) {
  range_coder<null_bitmap> c{8};
  c.encode(4);
  c.encode(7);
  c.encode(4);
  c.encode(3, 5);
  c.encode(3);
  c.encode(0);
  c.encode(1);
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
  c.encode(4);
  c.encode(5);
  c.encode(2);
  c.encode(3);
  c.encode(0);
  c.encode(1);
  CHECK_EQUAL(to_string(c.decode(equal, 0)), "000010");
  CHECK_EQUAL(to_string(c.decode(equal, 1)), "000001");
  CHECK_EQUAL(to_string(c.decode(equal, 2)), "001000");
  CHECK_EQUAL(to_string(c.decode(equal, 3)), "000100");
  CHECK_EQUAL(to_string(c.decode(equal, 4)), "100000");
  CHECK_EQUAL(to_string(c.decode(equal, 5)), "010000");
  CHECK_EQUAL(to_string(c.decode(in,    0)), "000000");
  CHECK_EQUAL(to_string(c.decode(in,    1)), "010101");
  CHECK_EQUAL(to_string(c.decode(in,    2)), "001100");
  CHECK_EQUAL(to_string(c.decode(in,    3)), "000100");
  CHECK_EQUAL(to_string(c.decode(in,    4)), "110000");
  CHECK_EQUAL(to_string(c.decode(in,    5)), "010000");
}

TEST(bitslice-coder 2) {
  bitslice_coder<null_bitmap> c{8};
  c.encode(0);
  c.encode(1);
  c.encode(3);
  c.encode(9);
  c.encode(10);
  c.encode(77);
  c.encode(99);
  c.encode(100);
  c.encode(128);
  CHECK_EQUAL(to_string(c.decode(equal, 0)), "100000000");
  CHECK_EQUAL(to_string(c.decode(equal, 1)), "010000000");
  CHECK_EQUAL(to_string(c.decode(equal, 3)), "001000000");
  CHECK_EQUAL(to_string(c.decode(equal, 9)), "000100000");
  CHECK_EQUAL(to_string(c.decode(equal, 10)), "000010000");
  CHECK_EQUAL(to_string(c.decode(equal, 77)), "000001000");
  CHECK_EQUAL(to_string(c.decode(equal, 99)), "000000100");
  CHECK_EQUAL(to_string(c.decode(equal, 100)), "000000010");
  CHECK_EQUAL(to_string(c.decode(equal, 128)), "000000001");
  CHECK_EQUAL(to_string(c.decode(less_equal, 0)), "100000000");
  CHECK_EQUAL(to_string(c.decode(greater, 0)), "011111111");
  CHECK_EQUAL(to_string(c.decode(less, 1)), "100000000");
  CHECK_EQUAL(to_string(c.decode(less_equal, 1)), "110000000");
  CHECK_EQUAL(to_string(c.decode(greater_equal, 3)), "001111111");
  CHECK_EQUAL(to_string(c.decode(less, 128)), "111111110");
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
  c.encode(42);
  c.encode(84);
  c.encode(42);
  c.encode(21);
  c.encode(30);
  CHECK_EQUAL(to_string(c.decode(equal,     21)), "00010");
  CHECK_EQUAL(to_string(c.decode(equal,     30)), "00001");
  CHECK_EQUAL(to_string(c.decode(equal,     42)), "10100");
  CHECK_EQUAL(to_string(c.decode(equal,     84)), "01000");
  CHECK_EQUAL(to_string(c.decode(equal,     13)), "00000");
  CHECK_EQUAL(to_string(c.decode(not_equal, 21)), "11101");
  CHECK_EQUAL(to_string(c.decode(not_equal, 30)), "11110");
  CHECK_EQUAL(to_string(c.decode(not_equal, 42)), "01011");
  CHECK_EQUAL(to_string(c.decode(not_equal, 84)), "10111");
  CHECK_EQUAL(to_string(c.decode(not_equal, 13)), "11111");
}

TEST(multi-level range coder) {
  using coder_type = multi_level_coder<range_coder<null_bitmap>>;
  auto c = coder_type{base::uniform(10, 3)};
  c.encode(0);
  c.encode(6);
  c.encode(9);
  c.encode(10);
  c.encode(77);
  c.encode(99);
  c.encode(100);
  c.encode(255);
  c.encode(254);
  CHECK_EQUAL(to_string(c.decode(less,          0))  , "000000000");
  CHECK_EQUAL(to_string(c.decode(less,          8))  , "110000000");
  CHECK_EQUAL(to_string(c.decode(less,          9))  , "110000000");
  CHECK_EQUAL(to_string(c.decode(less,          10)) , "111000000");
  CHECK_EQUAL(to_string(c.decode(less,          100)), "111111000");
  CHECK_EQUAL(to_string(c.decode(less,          254)), "111111100");
  CHECK_EQUAL(to_string(c.decode(less,          255)), "111111101");
  CHECK_EQUAL(to_string(c.decode(less_equal,    0))  , "100000000");
  CHECK_EQUAL(to_string(c.decode(less_equal,    8))  , "110000000");
  CHECK_EQUAL(to_string(c.decode(less_equal,    9))  , "111000000");
  CHECK_EQUAL(to_string(c.decode(less_equal,    10)) , "111100000");
  CHECK_EQUAL(to_string(c.decode(less_equal,    100)), "111111100");
  CHECK_EQUAL(to_string(c.decode(less_equal,    254)), "111111101");
  CHECK_EQUAL(to_string(c.decode(less_equal,    255)), "111111111");
  CHECK_EQUAL(to_string(c.decode(greater,       0))  , "011111111");
  CHECK_EQUAL(to_string(c.decode(greater,       8))  , "001111111");
  CHECK_EQUAL(to_string(c.decode(greater,       9))  , "000111111");
  CHECK_EQUAL(to_string(c.decode(greater,       10)) , "000011111");
  CHECK_EQUAL(to_string(c.decode(greater,       100)), "000000011");
  CHECK_EQUAL(to_string(c.decode(greater,       254)), "000000010");
  CHECK_EQUAL(to_string(c.decode(greater,       255)), "000000000");
  CHECK_EQUAL(to_string(c.decode(greater_equal, 0))  , "111111111");
  CHECK_EQUAL(to_string(c.decode(greater_equal, 8))  , "001111111");
  CHECK_EQUAL(to_string(c.decode(greater_equal, 9))  , "001111111");
  CHECK_EQUAL(to_string(c.decode(greater_equal, 10)) , "000111111");
  CHECK_EQUAL(to_string(c.decode(greater_equal, 100)), "000000111");
  CHECK_EQUAL(to_string(c.decode(greater_equal, 254)), "000000011");
  CHECK_EQUAL(to_string(c.decode(greater_equal, 255)), "000000010");
  CHECK_EQUAL(to_string(c.decode(equal,         0))  , "100000000");
  CHECK_EQUAL(to_string(c.decode(equal,         6))  , "010000000");
  CHECK_EQUAL(to_string(c.decode(equal,         8))  , "000000000");
  CHECK_EQUAL(to_string(c.decode(equal,         9))  , "001000000");
  CHECK_EQUAL(to_string(c.decode(equal,         10)) , "000100000");
  CHECK_EQUAL(to_string(c.decode(equal,         77)) , "000010000");
  CHECK_EQUAL(to_string(c.decode(equal,         100)), "000000100");
  CHECK_EQUAL(to_string(c.decode(equal,         254)), "000000001");
  CHECK_EQUAL(to_string(c.decode(equal,         255)), "000000010");
  CHECK_EQUAL(to_string(c.decode(not_equal,     0))  , "011111111");
  CHECK_EQUAL(to_string(c.decode(not_equal,     6))  , "101111111");
  CHECK_EQUAL(to_string(c.decode(not_equal,     8))  , "111111111");
  CHECK_EQUAL(to_string(c.decode(not_equal,     9))  , "110111111");
  CHECK_EQUAL(to_string(c.decode(not_equal,     10)) , "111011111");
  CHECK_EQUAL(to_string(c.decode(not_equal,     100)), "111111011");
  CHECK_EQUAL(to_string(c.decode(not_equal,     254)), "111111110");
  CHECK_EQUAL(to_string(c.decode(not_equal,     255)), "111111101");
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
  range_coder<null_bitmap> x{100}, y;
  x.encode(42);
  x.encode(84);
  x.encode(42);
  x.encode(21);
  x.encode(30);
  std::string buf;
  CHECK_EQUAL(save(sys, buf, x), caf::none);
  CHECK_EQUAL(load(sys, buf, y), caf::none);
  CHECK(x == y);
  CHECK_EQUAL(to_string(y.decode(equal,     21)), "00010");
  CHECK_EQUAL(to_string(y.decode(equal,     30)), "00001");
  CHECK_EQUAL(to_string(y.decode(equal,     42)), "10100");
  CHECK_EQUAL(to_string(y.decode(equal,     84)), "01000");
  CHECK_EQUAL(to_string(y.decode(equal,     13)), "00000");
  CHECK_EQUAL(to_string(y.decode(not_equal, 21)), "11101");
  CHECK_EQUAL(to_string(y.decode(not_equal, 30)), "11110");
  CHECK_EQUAL(to_string(y.decode(not_equal, 42)), "01011");
  CHECK_EQUAL(to_string(y.decode(not_equal, 84)), "10111");
  CHECK_EQUAL(to_string(y.decode(not_equal, 13)), "11111");
  CHECK_EQUAL(to_string(y.decode(greater, 21)), "11101");
  CHECK_EQUAL(to_string(y.decode(greater, 30)), "11100");
  CHECK_EQUAL(to_string(y.decode(greater, 42)), "01000");
  CHECK_EQUAL(to_string(y.decode(greater, 84)), "00000");
  CHECK_EQUAL(to_string(y.decode(greater, 13)), "11111");
}

TEST(serialization multi-level coder) {
  using coder_type = multi_level_coder<equality_coder<null_bitmap>>;
  auto x = coder_type{base{10, 10}};
  x.encode(42);
  x.encode(84);
  x.encode(42);
  x.encode(21);
  x.encode(30);
  std::string buf;
  CHECK_EQUAL(save(sys, buf, x), caf::none);
  auto y = coder_type{};
  CHECK_EQUAL(load(sys, buf, y), caf::none);
  CHECK(x == y);
  CHECK_EQUAL(to_string(y.decode(equal,     21)), "00010");
  CHECK_EQUAL(to_string(y.decode(equal,     30)), "00001");
  CHECK_EQUAL(to_string(y.decode(equal,     42)), "10100");
  CHECK_EQUAL(to_string(y.decode(equal,     84)), "01000");
  CHECK_EQUAL(to_string(y.decode(equal,     13)), "00000");
  CHECK_EQUAL(to_string(y.decode(not_equal, 21)), "11101");
  CHECK_EQUAL(to_string(y.decode(not_equal, 30)), "11110");
  CHECK_EQUAL(to_string(y.decode(not_equal, 42)), "01011");
  CHECK_EQUAL(to_string(y.decode(not_equal, 84)), "10111");
  CHECK_EQUAL(to_string(y.decode(not_equal, 13)), "11111");
}

TEST(printable) {
  equality_coder<null_bitmap> c{5};
  c.encode(1);
  c.encode(2);
  c.encode(1);
  c.encode(0);
  c.encode(4);
  auto expected = "0\t0001\n"
                  "1\t101\n"
                  "2\t01\n"
                  "3\t\n"
                  "4\t00001";
  CHECK_EQUAL(to_string(c), expected);
}

FIXTURE_SCOPE_END()

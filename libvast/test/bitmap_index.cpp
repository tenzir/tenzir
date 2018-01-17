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

#include "vast/bitmap_index.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/bitmap.hpp"
#include "vast/load.hpp"
#include "vast/null_bitmap.hpp"
#include "vast/save.hpp"
#include "vast/time.hpp"

#define SUITE bitmap_index
#include "test.hpp"

using namespace vast;
using namespace std::chrono_literals;

TEST(boolean bitmap index) {
  bitmap_index<bool, singleton_coder<null_bitmap>> bmi;
  bmi.push_back(true);
  bmi.push_back(false);
  bmi.push_back(false);
  bmi.push_back(true);
  bmi.push_back(false);
  CHECK_EQUAL(to_string(bmi.lookup(equal,     true)) , "10010");
  CHECK_EQUAL(to_string(bmi.lookup(equal,     false)), "01101");
  CHECK_EQUAL(to_string(bmi.lookup(not_equal, false)), "10010");
  CHECK_EQUAL(to_string(bmi.lookup(not_equal, true)) , "01101");
}

TEST(appending multiple values) {
  bitmap_index<uint8_t, range_coder<null_bitmap>> bmi{20};
  bmi.append(7, 4);
  bmi.append(3, 6);
  CHECK(bmi.size() == 10);
  CHECK(to_string(bmi.lookup(less, 10)) == "1111111111");
  CHECK(to_string(bmi.lookup(equal, 7)) == "1111000000");
  CHECK(to_string(bmi.lookup(equal, 3)) == "0000111111");
}

TEST(multi-level range-coded bitmap index) {
  using coder_type = multi_level_coder<range_coder<null_bitmap>>;
  auto bmi = bitmap_index<int8_t, coder_type>{base::uniform<8>(2)};
  bmi.push_back(42);
  bmi.push_back(84);
  bmi.push_back(42);
  bmi.push_back(21);
  bmi.push_back(30);
  CHECK_EQUAL(to_string(bmi.lookup(not_equal, 13)), "11111");
  CHECK_EQUAL(to_string(bmi.lookup(not_equal, 42)), "01011");
  CHECK_EQUAL(to_string(bmi.lookup(equal, 21)), "00010");
  CHECK_EQUAL(to_string(bmi.lookup(equal, 30)), "00001");
  CHECK_EQUAL(to_string(bmi.lookup(equal, 42)), "10100");
  CHECK_EQUAL(to_string(bmi.lookup(equal, 84)), "01000");
  CHECK_EQUAL(to_string(bmi.lookup(less_equal, 21)), "00010");
  CHECK_EQUAL(to_string(bmi.lookup(less_equal, 30)), "00011");
  CHECK_EQUAL(to_string(bmi.lookup(less_equal, 42)), "10111");
  CHECK_EQUAL(to_string(bmi.lookup(less_equal, 84)), "11111");
  CHECK_EQUAL(to_string(bmi.lookup(less_equal, 25)), "00010");
  CHECK_EQUAL(to_string(bmi.lookup(less_equal, 80)), "10111");
  CHECK_EQUAL(to_string(bmi.lookup(not_equal, 30)), "11110");
  CHECK_EQUAL(to_string(bmi.lookup(greater, 42)), "01000");
  CHECK_EQUAL(to_string(bmi.lookup(greater, 13)), "11111");
  CHECK_EQUAL(to_string(bmi.lookup(greater, 84)), "00000");
  CHECK_EQUAL(to_string(bmi.lookup(less, 42)), "00011");
  CHECK_EQUAL(to_string(bmi.lookup(less, 84)), "10111");
  CHECK_EQUAL(to_string(bmi.lookup(greater_equal, 84)), "01000");
  CHECK_EQUAL(to_string(bmi.lookup(greater_equal, -42)), "11111");
  CHECK_EQUAL(to_string(bmi.lookup(greater_equal, 22)), "11101");
}

TEST(multi-level range-coded bitmap index 2) {
  using coder_type = multi_level_coder<range_coder<null_bitmap>>;
  auto bmi = bitmap_index<uint16_t, coder_type>{base::uniform(9, 7)};
  bmi.push_back(80);
  bmi.push_back(443);
  bmi.push_back(53);
  bmi.push_back(8);
  bmi.push_back(31337);
  bmi.push_back(80);
  bmi.push_back(8080);
  // Results
  null_bitmap all_zeros;
  all_zeros.append_bits(false, 7);
  null_bitmap all_ones;
  all_ones.append_bits(true, 7);
  // > 8
  null_bitmap greater_eight;
  greater_eight.append_bit(1);
  greater_eight.append_bit(1);
  greater_eight.append_bit(1);
  greater_eight.append_bit(0);
  greater_eight.append_bit(1);
  greater_eight.append_bit(1);
  greater_eight.append_bit(1);
  // > 80
  null_bitmap greater_eighty;
  greater_eighty.append_bit(0);
  greater_eighty.append_bit(1);
  greater_eighty.append_bit(0);
  greater_eighty.append_bit(0);
  greater_eighty.append_bit(1);
  greater_eighty.append_bit(0);
  greater_eighty.append_bit(1);
  CHECK(bmi.lookup(greater, 1) == all_ones);
  CHECK(bmi.lookup(greater, 2) == all_ones);
  CHECK(bmi.lookup(greater, 3) == all_ones);
  CHECK(bmi.lookup(greater, 4) == all_ones);
  CHECK(bmi.lookup(greater, 5) == all_ones);
  CHECK(bmi.lookup(greater, 6) == all_ones);
  CHECK(bmi.lookup(greater, 7) == all_ones);
  CHECK(bmi.lookup(greater, 8) == greater_eight);
  CHECK(bmi.lookup(greater, 9) == greater_eight);
  CHECK(bmi.lookup(greater, 10) == greater_eight);
  CHECK(bmi.lookup(greater, 11) == greater_eight);
  CHECK(bmi.lookup(greater, 12) == greater_eight);
  CHECK(bmi.lookup(greater, 13) == greater_eight);
  CHECK(bmi.lookup(greater, 80) == greater_eighty);
  CHECK(bmi.lookup(greater, 80) == greater_eighty);
  CHECK(bmi.lookup(greater, 31337) == all_zeros);
  CHECK(bmi.lookup(greater, 31338) == all_zeros);
}

TEST(bitslice-coded bitmap index) {
  bitmap_index<int16_t, bitslice_coder<null_bitmap>> bmi{8};
  bmi.push_back(0);
  bmi.push_back(1);
  bmi.push_back(1);
  bmi.push_back(2);
  bmi.push_back(3);
  bmi.push_back(2);
  bmi.push_back(2);
  CHECK_EQUAL(to_string(bmi.lookup(equal, 0)), "1000000");
  CHECK_EQUAL(to_string(bmi.lookup(equal, 1)), "0110000");
  CHECK_EQUAL(to_string(bmi.lookup(equal, 2)), "0001011");
  CHECK_EQUAL(to_string(bmi.lookup(equal, 3)), "0000100");
  CHECK_EQUAL(to_string(bmi.lookup(equal, -42)), "0000000");
  CHECK_EQUAL(to_string(bmi.lookup(equal, 4)), "0000000");
  CHECK_EQUAL(to_string(bmi.lookup(not_equal, -42)), "1111111");
  CHECK_EQUAL(to_string(bmi.lookup(not_equal, 0)), "0111111");
  CHECK_EQUAL(to_string(bmi.lookup(not_equal, 1)), "1001111");
  CHECK_EQUAL(to_string(bmi.lookup(not_equal, 2)), "1110100");
  CHECK_EQUAL(to_string(bmi.lookup(not_equal, 3)), "1111011");
}

namespace {

template <class Coder>
auto append_test() {
  using coder_type = multi_level_coder<Coder>;
  auto b = base::uniform(10, 6);
  auto bmi1 = bitmap_index<uint16_t, coder_type>{b};
  auto bmi2 = bitmap_index<uint16_t, coder_type>{b};
  // Fist
  bmi1.push_back(43);
  bmi1.push_back(42);
  bmi1.push_back(42);
  bmi1.push_back(1337);
  // Second
  bmi2.push_back(4711);
  bmi2.push_back(123);
  bmi2.push_back(1337);
  bmi2.push_back(456);
  CHECK(to_string(bmi1.lookup(equal, 42)) ==   "0110");
  CHECK(to_string(bmi1.lookup(equal, 1337)) == "0001");
  // bmi1 += bmi2
  bmi1.append(bmi2);
  REQUIRE(bmi1.size() == 8);
  CHECK(to_string(bmi1.lookup(equal, 42)) ==   "01100000");
  CHECK(to_string(bmi1.lookup(equal, 123)) ==  "00000100");
  CHECK(to_string(bmi1.lookup(equal, 1337)) == "00010010");
  CHECK(to_string(bmi1.lookup(equal, 456)) ==  "00000001");
  // bmi2 += bmi1
  bmi2.append(bmi1);
  REQUIRE(bmi2.size() == 12);
  CHECK(to_string(bmi2.lookup(equal, 42)) ==   "000001100000");
  CHECK(to_string(bmi2.lookup(equal, 1337)) == "001000010010");
  CHECK(to_string(bmi2.lookup(equal, 456)) ==  "000100000001");
  return bmi2;
}

} // namespace <anonymous>

TEST(equality-coder append) {
  append_test<equality_coder<null_bitmap>>();
}

TEST(range-coder append) {
  auto bmi = append_test<range_coder<null_bitmap>>();
  CHECK(to_string(bmi.lookup(greater_equal, 42)) == "111111111111");
  CHECK(to_string(bmi.lookup(less_equal, 10))    == "000000000000");
  CHECK(to_string(bmi.lookup(less_equal, 100))   == "000011100000");
  CHECK(to_string(bmi.lookup(greater, 1000))     == "101000011010");
}

TEST(bitslice-coder append) {
  append_test<bitslice_coder<null_bitmap>>();
}

TEST(fractional precision-binner) {
  using binner = precision_binner<2, 3>;
  using coder_type = multi_level_coder<range_coder<null_bitmap>>;
  auto bmi = bitmap_index<double, coder_type, binner>{base::uniform<64>(2)};
  bmi.push_back(42.001);
  bmi.push_back(42.002);
  bmi.push_back(43.0014);
  bmi.push_back(43.0013);
  bmi.push_back(43.0005);
  bmi.push_back(43.0015);
  CHECK(to_string(bmi.lookup(equal, 42.001)) == "100000");
  CHECK(to_string(bmi.lookup(equal, 42.002)) == "010000");
  CHECK(to_string(bmi.lookup(equal, 43.001)) == "001110");
  CHECK(to_string(bmi.lookup(equal, 43.002)) == "000001");
}

TEST(decimal binner with integers) {
  using binner = decimal_binner<2>;
  bitmap_index<uint16_t, equality_coder<null_bitmap>, binner> bmi{400};
  bmi.push_back(183);
  bmi.push_back(215);
  bmi.push_back(350);
  bmi.push_back(253);
  bmi.push_back(101);
  CHECK(to_string(bmi.lookup(equal, 100)) == "10001");
  CHECK(to_string(bmi.lookup(equal, 200)) == "01010");
  CHECK(to_string(bmi.lookup(equal, 300)) == "00100");
}

TEST(decimal binner with time) {
  using namespace std::chrono;
  using binner = decimal_binner<3>; // ns -> us
  CHECK_EQUAL(binner::bucket_size, 1000u);
  using coder = multi_level_coder<range_coder<null_bitmap>>;
  auto bmi = bitmap_index<int64_t, coder, binner>{base::uniform<64>(10)};
  bmi.push_back((10100ns).count());
  bmi.push_back((10110ns).count());
  bmi.push_back((10111ns).count());
  bmi.push_back((10999ns).count());
  bmi.push_back((11000ns).count());
  bmi.push_back((100000ns).count());
  CHECK_EQUAL(to_string(bmi.lookup(greater, (100000ns).count())), "000000");
  CHECK_EQUAL(to_string(bmi.lookup(greater, (10998ns).count())), "000011");
  CHECK_EQUAL(to_string(bmi.lookup(greater, (11000ns).count())), "000001");
  CHECK_EQUAL(to_string(bmi.lookup(greater, (10000ns).count())), "000011");
  CHECK_EQUAL(to_string(bmi.lookup(less, (10999ns).count())), "000000");
  CHECK_EQUAL(to_string(bmi.lookup(less, (11000ns).count())), "111100");
}

TEST(decimal binner with floating-point) {
  using binner = decimal_binner<1>;
  using coder_type = multi_level_coder<range_coder<null_bitmap>>;
  auto bmi = bitmap_index<double, coder_type, binner>{base::uniform<64>(2)};
  bmi.push_back(42.123);
  bmi.push_back(53.9);
  bmi.push_back(41.02014);
  bmi.push_back(44.91234543);
  bmi.push_back(39.5);
  bmi.push_back(49.5);
  CHECK(to_string(bmi.lookup(equal, 40.0)) == "101110");
  CHECK(to_string(bmi.lookup(equal, 50.0)) == "010001");
}

TEST(serialization) {
  using coder = multi_level_coder<equality_coder<null_bitmap>>;
  using bitmap_index_type = bitmap_index<int8_t, coder>;
  auto bmi1 = bitmap_index_type{base::uniform<8>(2)};
  bmi1.push_back(52);
  bmi1.push_back(84);
  bmi1.push_back(100);
  bmi1.push_back(-42);
  bmi1.push_back(-100);
  CHECK_EQUAL(to_string(bmi1.lookup(not_equal, 100)), "11011");
  std::vector<char> buf;
  save(buf, bmi1);
  auto bmi2 = bitmap_index_type{};
  load(buf, bmi2);
  CHECK(bmi1 == bmi2);
  CHECK_EQUAL(to_string(bmi2.lookup(not_equal, 100)), "11011");
}

//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE bitmap_index

#include "vast/bitmap_index.hpp"

#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/bitmap.hpp"
#include "vast/detail/legacy_deserialize.hpp"
#include "vast/detail/serialize.hpp"
#include "vast/flatbuffer.hpp"
#include "vast/null_bitmap.hpp"
#include "vast/test/test.hpp"
#include "vast/time.hpp"

#include <caf/test/dsl.hpp>

using namespace vast;
using namespace std::chrono_literals;

TEST(bool bitmap index) {
  bitmap_index<bool, singleton_coder<null_bitmap>> bmi;
  bmi.append(true);
  bmi.append(false);
  bmi.append(false);
  bmi.append(true);
  bmi.append(false);
  CHECK_EQUAL(to_string(bmi.lookup(relational_operator::equal, true)), "10010");
  CHECK_EQUAL(to_string(bmi.lookup(relational_operator::equal, false)), "0110"
                                                                        "1");
  CHECK_EQUAL(to_string(bmi.lookup(relational_operator::not_equal, false)),
              "10010");
  CHECK_EQUAL(to_string(bmi.lookup(relational_operator::not_equal, true)),
              "01101");
  auto builder = flatbuffers::FlatBufferBuilder{};
  builder.Finish(pack(builder, bmi));
  auto fb = unbox(flatbuffer<fbs::BitmapIndex>::make(builder.Release()));
  REQUIRE(fb);
  bitmap_index<bool, singleton_coder<null_bitmap>> bmi2;
  REQUIRE_EQUAL(unpack(*fb, bmi2), caf::none);
  CHECK_EQUAL(bmi, bmi2);
}

TEST(appending multiple values) {
  bitmap_index<uint8_t, range_coder<null_bitmap>> bmi{20};
  bmi.append(7, 4);
  bmi.append(3, 6);
  CHECK(bmi.size() == 10);
  CHECK(to_string(bmi.lookup(relational_operator::less, 10)) == "1111111111");
  CHECK(to_string(bmi.lookup(relational_operator::equal, 7)) == "1111000000");
  CHECK(to_string(bmi.lookup(relational_operator::equal, 3)) == "0000111111");
  auto builder = flatbuffers::FlatBufferBuilder{};
  builder.Finish(pack(builder, bmi));
  auto fb = unbox(flatbuffer<fbs::BitmapIndex>::make(builder.Release()));
  REQUIRE(fb);
  bitmap_index<uint8_t, range_coder<null_bitmap>> bmi2{};
  REQUIRE_EQUAL(unpack(*fb, bmi2), caf::none);
  CHECK_EQUAL(bmi, bmi2);
}

TEST(multi - level range - coded bitmap index) {
  using coder_type = multi_level_coder<range_coder<null_bitmap>>;
  auto bmi = bitmap_index<int8_t, coder_type>{base::uniform<8>(2)};
  bmi.append(42);
  bmi.append(84);
  bmi.append(42);
  bmi.append(21);
  bmi.append(30);
  CHECK_EQUAL(to_string(bmi.lookup(relational_operator::not_equal, 13)), "1111"
                                                                         "1");
  CHECK_EQUAL(to_string(bmi.lookup(relational_operator::not_equal, 42)), "0101"
                                                                         "1");
  CHECK_EQUAL(to_string(bmi.lookup(relational_operator::equal, 21)), "00010");
  CHECK_EQUAL(to_string(bmi.lookup(relational_operator::equal, 30)), "00001");
  CHECK_EQUAL(to_string(bmi.lookup(relational_operator::equal, 42)), "10100");
  CHECK_EQUAL(to_string(bmi.lookup(relational_operator::equal, 84)), "01000");
  CHECK_EQUAL(to_string(bmi.lookup(relational_operator::less_equal, 21)), "0001"
                                                                          "0");
  CHECK_EQUAL(to_string(bmi.lookup(relational_operator::less_equal, 30)), "0001"
                                                                          "1");
  CHECK_EQUAL(to_string(bmi.lookup(relational_operator::less_equal, 42)), "1011"
                                                                          "1");
  CHECK_EQUAL(to_string(bmi.lookup(relational_operator::less_equal, 84)), "1111"
                                                                          "1");
  CHECK_EQUAL(to_string(bmi.lookup(relational_operator::less_equal, 25)), "0001"
                                                                          "0");
  CHECK_EQUAL(to_string(bmi.lookup(relational_operator::less_equal, 80)), "1011"
                                                                          "1");
  CHECK_EQUAL(to_string(bmi.lookup(relational_operator::not_equal, 30)), "1111"
                                                                         "0");
  CHECK_EQUAL(to_string(bmi.lookup(relational_operator::greater, 42)), "01000");
  CHECK_EQUAL(to_string(bmi.lookup(relational_operator::greater, 13)), "11111");
  CHECK_EQUAL(to_string(bmi.lookup(relational_operator::greater, 84)), "00000");
  CHECK_EQUAL(to_string(bmi.lookup(relational_operator::less, 42)), "00011");
  CHECK_EQUAL(to_string(bmi.lookup(relational_operator::less, 84)), "10111");
  CHECK_EQUAL(to_string(bmi.lookup(relational_operator::greater_equal, 84)),
              "01000");
  CHECK_EQUAL(to_string(bmi.lookup(relational_operator::greater_equal, -42)),
              "11111");
  CHECK_EQUAL(to_string(bmi.lookup(relational_operator::greater_equal, 22)),
              "11101");
  auto builder = flatbuffers::FlatBufferBuilder{};
  builder.Finish(pack(builder, bmi));
  auto fb = unbox(flatbuffer<fbs::BitmapIndex>::make(builder.Release()));
  REQUIRE(fb);
  auto bmi2 = bitmap_index<int8_t, coder_type>{};
  REQUIRE_EQUAL(unpack(*fb, bmi2), caf::none);
  CHECK_EQUAL(bmi, bmi2);
}

TEST(multi - level range - coded bitmap index 2) {
  using coder_type = multi_level_coder<range_coder<null_bitmap>>;
  auto bmi = bitmap_index<uint16_t, coder_type>{base::uniform(9, 7)};
  bmi.append(80);
  bmi.append(443);
  bmi.append(53);
  bmi.append(8);
  bmi.append(31337);
  bmi.append(80);
  bmi.append(8080);
  // Results
  null_bitmap all_zeros;
  all_zeros.append_bits(false, 7);
  null_bitmap all_ones;
  all_ones.append_bits(true, 7);
  // > 8
  null_bitmap greater_eight;
  greater_eight.append_bit(true);
  greater_eight.append_bit(true);
  greater_eight.append_bit(true);
  greater_eight.append_bit(false);
  greater_eight.append_bit(true);
  greater_eight.append_bit(true);
  greater_eight.append_bit(true);
  // > 80
  null_bitmap greater_eighty;
  greater_eighty.append_bit(false);
  greater_eighty.append_bit(true);
  greater_eighty.append_bit(false);
  greater_eighty.append_bit(false);
  greater_eighty.append_bit(true);
  greater_eighty.append_bit(false);
  greater_eighty.append_bit(true);
  CHECK(bmi.lookup(relational_operator::greater, 1) == all_ones);
  CHECK(bmi.lookup(relational_operator::greater, 2) == all_ones);
  CHECK(bmi.lookup(relational_operator::greater, 3) == all_ones);
  CHECK(bmi.lookup(relational_operator::greater, 4) == all_ones);
  CHECK(bmi.lookup(relational_operator::greater, 5) == all_ones);
  CHECK(bmi.lookup(relational_operator::greater, 6) == all_ones);
  CHECK(bmi.lookup(relational_operator::greater, 7) == all_ones);
  CHECK(bmi.lookup(relational_operator::greater, 8) == greater_eight);
  CHECK(bmi.lookup(relational_operator::greater, 9) == greater_eight);
  CHECK(bmi.lookup(relational_operator::greater, 10) == greater_eight);
  CHECK(bmi.lookup(relational_operator::greater, 11) == greater_eight);
  CHECK(bmi.lookup(relational_operator::greater, 12) == greater_eight);
  CHECK(bmi.lookup(relational_operator::greater, 13) == greater_eight);
  CHECK(bmi.lookup(relational_operator::greater, 80) == greater_eighty);
  CHECK(bmi.lookup(relational_operator::greater, 80) == greater_eighty);
  CHECK(bmi.lookup(relational_operator::greater, 31337) == all_zeros);
  CHECK(bmi.lookup(relational_operator::greater, 31338) == all_zeros);
  auto builder = flatbuffers::FlatBufferBuilder{};
  builder.Finish(pack(builder, bmi));
  auto fb = unbox(flatbuffer<fbs::BitmapIndex>::make(builder.Release()));
  REQUIRE(fb);
  auto bmi2 = bitmap_index<uint16_t, coder_type>{};
  REQUIRE_EQUAL(unpack(*fb, bmi2), caf::none);
  CHECK_EQUAL(bmi, bmi2);
}

TEST(bitslice - coded bitmap index) {
  bitmap_index<int16_t, bitslice_coder<null_bitmap>> bmi{8};
  bmi.append(0);
  bmi.append(1);
  bmi.append(1);
  bmi.append(2);
  bmi.append(3);
  bmi.append(2);
  bmi.append(2);
  CHECK_EQUAL(to_string(bmi.lookup(relational_operator::equal, 0)), "1000000");
  CHECK_EQUAL(to_string(bmi.lookup(relational_operator::equal, 1)), "0110000");
  CHECK_EQUAL(to_string(bmi.lookup(relational_operator::equal, 2)), "0001011");
  CHECK_EQUAL(to_string(bmi.lookup(relational_operator::equal, 3)), "0000100");
  CHECK_EQUAL(to_string(bmi.lookup(relational_operator::equal, -42)), "000000"
                                                                      "0");
  CHECK_EQUAL(to_string(bmi.lookup(relational_operator::equal, 4)), "0000000");
  CHECK_EQUAL(to_string(bmi.lookup(relational_operator::not_equal, -42)),
              "1111111");
  CHECK_EQUAL(to_string(bmi.lookup(relational_operator::not_equal, 0)), "011111"
                                                                        "1");
  CHECK_EQUAL(to_string(bmi.lookup(relational_operator::not_equal, 1)), "100111"
                                                                        "1");
  CHECK_EQUAL(to_string(bmi.lookup(relational_operator::not_equal, 2)), "111010"
                                                                        "0");
  CHECK_EQUAL(to_string(bmi.lookup(relational_operator::not_equal, 3)), "111101"
                                                                        "1");
  auto builder = flatbuffers::FlatBufferBuilder{};
  builder.Finish(pack(builder, bmi));
  auto fb = unbox(flatbuffer<fbs::BitmapIndex>::make(builder.Release()));
  REQUIRE(fb);
  bitmap_index<int16_t, bitslice_coder<null_bitmap>> bmi2{};
  REQUIRE_EQUAL(unpack(*fb, bmi2), caf::none);
  CHECK_EQUAL(bmi, bmi2);
}

namespace {

template <class Coder>
auto append_test() {
  using coder_type = multi_level_coder<Coder>;
  auto b = base::uniform(10, 6);
  auto bmi1 = bitmap_index<uint16_t, coder_type>{b};
  auto bmi2 = bitmap_index<uint16_t, coder_type>{b};
  // Fist
  bmi1.append(43);
  bmi1.append(42);
  bmi1.append(42);
  bmi1.append(1337);
  // Second
  bmi2.append(4711);
  bmi2.append(123);
  bmi2.append(1337);
  bmi2.append(456);
  CHECK(to_string(bmi1.lookup(relational_operator::equal, 42)) == "0110");
  CHECK(to_string(bmi1.lookup(relational_operator::equal, 1337)) == "0001");
  // bmi1 += bmi2
  bmi1.append(bmi2);
  REQUIRE(bmi1.size() == 8);
  CHECK(to_string(bmi1.lookup(relational_operator::equal, 42)) == "01100000");
  CHECK(to_string(bmi1.lookup(relational_operator::equal, 123)) == "00000100");
  CHECK(to_string(bmi1.lookup(relational_operator::equal, 1337)) == "00010010");
  CHECK(to_string(bmi1.lookup(relational_operator::equal, 456)) == "00000001");
  // bmi2 += bmi1
  bmi2.append(bmi1);
  REQUIRE(bmi2.size() == 12);
  CHECK(to_string(bmi2.lookup(relational_operator::equal, 42))
        == "000001100000");
  CHECK(to_string(bmi2.lookup(relational_operator::equal, 1337))
        == "001000010010");
  CHECK(to_string(bmi2.lookup(relational_operator::equal, 456))
        == "000100000001");
  return bmi2;
}

} // namespace

TEST(equality - coder append) {
  append_test<equality_coder<null_bitmap>>();
}

TEST(range - coder append) {
  auto bmi = append_test<range_coder<null_bitmap>>();
  CHECK(to_string(bmi.lookup(relational_operator::greater_equal, 42))
        == "111111111111");
  CHECK(to_string(bmi.lookup(relational_operator::less_equal, 10))
        == "000000000000");
  CHECK(to_string(bmi.lookup(relational_operator::less_equal, 100))
        == "000011100000");
  CHECK(to_string(bmi.lookup(relational_operator::greater, 1000))
        == "101000011010");
}

TEST(bitslice - coder append) {
  append_test<bitslice_coder<null_bitmap>>();
}

TEST(fractional precision - binner) {
  using binner = precision_binner<2, 3>;
  using coder_type = multi_level_coder<range_coder<null_bitmap>>;
  auto bmi = bitmap_index<double, coder_type, binner>{base::uniform<64>(2)};
  bmi.append(42.001);
  bmi.append(42.002);
  bmi.append(43.0014);
  bmi.append(43.0013);
  bmi.append(43.0005);
  bmi.append(43.0015);
  CHECK(to_string(bmi.lookup(relational_operator::equal, 42.001)) == "100000");
  CHECK(to_string(bmi.lookup(relational_operator::equal, 42.002)) == "010000");
  CHECK(to_string(bmi.lookup(relational_operator::equal, 43.001)) == "001110");
  CHECK(to_string(bmi.lookup(relational_operator::equal, 43.002)) == "000001");
}

TEST(decimal binner with integers) {
  using binner = decimal_binner<2>;
  bitmap_index<uint16_t, equality_coder<null_bitmap>, binner> bmi{400};
  bmi.append(183);
  bmi.append(215);
  bmi.append(350);
  bmi.append(253);
  bmi.append(101);
  CHECK(to_string(bmi.lookup(relational_operator::equal, 100)) == "10001");
  CHECK(to_string(bmi.lookup(relational_operator::equal, 200)) == "01010");
  CHECK(to_string(bmi.lookup(relational_operator::equal, 300)) == "00100");
}

TEST(decimal binner with time) {
  using namespace std::chrono;
  using binner = decimal_binner<3>; // ns -> us
  CHECK_EQUAL(binner::bucket_size, 1000u);
  using coder = multi_level_coder<range_coder<null_bitmap>>;
  auto bmi = bitmap_index<int64_t, coder, binner>{base::uniform<64>(10)};
  bmi.append((10100ns).count());
  bmi.append((10110ns).count());
  bmi.append((10111ns).count());
  bmi.append((10999ns).count());
  bmi.append((11000ns).count());
  bmi.append((100000ns).count());
  CHECK_EQUAL(
    to_string(bmi.lookup(relational_operator::greater, (100000ns).count())),
    "000001");
  CHECK_EQUAL(
    to_string(bmi.lookup(relational_operator::greater, (10998ns).count())),
    "111111");
  CHECK_EQUAL(
    to_string(bmi.lookup(relational_operator::greater, (11000ns).count())),
    "000011");
  CHECK_EQUAL(
    to_string(bmi.lookup(relational_operator::greater, (10000ns).count())),
    "111111");
  CHECK_EQUAL(
    to_string(bmi.lookup(relational_operator::less, (10999ns).count())), "11110"
                                                                         "0");
  CHECK_EQUAL(
    to_string(bmi.lookup(relational_operator::less, (11000ns).count())), "11111"
                                                                         "0");
}

TEST(decimal binner with floating - point) {
  using binner = decimal_binner<1>;
  using coder_type = multi_level_coder<range_coder<null_bitmap>>;
  auto bmi = bitmap_index<double, coder_type, binner>{base::uniform<64>(2)};
  bmi.append(42.123);
  bmi.append(53.9);
  bmi.append(41.02014);
  bmi.append(44.91234543);
  bmi.append(39.5);
  bmi.append(49.5);
  CHECK(to_string(bmi.lookup(relational_operator::equal, 40.0)) == "101110");
  CHECK(to_string(bmi.lookup(relational_operator::equal, 50.0)) == "010001");
}

TEST(serialization) {
  using coder = multi_level_coder<equality_coder<null_bitmap>>;
  using bitmap_index_type = bitmap_index<int8_t, coder>;
  auto bmi1 = bitmap_index_type{base::uniform<8>(2)};
  bmi1.append(52);
  bmi1.append(84);
  bmi1.append(100);
  bmi1.append(-42);
  bmi1.append(-100);
  CHECK_EQUAL(to_string(bmi1.lookup(relational_operator::not_equal, 100)),
              "11011");
  caf::byte_buffer buf;
  CHECK_EQUAL(detail::serialize(buf, bmi1), true);
  auto bmi2 = bitmap_index_type{};
  CHECK_EQUAL(detail::legacy_deserialize(buf, bmi2), true);
  CHECK(bmi1 == bmi2);
  CHECK_EQUAL(to_string(bmi2.lookup(relational_operator::not_equal, 100)),
              "11011");
}

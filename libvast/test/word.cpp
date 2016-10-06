#include <cstdint>

#include "vast/word.hpp"

#define SUITE word
#include "test.hpp"

using namespace vast;

using w8 = word<uint8_t>;
using w64 = word<uint64_t>;

TEST(constants) {
  CHECK_EQUAL(w8::none, 0b00000000);
  CHECK_EQUAL(w8::all,  0b11111111);
  CHECK_EQUAL(w8::msb0, 0b01111111);
  CHECK_EQUAL(w8::msb1, 0b10000000);
  CHECK_EQUAL(w8::lsb0, 0b11111110);
  CHECK_EQUAL(w8::lsb1, 0b00000001);
}

TEST(masks) {
  CHECK_EQUAL(w8::mask(0), w8::lsb1);
  CHECK_EQUAL(w8::mask(1), 0b00000010);
  CHECK_EQUAL(w8::mask(7), w8::msb1);
  CHECK_EQUAL(w8::lsb_mask(7), w8::msb0);
  CHECK_EQUAL(w8::lsb_mask(3), 0b00000111);
  CHECK_EQUAL(w8::lsb_mask(5), 0b00011111);
  CHECK_EQUAL(w8::lsb_mask(0), w8::none);
  CHECK_EQUAL(w8::msb_mask(7), w8::lsb0);
  CHECK_EQUAL(w8::msb_mask(3), 0b11100000);
  CHECK_EQUAL(w8::msb_mask(5), 0b11111000);
  CHECK_EQUAL(w8::msb_mask(0), w8::none);
}

TEST(manipulation) {
  CHECK_EQUAL(w8::flip(w8::msb0, 7), w8::all);
  CHECK_EQUAL(w8::flip(w8::msb1, 7), w8::none);
  CHECK_EQUAL(w8::flip(w8::lsb0, 0), w8::all);
  CHECK_EQUAL(w8::flip(w8::lsb1, 0), w8::none);
  CHECK_EQUAL(w8::set(w8::lsb0, 0, 0), w8::lsb0); //nop
  CHECK_EQUAL(w8::set(w8::lsb0, 0, 1), w8::all);
  CHECK_EQUAL(w8::set(w8::none, 5, 1), 0b00100000);
  CHECK_EQUAL(w8::set(w8::all, 5, 0), 0b11011111);
}

TEST(counting) {
  CHECK_EQUAL(w8::count_trailing_zeros(0b00101000), 3u);
  CHECK_EQUAL(w8::count_trailing_ones(0b00101111), 4u);
  CHECK_EQUAL(w8::count_leading_zeros(0b00101000), 2u);
  CHECK_EQUAL(w8::count_leading_ones(0b11111110), 7u);
  CHECK_EQUAL(w8::popcount(0b10111100), 5u);
  CHECK_EQUAL(w8::popcount(0b01111110), 6u);
  CHECK_EQUAL(w8::parity(0b10111100), 1u);
  CHECK_EQUAL(w8::parity(0b01111110), 0u);
  // Make sure SFINAE overloads work.
  auto x = 0b0000000001010100010101000101010001010100010101000101010000000000;
  auto y = 0b1111111111111110000000000000000000000000000000000000000011111111;
  CHECK_EQUAL(w64::count_trailing_zeros(x), 10u);
  CHECK_EQUAL(w64::count_trailing_zeros(y), 0u);
  CHECK_EQUAL(w64::count_trailing_ones(x), 0u);
  CHECK_EQUAL(w64::count_trailing_ones(y), 8u);
  CHECK_EQUAL(w64::count_leading_zeros(x), 9u);
  CHECK_EQUAL(w64::count_leading_zeros(y), 0u);
  CHECK_EQUAL(w64::count_leading_ones(x), 0u);
  CHECK_EQUAL(w64::count_leading_ones(y), 15u);
  CHECK_EQUAL(w64::popcount(x), 18u);
  CHECK_EQUAL(w64::popcount(y), 23u);
  CHECK_EQUAL(w64::parity(x), 0u);
  CHECK_EQUAL(w64::parity(y), 1u);
}

TEST(next) {
  CHECK_EQUAL(w8::next(w8::none, 0), w8::npos);
  CHECK_EQUAL(w8::next(w8::none, 7), w8::npos);
  for (auto i = 0u; i < w8::width - 1; ++i)
    CHECK_EQUAL(w8::next(w8::all, i), i + 1);
  auto x = 0b0000000001010100010101000101010001010100010101000101010000000000;
  auto first_one = w64::count_trailing_zeros(x);
  auto last_one = w64::width - w64::count_leading_zeros(x) - 1;
  CHECK_EQUAL(w64::next(x, 0), first_one);
  CHECK_EQUAL(w64::next(x, 1), first_one);
  CHECK_EQUAL(w64::next(x, 9), first_one);
  CHECK_EQUAL(w64::next(x, 10), first_one + 2);
  CHECK_EQUAL(w64::next(x, last_one), w64::npos);
  CHECK_EQUAL(w64::next(x, last_one - 1), last_one);
  CHECK_EQUAL(w64::next(x, last_one - 2), last_one);
  CHECK_EQUAL(w64::next(x, last_one - 3), last_one - 2);
}

TEST(prev) {
  CHECK_EQUAL(w8::prev(w8::none, 0), w8::npos);
  CHECK_EQUAL(w8::prev(w8::none, 7), w8::npos);
  for (auto i = 1u; i < w8::width; ++i)
    CHECK_EQUAL(w8::prev(w8::all, i), i - 1);
  auto x = 0b1111111111111110000000000000000000000000000000000000000011111111;
  CHECK_EQUAL(w64::prev(x, 0), w64::npos);
  CHECK_EQUAL(w64::prev(x, 1), 0u);
  auto first_zero = w64::count_trailing_ones(x);
  auto last_zero = w64::width - w64::count_leading_ones(x) - 1;
  CHECK_EQUAL(w64::prev(x, first_zero), first_zero - 1);
  CHECK_EQUAL(w64::prev(x, first_zero + 10), first_zero - 1);
  CHECK_EQUAL(w64::prev(x, 63), 62u);
  CHECK_EQUAL(w64::prev(x, last_zero), first_zero - 1);
  CHECK_EQUAL(w64::prev(x, last_zero + 1), first_zero - 1);
  CHECK_EQUAL(w64::prev(x, last_zero + 2), last_zero + 1);
}

TEST(math) {
  CHECK_EQUAL(w8::log2(0b00000001), 0u);
  CHECK_EQUAL(w8::log2(0b00000010), 1u);
  CHECK_EQUAL(w8::log2(0b01001001), 6u);
  CHECK_EQUAL(w8::log2(0b10001001), 7u);
}

#include <cstdint>

#include "vast/bits.hpp"

#define SUITE bits
#include "test.hpp"

using namespace vast;

using b8 = bits<uint8_t>;
using b64 = bits<uint64_t>;

TEST(constants) {
  CHECK_EQUAL(b8::none, 0b00000000);
  CHECK_EQUAL(b8::all,  0b11111111);
  CHECK_EQUAL(b8::msb0, 0b01111111);
  CHECK_EQUAL(b8::msb1, 0b10000000);
  CHECK_EQUAL(b8::lsb0, 0b11111110);
  CHECK_EQUAL(b8::lsb1, 0b00000001);
}

TEST(manipulation) {
  CHECK_EQUAL(b8::mask(0), b8::lsb1);
  CHECK_EQUAL(b8::mask(1), 0b00000010);
  CHECK_EQUAL(b8::mask(7), b8::msb1);
  CHECK_EQUAL(b8::flip(b8::msb0, 7), b8::all);
  CHECK_EQUAL(b8::flip(b8::msb1, 7), b8::none);
  CHECK_EQUAL(b8::flip(b8::lsb0, 0), b8::all);
  CHECK_EQUAL(b8::flip(b8::lsb1, 0), b8::none);
  CHECK_EQUAL(b8::set(b8::lsb0, 0, 0), b8::lsb0); //nop
  CHECK_EQUAL(b8::set(b8::lsb0, 0, 1), b8::all);
  CHECK_EQUAL(b8::set(b8::none, 5, 1), 0b00100000);
  CHECK_EQUAL(b8::set(b8::all, 5, 0), 0b11011111);
}

TEST(counting) {
  CHECK_EQUAL(b8::count_trailing_zeros(0b00101000u), 3u);
  CHECK_EQUAL(b8::count_trailing_ones(0b00101111u), 4u);
  CHECK_EQUAL(b8::count_leading_zeros(0b00101000u), 2u);
  CHECK_EQUAL(b8::count_leading_ones(0b11111110u), 7u);
  CHECK_EQUAL(b8::popcount(0b10111100u), 5u);
  CHECK_EQUAL(b8::popcount(0b01111110u), 6u);
  CHECK_EQUAL(b8::parity(0b10111100u), 1u);
  CHECK_EQUAL(b8::parity(0b01111110u), 0u);
  // Make sure SFINAE overloads work.
  auto x = 0b0000000001010100010101000101010001010100010101000101010000000000u;
  auto y = 0b1111111111111110000000000000000000000000000000000000000011111111u;
  CHECK_EQUAL(b64::count_trailing_zeros(x), 10u);
  CHECK_EQUAL(b64::count_trailing_zeros(y), 0u);
  CHECK_EQUAL(b64::count_trailing_ones(x), 0u);
  CHECK_EQUAL(b64::count_trailing_ones(y), 8u);
  CHECK_EQUAL(b64::count_leading_zeros(x), 9u);
  CHECK_EQUAL(b64::count_leading_zeros(y), 0u);
  CHECK_EQUAL(b64::count_leading_ones(x), 0u);
  CHECK_EQUAL(b64::count_leading_ones(y), 15u);
  CHECK_EQUAL(b64::popcount(x), 18u);
  CHECK_EQUAL(b64::popcount(y), 23u);
  CHECK_EQUAL(b64::parity(x), 0u);
  CHECK_EQUAL(b64::parity(y), 1u);
}

TEST(math) {
  CHECK_EQUAL(b8::log2(0b00000001u), 0u);
  CHECK_EQUAL(b8::log2(0b00000010u), 1u);
  CHECK_EQUAL(b8::log2(0b01001001u), 6u);
  CHECK_EQUAL(b8::log2(0b10001001u), 7u);
}

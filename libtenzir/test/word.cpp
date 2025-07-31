//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/word.hpp"

#include "tenzir/test/test.hpp"

#include <cstdint>

using namespace tenzir;

using w8 = word<uint8_t>;
using w64 = word<uint64_t>;

TEST("constants") {
  CHECK_EQUAL(w8::none, 0b00000000);
  CHECK_EQUAL(w8::all, 0b11111111);
  CHECK_EQUAL(w8::msb0, 0b01111111);
  CHECK_EQUAL(w8::msb1, 0b10000000);
  CHECK_EQUAL(w8::lsb0, 0b11111110);
  CHECK_EQUAL(w8::lsb1, 0b00000001);
}

TEST("masks") {
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
  CHECK_EQUAL(w8::lsb_fill(w8::width), w8::all);
  CHECK_EQUAL(w8::lsb_fill(1), w8::lsb1);
  CHECK_EQUAL(w8::lsb_fill(3), 0b00000111);
  CHECK_EQUAL(w8::msb_fill(w8::width), w8::all);
  CHECK_EQUAL(w8::msb_fill(1), w8::msb1);
  CHECK_EQUAL(w8::msb_fill(3), 0b11100000);
}

TEST("tests") {
  CHECK(w8::all_or_none(w8::all));
  CHECK(w8::all_or_none(w8::none));
  CHECK(! w8::all_or_none(w8::msb0));
  CHECK(! w8::all_or_none(w8::msb1));
  CHECK(! w8::all_or_none(w8::lsb0));
  CHECK(! w8::all_or_none(w8::lsb1));
  for (auto i = 0u; i < w8::width; ++i) {
    CHECK(w8::all_or_none(w8::all, i));
    CHECK(w8::all_or_none(w8::none, i));
  }
  for (auto i = 0u; i < w8::width / 2; ++i) {
    CHECK(w8::all_or_none(0b11111000, i));
    CHECK(w8::all_or_none(0b00000111, i));
  }
  for (auto i = w8::width / 2; i < w8::width; ++i) {
    CHECK(! w8::all_or_none(0b11111000, i));
    CHECK(! w8::all_or_none(0b00000111, i));
  }
  CHECK(w8::test(w8::all, 0));
  CHECK(w8::test(w8::all, 7));
  CHECK(! w8::test(w8::none, 0));
  CHECK(! w8::test(w8::none, 7));
  CHECK(! w8::test(0b00100000, 4));
  CHECK(w8::test(0b00100000, 5));
  CHECK(! w8::test(0b00100000, 6));
}

TEST("manipulation") {
  CHECK_EQUAL(w8::flip(w8::msb0, 7), w8::all);
  CHECK_EQUAL(w8::flip(w8::msb1, 7), w8::none);
  CHECK_EQUAL(w8::flip(w8::lsb0, 0), w8::all);
  CHECK_EQUAL(w8::flip(w8::lsb1, 0), w8::none);
  CHECK_EQUAL(w8::set<0>(w8::lsb0, 0), w8::lsb0); // nop
  CHECK_EQUAL(w8::set<1>(w8::lsb0, 0), w8::all);
  CHECK_EQUAL(w8::set(w8::lsb0, 0, 0), w8::lsb0);
  CHECK_EQUAL(w8::set(w8::lsb0, 0, 1), w8::all);
  CHECK_EQUAL(w8::set(w8::none, 5, 1), 0b00100000);
  CHECK_EQUAL(w8::set(w8::all, 5, 0), 0b11011111);
}

TEST("counting") {
  CHECK_EQUAL(w8::count_trailing_zeros(0b00101000), 3u);
  CHECK_EQUAL(w8::count_trailing_ones(0b00101111), 4u);
  CHECK_EQUAL(w8::count_leading_zeros(0b00101000), 2u);
  CHECK_EQUAL(w8::count_leading_ones(0b11111110), 7u);
  CHECK_EQUAL(w8::popcount(0b10111100), 5u);
  CHECK_EQUAL(w8::popcount(0b01111110), 6u);
  CHECK_EQUAL(w8::parity(0b10111100), 1u);
  CHECK_EQUAL(w8::parity(0b01111110), 0u);
  // Make sure SFINAE overloads work.
  auto x = 0b0000000001010100010101000101010001010100010101000101010000000000u;
  auto y = 0b1111111111111110000000000000000000000000000000000000000011111111u;
  CHECK_EQUAL(w64::count_trailing_zeros(x), 10u);
  CHECK_EQUAL(w64::count_trailing_zeros(y), 0u);
  CHECK_EQUAL(w64::count_trailing_ones(x), 0u);
  CHECK_EQUAL(w64::count_trailing_ones(y), 8u);
  CHECK_EQUAL(w64::count_leading_zeros(x), 9u);
  CHECK_EQUAL(w64::count_leading_zeros(y), 0u);
  CHECK_EQUAL(w64::count_leading_ones(x), 0u);
  CHECK_EQUAL(w64::count_leading_ones(y), 15u);
  MESSAGE("popcount");
  CHECK_EQUAL(w64::popcount(x), 18u);
  CHECK_EQUAL(w64::popcount(y), 23u);
  MESSAGE("parity");
  CHECK_EQUAL(w64::parity(x), 0u);
  CHECK_EQUAL(w64::parity(y), 1u);
}

TEST("word rank") {
  auto x = 0b0000000001010100010101000101010001010100010101000101010000000000u;
  auto y = 0b1111111111111110000000000000000000000000000000000000000011111111u;
  MESSAGE("rank");
  for (auto i = 0u; i < w8::width; ++i) {
    CHECK_EQUAL(rank(w8::all, i), i + 1);
  }
  CHECK_EQUAL(rank(0b01011000u, 7), 3u);
  CHECK_EQUAL(rank(0b01011000u, 3), 1u);
  CHECK_EQUAL(rank(0b01011000u, 4), 2u);
  CHECK_EQUAL(rank(0b01011000u, 5), 2u);
  CHECK_EQUAL(rank(x, 63), w64::popcount(x));
  CHECK_EQUAL(rank(y, 63), w64::popcount(y));
  CHECK_EQUAL(rank(x, 0), 0u);
  CHECK_EQUAL(rank(y, 0), 1u);
  CHECK_EQUAL(rank(x, 1), 0u);
  CHECK_EQUAL(rank(y, 1), 2u);
  CHECK_EQUAL(rank(x, 10), 1u);
  CHECK_EQUAL(rank(y, 10), 8u);
}

TEST("find_next") {
  CHECK_EQUAL(find_next(w8::none, 0), w8::npos);
  CHECK_EQUAL(find_next(w8::none, 7), w8::npos);
  for (auto i = 0u; i < w8::width - 1; ++i) {
    CHECK_EQUAL(find_next(w8::all, i), i + 1);
  }
  auto x = 0b0000000001010100010101000101010001010100010101000101010000000000u;
  auto first_one = w64::count_trailing_zeros(x);
  auto last_one = w64::width - w64::count_leading_zeros(x) - 1;
  CHECK_EQUAL(find_next(x, 0), first_one);
  CHECK_EQUAL(find_next(x, 1), first_one);
  CHECK_EQUAL(find_next(x, 9), first_one);
  CHECK_EQUAL(find_next(x, 10), first_one + 2);
  CHECK_EQUAL(find_next(x, last_one), w64::npos);
  CHECK_EQUAL(find_next(x, last_one - 1), last_one);
  CHECK_EQUAL(find_next(x, last_one - 2), last_one);
  CHECK_EQUAL(find_next(x, last_one - 3), last_one - 2);
}

TEST("find_prev") {
  CHECK_EQUAL(find_prev(w8::none, 0), w8::npos);
  CHECK_EQUAL(find_prev(w8::none, 7), w8::npos);
  for (auto i = 1u; i < w8::width; ++i) {
    CHECK_EQUAL(find_prev(w8::all, i), i - 1);
  }
  auto x = 0b1111111111111110000000000000000000000000000000000000000011111111u;
  CHECK_EQUAL(find_prev(x, 0), w64::npos);
  CHECK_EQUAL(find_prev(x, 1), 0u);
  auto first_zero = w64::count_trailing_ones(x);
  auto last_zero = w64::width - w64::count_leading_ones(x) - 1;
  CHECK_EQUAL(find_prev(x, first_zero), first_zero - 1);
  CHECK_EQUAL(find_prev(x, first_zero + 10), first_zero - 1);
  CHECK_EQUAL(find_prev(x, 63), 62u);
  CHECK_EQUAL(find_prev(x, last_zero), first_zero - 1);
  CHECK_EQUAL(find_prev(x, last_zero + 1), first_zero - 1);
  CHECK_EQUAL(find_prev(x, last_zero + 2), last_zero + 1);
}

TEST("select") {
  CHECK_EQUAL(select(w8::none, 1), w8::npos);
  for (auto i = 0u; i < w8::width; ++i) {
    CHECK_EQUAL(select(w8::all, i + 1), i);
  }
  CHECK_EQUAL(select(w8::msb1, 1), 7u);
  CHECK_EQUAL(select(w8::msb1, 2), w8::npos);
  CHECK_EQUAL(select(w8::lsb1, 1), 0u);
  CHECK_EQUAL(select(w8::lsb1, 2), w8::npos);
  CHECK_EQUAL(select(0b01011000u, 1), 3u);
  CHECK_EQUAL(select(0b01011000u, 2), 4u);
  CHECK_EQUAL(select(0b01011000u, 3), 6u);
  CHECK_EQUAL(select(0b01011000u, 4), w8::npos);
}

TEST("math") {
  CHECK_EQUAL(w8::log2(0b00000001), 0u);
  CHECK_EQUAL(w8::log2(0b00000010), 1u);
  CHECK_EQUAL(w8::log2(0b01001001), 6u);
  CHECK_EQUAL(w8::log2(0b10001001), 7u);
}

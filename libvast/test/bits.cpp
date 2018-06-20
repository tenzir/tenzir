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

#include <cstdint>

#include "vast/bits.hpp"

#define SUITE bits
#include "test.hpp"

using namespace vast;

namespace {

using bits8 = bits<uint8_t>;
using w8 = bits8::word_type;

using bits64 = bits<uint64_t>;
using w64 = bits64::word_type;

} // namespace <anonymous>

TEST(access) {
  auto x = bits8{0b10110010};
  CHECK(!x[0]);
  CHECK(x[1]);
  CHECK(!x[2]);
  CHECK(!x[3]);
  CHECK(x[4]);
  CHECK(x[5]);
  CHECK(!x[6]);
  CHECK(x[7]);
  x = bits8{0b10110010, 5};
  CHECK(x[4]);
  CHECK(!(x.data() & w8::mask(5)));
  CHECK(!(x.data() & w8::mask(6)));
  CHECK(!(x.data() & w8::mask(7)));
  x = bits8{w8::all, 1337};
  CHECK(x[0]);
  CHECK(x[1000]);
  CHECK(x[1336]);
  x = bits8{w8::none, 1337};
  CHECK(!x[0]);
  CHECK(!x[1000]);
  CHECK(!x[1336]);
}

TEST(homogeneity) {
  CHECK(!bits8{0b10110000}.homogeneous());
  CHECK(bits8{0b10110000, 4}.homogeneous());
  CHECK(bits8{0b10111111, 6}.homogeneous());
  CHECK(bits8{w8::all}.homogeneous());
  CHECK(bits8{w8::none}.homogeneous());
}

TEST(finding - block) {
  MESSAGE("8 bits");
  auto x = bits8{0b00000001};
  CHECK_EQUAL(find_first<1>(x), 0u);
  CHECK_EQUAL(find_next<1>(x, 0), w8::npos);
  CHECK_EQUAL(find_next<1>(x, 1), w8::npos);
  CHECK_EQUAL(find_next<1>(x, 7), w8::npos);
  CHECK_EQUAL(find_last<1>(x), 0u);
  CHECK_EQUAL(find_first<0>(x), 1u);
  CHECK_EQUAL(find_next<0>(x, 0), 1u);
  CHECK_EQUAL(find_next<0>(x, 1), 2u);
  CHECK_EQUAL(find_next<0>(x, 7), w8::npos);
  CHECK_EQUAL(find_last<0>(x), 7u);
  x = bits8{0b10110010};
  CHECK_EQUAL(find_first<1>(x), 1u);
  CHECK_EQUAL(find_next<1>(x, 0), 1u);
  CHECK_EQUAL(find_next<1>(x, 1), 4u);
  CHECK_EQUAL(find_next<1>(x, 7), w8::npos);
  CHECK_EQUAL(find_last<1>(x), 7u);
  CHECK_EQUAL(find_first<0>(x), 0u);
  CHECK_EQUAL(find_next<0>(x, 0), 2u);
  CHECK_EQUAL(find_next<0>(x, 2), 3u);
  CHECK_EQUAL(find_next<0>(x, 3), 6u);
  CHECK_EQUAL(find_next<0>(x, 6), w8::npos);
  CHECK_EQUAL(find_next<0>(x, 7), w8::npos);
  CHECK_EQUAL(find_last<0>(x), 6u);
  x = bits8{0b10000000, 7};
  CHECK_EQUAL(find_first<1>(x), w8::npos);
  CHECK_EQUAL(find_last<1>(x), w8::npos);
  CHECK_EQUAL(find_first<0>(x), 0u);
  CHECK_EQUAL(find_last<0>(x), 6u);
  x = bits8{0b01111111, 6};
  CHECK_EQUAL(find_first<1>(x), 0u);
  CHECK_EQUAL(find_last<1>(x), 5u);
  CHECK_EQUAL(find_next<1>(x, 0), 1u);
  CHECK_EQUAL(find_next<1>(x, 4), 5u);
  CHECK_EQUAL(find_next<1>(x, 5), w8::npos);
  CHECK_EQUAL(find_first<0>(x), w8::npos);
  CHECK_EQUAL(find_last<0>(x), w8::npos);
  CHECK_EQUAL(find_next<0>(x, 0), w8::npos);
  CHECK_EQUAL(find_next<0>(x, 4), w8::npos);
  CHECK_EQUAL(find_next<0>(x, 5), w8::npos);
  MESSAGE("64 bits");
  auto y =
    bits64{0b0000000001010100010101000101010001010100010101000101010000000000};
  CHECK_EQUAL(find_first<1>(y), 10u);
  CHECK_EQUAL(find_last<1>(y), 54u);
  CHECK_EQUAL(find_first<0>(y), 0u);
  CHECK_EQUAL(find_last<0>(y), 63u);
  y =
    bits64{0b1111111111111110000000000000000000000000000000000000000011111111};
  CHECK_EQUAL(find_first<1>(y), 0u);
  CHECK_EQUAL(find_last<1>(y), 63u);
  CHECK_EQUAL(find_first<0>(y), 8u);
  CHECK_EQUAL(find_last<0>(y), 48u);
  y = bits64{0b0111101111111110000000001000000000001000000000000000000011110111,
             48};
  CHECK_EQUAL(find_first<1>(y), 0u);
  CHECK_EQUAL(find_last<1>(y), 39u);
  CHECK_EQUAL(find_first<0>(y), 3u);
  CHECK_EQUAL(find_last<0>(y), 47u);
}

TEST(finding - sequence) {
  MESSAGE("all");
  auto x = bits8{w8::all, 666};
  CHECK_EQUAL(find_first<1>(x), 0u);
  CHECK_EQUAL(find_next<1>(x, 0), 1u);
  CHECK_EQUAL(find_next<1>(x, 1), 2u);
  CHECK_EQUAL(find_last<1>(x), 665u);
  CHECK_EQUAL(find_first<0>(x), w8::npos);
  CHECK_EQUAL(find_next<0>(x, 0), w8::npos);
  CHECK_EQUAL(find_next<0>(x, 100), w8::npos);
  CHECK_EQUAL(find_last<0>(x), w8::npos);
  MESSAGE("none");
  x = bits8{w8::none, 666};
  CHECK_EQUAL(find_first<0>(x), 0u);
  CHECK_EQUAL(find_next<0>(x, 0), 1u);
  CHECK_EQUAL(find_next<0>(x, 1), 2u);
  CHECK_EQUAL(find_last<0>(x), 665u);
  CHECK_EQUAL(find_first<1>(x), w8::npos);
  CHECK_EQUAL(find_next<1>(x, 0), w8::npos);
  CHECK_EQUAL(find_next<1>(x, 100), w8::npos);
  CHECK_EQUAL(find_last<1>(x), w8::npos);
}

TEST(counting) {
  CHECK_EQUAL(rank(bits8{w8::all}, 0), 1u);
  CHECK_EQUAL(rank(bits8{w8::all}, 1), 2u);
  CHECK_EQUAL(rank(bits8{w8::all}, 2), 3u);
  CHECK_EQUAL(rank(bits8{w8::all}, 3), 4u);
  CHECK_EQUAL(rank(bits8{w8::all}, 4), 5u);
  CHECK_EQUAL(rank(bits8{w8::all}, 5), 6u);
  CHECK_EQUAL(rank(bits8{w8::all}, 6), 7u);
  CHECK_EQUAL(rank(bits8{w8::all}, 7), 8u);
  CHECK_EQUAL(rank(bits8(w8::all)), 8u);
  CHECK_EQUAL(rank(bits8(w8::none)), 0u);
  CHECK_EQUAL(rank(bits8{0b1011'0000}, 4), 1u);
  CHECK_EQUAL(rank(bits8{0b1011'1011}, 6), 5u);
  CHECK_EQUAL(rank(bits8{0b1011'1011}), 6u);
}


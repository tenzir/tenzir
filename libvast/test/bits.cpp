#include <cstdint>

#include "vast/bits.hpp"

#define SUITE bits
#include "test.hpp"

using namespace vast;

namespace {

using bits8 = bits<uint8_t>;
using bits64 = bits<uint64_t>;

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
  CHECK(!(x.data() & bits8::word_type::mask(5)));
  CHECK(!(x.data() & bits8::word_type::mask(6)));
  CHECK(!(x.data() & bits8::word_type::mask(7)));
  x = bits8{bits8::word_type::all, 1337};
  CHECK(x[0]);
  CHECK(x[1000]);
  CHECK(x[1336]);
  x = bits8{bits8::word_type::none, 1337};
  CHECK(!x[0]);
  CHECK(!x[1000]);
  CHECK(!x[1336]);
}

TEST(homogeneity) {
  CHECK(!bits8{0b10110000}.homogeneous());
  CHECK(bits8{0b10110000, 4}.homogeneous());
  CHECK(bits8{0b10111111, 6}.homogeneous());
  CHECK(bits8{bits8::word_type::all}.homogeneous());
  CHECK(bits8{bits8::word_type::none}.homogeneous());
}

TEST(count) {
  CHECK_EQUAL(bits8(0b10110000, 4).count(), 0u);
  CHECK_EQUAL(bits8(0b10111011, 6).count(), 5u);
  CHECK_EQUAL(bits8(0b10111011).count(), 6u);
  CHECK_EQUAL(bits8(bits8::word_type::all).count(), 8u);
  CHECK_EQUAL(bits8(bits8::word_type::none).count(), 0u);
}

TEST(finding - block) {
  MESSAGE("8 bits");
  auto x = bits8{0b00000001};
  CHECK_EQUAL(x.find_first<1>(), 0u);
  CHECK_EQUAL(x.find_next<1>(0), bits8::word_type::npos);
  CHECK_EQUAL(x.find_next<1>(1), bits8::word_type::npos);
  CHECK_EQUAL(x.find_next<1>(7), bits8::word_type::npos);
  CHECK_EQUAL(x.find_last<1>(), 0u);
  CHECK_EQUAL(x.find_first<0>(), 1u);
  CHECK_EQUAL(x.find_next<0>(0), 1u);
  CHECK_EQUAL(x.find_next<0>(1), 2u);
  CHECK_EQUAL(x.find_next<0>(7), bits8::word_type::npos);
  CHECK_EQUAL(x.find_last<0>(), 7u);
  x = bits8{0b10110010};
  CHECK_EQUAL(x.find_first<1>(), 1u);
  CHECK_EQUAL(x.find_next<1>(0), 1u);
  CHECK_EQUAL(x.find_next<1>(1), 4u);
  CHECK_EQUAL(x.find_next<1>(7), bits8::word_type::npos);
  CHECK_EQUAL(x.find_last<1>(), 7u);
  CHECK_EQUAL(x.find_first<0>(), 0u);
  CHECK_EQUAL(x.find_next<0>(0), 2u);
  CHECK_EQUAL(x.find_next<0>(2), 3u);
  CHECK_EQUAL(x.find_next<0>(3), 6u);
  CHECK_EQUAL(x.find_next<0>(6), bits8::word_type::npos);
  CHECK_EQUAL(x.find_next<0>(7), bits8::word_type::npos);
  CHECK_EQUAL(x.find_last<0>(), 6u);
  MESSAGE("64 bits");
  auto y =
    bits64{0b0000000001010100010101000101010001010100010101000101010000000000};
  CHECK_EQUAL(y.find_first<1>(), 10u);
  CHECK_EQUAL(y.find_last<1>(), 54u);
  CHECK_EQUAL(y.find_first<0>(), 0u);
  CHECK_EQUAL(y.find_last<0>(), 63u);
  y =
    bits64{0b1111111111111110000000000000000000000000000000000000000011111111};
  CHECK_EQUAL(y.find_first<1>(), 0u);
  CHECK_EQUAL(y.find_last<1>(), 63u);
  CHECK_EQUAL(y.find_first<0>(), 8u);
  CHECK_EQUAL(y.find_last<0>(), 48u);
}

TEST(finding - sequence) {
  MESSAGE("all");
  auto x = bits8{bits8::word_type::all, 666};
  CHECK_EQUAL(x.find_first<1>(), 0u);
  CHECK_EQUAL(x.find_next<1>(0), 1u);
  CHECK_EQUAL(x.find_next<1>(1), 2u);
  CHECK_EQUAL(x.find_last<1>(), 665u);
  CHECK_EQUAL(x.find_first<0>(), bits8::word_type::npos);
  CHECK_EQUAL(x.find_next<0>(0), bits8::word_type::npos);
  CHECK_EQUAL(x.find_next<0>(100), bits8::word_type::npos);
  CHECK_EQUAL(x.find_last<0>(), bits8::word_type::npos);
  MESSAGE("none");
  x = bits8{bits8::word_type::none, 666};
  CHECK_EQUAL(x.find_first<0>(), 0u);
  CHECK_EQUAL(x.find_next<0>(0), 1u);
  CHECK_EQUAL(x.find_next<0>(1), 2u);
  CHECK_EQUAL(x.find_last<0>(), 665u);
  CHECK_EQUAL(x.find_first<1>(), bits8::word_type::npos);
  CHECK_EQUAL(x.find_next<1>(0), bits8::word_type::npos);
  CHECK_EQUAL(x.find_next<1>(100), bits8::word_type::npos);
  CHECK_EQUAL(x.find_last<1>(), bits8::word_type::npos);
}

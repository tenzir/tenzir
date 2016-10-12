#include "vast/null_bitmap.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/bitmap.hpp"

#define SUITE bitmap
#include "test.hpp"

using namespace vast;
using namespace std::string_literals;

namespace {

template <class Bitmap>
struct bitmap_test_harness {
  bitmap_test_harness() {
    CHECK(x.empty());
    CHECK(y.empty());
    CHECK_EQUAL(x.size(), 0u);
    CHECK_EQUAL(y.size(), 0u);
  }

  void test_construction() {
    MESSAGE("copy construction");
    Bitmap a{x};
    CHECK_EQUAL(a, x);
    MESSAGE("move construction");
    Bitmap b{std::move(a)};
    CHECK_EQUAL(b, x);
  }

  void test_append() {
    MESSAGE("bitmap-specific append");
    CHECK(x.append_bit(false));
    CHECK(x.append_block(0b0111000, 7));
    CHECK(x.append_bits(true, 20));
    CHECK(x.append_bit(true));
    CHECK(x.append_block(0b0111000, 7));
    CHECK(x.append_bits(true, 20));
    CHECK(y.append_bits(true, 11));
    CHECK(y.append_bits(false, 34));
    CHECK(y.append_bit(true));
    CHECK(y.append_bits(true, 6));
    auto s = to_string(x);
    CHECK_EQUAL(s, "00001110111111111111111111111000111011111111111111111111");
    s = to_string(y);
    CHECK_EQUAL(s, "1111111111100000000000000000000000000000000001111111");
    MESSAGE("longer sequence");
    CHECK(a.append_bit(false));
    CHECK(a.append_bit(true));
    CHECK(a.append_bits(false, 421));
    CHECK(a.append_bit(true));
    CHECK(a.append_bit(true));
    CHECK_EQUAL(a.size(), 425u);
    s = "01";
    s.append(421, '0');
    s += "11";
    std::string str;
    CHECK_EQUAL(to_string(a), s);
    b.append_bits(true, 222);
    b.append_bit(false);
    b.append_bit(true);
    b.append_bit(false);
    b.append_block(0x000000cccccccccc);
    b.append_bit(false);
    b.append_bit(true);
    s.clear();
    s.append(222, '1');
    s += "010";
    // 0xcccccccccc
    s += "0011001100110011001100110011001100110011000000000000000000000000";
    s += "01";
    CHECK_EQUAL(to_string(b), s);
  }

  void test_simple_bitwise_operations() {
    MESSAGE("simple unary");
    CHECK_EQUAL(~~a, a);
    CHECK_EQUAL(~~b, b);
    CHECK_EQUAL(~~x, x);
    CHECK_EQUAL(~~y, y);
    auto s = to_string(~x);
    CHECK_EQUAL(s, "11110001000000000000000000000111000100000000000000000000");
    MESSAGE("simple binary");
    s = to_string(x & y);
    CHECK_EQUAL(s, "00001110111000000000000000000000000000000000011111110000");
    s = to_string(x | y);
    CHECK_EQUAL(s, "11111111111111111111111111111000111011111111111111111111");
    s = to_string(x ^ y);
    CHECK_EQUAL(s, "11110001000111111111111111111000111011111111100000001111");
    s = to_string(x - y);
    CHECK_EQUAL(s, "00000000000111111111111111111000111011111111100000001111");
    s = to_string(y - x);
    CHECK_EQUAL(s, "11110001000000000000000000000000000000000000000000000000");
  }

  void test_bitwise_and() {
    MESSAGE("bitwise AND");
    Bitmap bm1, bm2;
    bm1.append_bit(false);
    bm1.append_bits(true, 63);
    bm1.append_bits(true, 32);
    bm2.append_block(0xfcfcfcfc, 48);
    auto str = "00111111001111110011111100111111"s;
    str += "0000000000000000000000000000000000000000000000000000000000000000";
    CHECK_EQUAL(to_string(bm1 & bm2), str);
  }

  void test_bitwise_or() {
    MESSAGE("bitwise OR");
    Bitmap bm1, bm2;
    bm1.append_bits(true, 50);
    bm2.append_bits(false, 50);
    bm2.append_bits(true, 50);
    CHECK_EQUAL(to_string(bm1 | bm2), std::string(100, '1'));
  }

  void test_bitwise_nand() {
    Bitmap bm1, bm2;
    bm1.append_bits(true, 100);
    bm2.append_bit(true);
    bm2.append_bits(false, 50);
    bm2.append_bits(true, 13);
    auto str = "0"s;
    str.append(50, '1');
    str.append(13, '0');
    str.append(36, '1');
    CHECK_EQUAL(to_string(bm1 - bm2), str);
  }

  void test_rank() {
    MESSAGE("rank");
    Bitmap bm;
    bm.append_bit(true);
    bm.append_bit(false);
    CHECK_EQUAL(rank<0>(bm), 1u);
    CHECK_EQUAL(rank<1>(bm), 1u);
    bm.append_bits(true, 62);
    CHECK_EQUAL(rank<0>(bm), 1u);
    CHECK_EQUAL(rank<1>(bm), 63u);
    bm.append_bits(false, 320);
    CHECK_EQUAL(rank<0>(bm), 321u);
    CHECK_EQUAL(rank<1>(bm), 63u);
    bm.append_bits(true, 512);
    CHECK_EQUAL(rank<0>(bm), 321u);
    CHECK_EQUAL(rank<1>(bm), 575u);
    bm.append_bits(false, 47);
    CHECK_EQUAL(rank<0>(bm), 368u);
    CHECK_EQUAL(rank<1>(bm), 575u);
    MESSAGE("partial rank");
    CHECK_EQUAL(rank<0>(bm, 1), 1u);
    CHECK_EQUAL(rank<1>(bm, 1), 1u);
    CHECK_EQUAL(rank<0>(bm, 10), 1u);
    CHECK_EQUAL(rank<1>(bm, 10), 10u);
    CHECK_EQUAL(rank<0>(bm, 10), 1u);
    CHECK_EQUAL(rank<0>(bm, bm.size() - 1), 368u);
    CHECK_EQUAL(rank<1>(bm, bm.size() - 1), 575u);
    CHECK_EQUAL(rank<0>(bm, bm.size() - 2), 367u);
    CHECK_EQUAL(rank<1>(bm, bm.size() - 2), 575u);
  }

  void test_select() {
    MESSAGE("select");
    CHECK_EQUAL(select<0>(b, 1), 222u);
    CHECK_EQUAL(select<0>(b, 2), 224u);
    CHECK_EQUAL(select<0>(b, 3), 225u);
    CHECK_EQUAL(select<0>(b, 4), 226u);
    CHECK_EQUAL(select<1>(b, 1), 0u);
    CHECK_EQUAL(select<1>(b, 100), 99u);
    CHECK_EQUAL(select<1>(b, 222), 221u);
    CHECK_EQUAL(select<1>(b, 223), 223u);
    CHECK_EQUAL(select<1>(b, 224), 227u);
    CHECK_EQUAL(select<1>(b, rank<1>(b)), b.size() - 1); // last bit
  }

  void test_printable() {
    std::string str;
    printers::bitmap<Bitmap, policy::rle>(str, a);
    // TODO: we could change the filter so that consecutive runs never occur,
    // even at word boundaries.
    //CHECK_EQUAL(str, "1F1T421F2T");
    CHECK_EQUAL(str, "1F1T62F320F39F2T");
    str.clear();
    printers::bitmap<Bitmap, policy::rle>(str, x);
    CHECK_EQUAL(str, "4F3T1F21T3F3T1F20T");
    str.clear();
    printers::bitmap<Bitmap, policy::rle>(str, y);
    CHECK_EQUAL(str, "11T34F7T");
  }

  void execute() {
    test_append();
    test_construction();
    test_simple_bitwise_operations();
    test_bitwise_and();
    test_bitwise_or();
    test_bitwise_nand();
    test_rank();
    test_select();
    test_printable();
  }

  Bitmap a;
  Bitmap b;
  Bitmap x;
  Bitmap y;
};

} // namespace <anonymous>

FIXTURE_SCOPE(null_bitmap_tests, bitmap_test_harness<null_bitmap>)

TEST(null_bitmap) {
  execute();
}

FIXTURE_SCOPE_END()

FIXTURE_SCOPE(ewah_bitmap_tests, bitmap_test_harness<ewah_bitmap>)

TEST(ewah_bitmap) {
  execute();
}

FIXTURE_SCOPE_END()

FIXTURE_SCOPE(bitmap_tests, bitmap_test_harness<bitmap>)

TEST(bitmap) {
  execute();
}

FIXTURE_SCOPE_END()

namespace {

ewah_bitmap make_ewah1() {
  ewah_bitmap bm;
  bm.append_bits(true, 10);
  bm.append_bits(false, 20);
  bm.append_bits(true, 40);
  bm.append_bit(false);
  bm.append_bit(true);
  bm.append_bit(false);
  bm.append_bits(true, 53);
  bm.append_bit(false);
  bm.append_bit(false);
  bm.append_bits(true, 192);
  bm.append_bits(false, 64 * 16);
  bm.append_bits(true, 64ull * ((1ull << 32) - 1));
  bm.append_bit(false);
  bm.append_bits(true, 63);
  for (auto i = 0; i < 64; ++i)
    bm.append_bit(i % 2 == 0);
  bm.append_bits(false, (1ull << (32 + 3)) * 64);
  bm.append_bit(true);
  return bm;
}

ewah_bitmap make_ewah2() {
  ewah_bitmap bm;
  bm.append_bit(false);
  bm.append_bit(true);
  bm.append_bits(false, 421);
  bm.append_bits(true, 2);
  return bm;
}

ewah_bitmap make_ewah3() {
  ewah_bitmap bm;
  bm.append_bits(true, 222);
  bm.append_bit(false);
  bm.append_bit(true);
  bm.append_bit(false);
  bm.append_block(0xcccccccccc);
  bm.append_bit(false);
  bm.append_bit(true);
  return bm;
}

std::string to_block_string(ewah_bitmap const& bm) {
  using word_type = ewah_bitmap::word_type;
  std::string str;
  if (bm.blocks().empty())
    return str;
  auto last = bm.blocks().end() - 1;
  auto partial = bm.size() % word_type::width;
  if (partial == 0)
    ++last;
  for (auto i = bm.blocks().begin(); i != last; ++i) {
    for (auto b = 0u; b < word_type::width; ++b)
      str += word_type::test(*i, word_type::width - b - 1) ? '1' : '0';
    str += '\n';
  }
  if (partial > 0) {
    str.append(word_type::width - partial, ' ');
    for (auto b = 0u; b < partial; ++b)
      str += word_type::test(*last, partial - b - 1) ? '1' : '0';
    str += '\n';
  }
  return str;
}

} // namespace <anonymous>

TEST(EWAH construction 1) {
  ewah_bitmap bm;
  bm.append_bits(true, 10);
  bm.append_bits(false, 20);
  auto str =
    "0000000000000000000000000000000000000000000000000000000000000000\n"
    "                                  000000000000000000001111111111\n";
  REQUIRE_EQUAL(to_block_string(bm), str);
  // Cause the first dirty block to overflow and bumps the dirty counter of
  // the first marker to 1.
  bm.append_bits(true, 40);
  // Fill up another dirty block.
  bm.append_bit(false);
  bm.append_bit(true);
  bm.append_bit(false);
  bm.append_bits(true, 53);
  bm.append_bit(false);
  bm.append_bit(false);
  CHECK_EQUAL(bm.size(), 128u);
  // Bump the dirty count to 2 and fill up the current dirty block.
  bm.append_bit(true);
  bm.append_bits(true, 63);
  str =
    "0000000000000000000000000000000000000000000000000000000000000010\n"
    "1111111111111111111111111111111111000000000000000000001111111111\n"
    "0011111111111111111111111111111111111111111111111111111010111111\n"
    "1111111111111111111111111111111111111111111111111111111111111111\n";
  REQUIRE_EQUAL(to_block_string(bm), str);
  // Appending anything now transforms the last block into a marker, because
  // it it turns out it was all 1s.
  bm.append_bit(true);
  str =
    "0000000000000000000000000000000000000000000000000000000000000010\n"
    "1111111111111111111111111111111111000000000000000000001111111111\n"
    "0011111111111111111111111111111111111111111111111111111010111111\n"
    "1000000000000000000000000000000010000000000000000000000000000000\n"
    "                                                               1\n";
  REQUIRE_EQUAL(to_block_string(bm), str);
  CHECK_EQUAL(bm.size(), 193u);
  // Fill up the dirty block and append another full block. This bumps the
  // clean count of the last marker to 2.
  bm.append_bits(true, 63);
  bm.append_bits(true, 64);
  str =
    "0000000000000000000000000000000000000000000000000000000000000010\n"
    "1111111111111111111111111111111111000000000000000000001111111111\n"
    "0011111111111111111111111111111111111111111111111111111010111111\n"
    "1000000000000000000000000000000100000000000000000000000000000000\n"
    "1111111111111111111111111111111111111111111111111111111111111111\n";
  REQUIRE_EQUAL(to_block_string(bm), str);
  // Now we'll add some 0 bits. We had a complete block left, so that make the
  // clean count of the last marker 3.
  bm.append_bits(false, 64);
  CHECK_EQUAL(bm.size(), 384u);
  // Add 15 clean blocks of 0, of which 14 get merged with the previous
  // marker and 1 remains a non-marker block. That yields a marker count of
  // 1111 (15).
  bm.append_bits(false, 64 * 15);
  str =
    "0000000000000000000000000000000000000000000000000000000000000010\n"
    "1111111111111111111111111111111111000000000000000000001111111111\n"
    "0011111111111111111111111111111111111111111111111111111010111111\n"
    "1000000000000000000000000000000110000000000000000000000000000000\n"
    "0000000000000000000000000000011110000000000000000000000000000000\n"
    "0000000000000000000000000000000000000000000000000000000000000000\n";
  REQUIRE_EQUAL(to_block_string(bm), str);
  CHECK_EQUAL(bm.size(), 384u + 64 * 15);
  // Now we're at the maximum number of new blocks with value 1. This
  // amounts to 64 * (2^32-1) = 274,877,906,880 bits in 2^32-2 blocks. Note
  // that the maximum value of a clean block is 2^32-1, but the invariant
  // requires the last block to be dirty, so we have to subtract yet another
  // block.
  bm.append_bits(true, 64ull * ((1ull << 32) - 1));
  // Appending a single bit here just triggers the coalescing of the last
  // block with the current marker, making the clean count have the maximum
  // value of 2^32-1.
  bm.append_bit(false);
  str =
    "0000000000000000000000000000000000000000000000000000000000000010\n"
    "1111111111111111111111111111111111000000000000000000001111111111\n"
    "0011111111111111111111111111111111111111111111111111111010111111\n"
    "1000000000000000000000000000000110000000000000000000000000000000\n"
    "0000000000000000000000000000100000000000000000000000000000000000\n"
    "1111111111111111111111111111111110000000000000000000000000000000\n"
    "                                                               0\n";
  REQUIRE_EQUAL(to_block_string(bm), str);
  CHECK_EQUAL(bm.size(), 1344 + 274877906880ull + 1);
  /// Complete the block as dirty.
  bm.append_bits(true, 63);
  /// Create another full dirty block, just so that we can check that the
  /// dirty counter works properly.
  for (auto i = 0; i < 64; ++i)
    bm.append_bit(i % 2 == 0);
  CHECK_EQUAL(bm.size(), 274877908352ull);
  // Now we add 2^3 full markers. Because the maximum clean count is 2^32-1,
  // we end up with 8 full markers and 7 clean blocks.
  bm.append_bits(false, (1ull << (32 + 3)) * 64);
  str =
    "0000000000000000000000000000000000000000000000000000000000000010\n"
    "1111111111111111111111111111111111000000000000000000001111111111\n"
    "0011111111111111111111111111111111111111111111111111111010111111\n"
    "1000000000000000000000000000000110000000000000000000000000000000\n"
    "0000000000000000000000000000100000000000000000000000000000000000\n"
    "1111111111111111111111111111111110000000000000000000000000000010\n"
    "1111111111111111111111111111111111111111111111111111111111111110\n"
    "0101010101010101010101010101010101010101010101010101010101010101\n"
    "0111111111111111111111111111111110000000000000000000000000000000\n"
    "0111111111111111111111111111111110000000000000000000000000000000\n"
    "0111111111111111111111111111111110000000000000000000000000000000\n"
    "0111111111111111111111111111111110000000000000000000000000000000\n"
    "0111111111111111111111111111111110000000000000000000000000000000\n"
    "0111111111111111111111111111111110000000000000000000000000000000\n"
    "0111111111111111111111111111111110000000000000000000000000000000\n"
    "0111111111111111111111111111111110000000000000000000000000000000\n"
    "0000000000000000000000000000001110000000000000000000000000000000\n"
    "0000000000000000000000000000000000000000000000000000000000000000\n";
  REQUIRE_EQUAL(to_block_string(bm), str);
  CHECK_EQUAL(bm.size(), 274877908352ull + 2199023255552ull);
  /// Adding another bit just consolidates the last clean block with the
  /// last marker.
  bm.append_bit(true);
  str =
    "0000000000000000000000000000000000000000000000000000000000000010\n"
    "1111111111111111111111111111111111000000000000000000001111111111\n"
    "0011111111111111111111111111111111111111111111111111111010111111\n"
    "1000000000000000000000000000000110000000000000000000000000000000\n"
    "0000000000000000000000000000100000000000000000000000000000000000\n"
    "1111111111111111111111111111111110000000000000000000000000000010\n"
    "1111111111111111111111111111111111111111111111111111111111111110\n"
    "0101010101010101010101010101010101010101010101010101010101010101\n"
    "0111111111111111111111111111111110000000000000000000000000000000\n"
    "0111111111111111111111111111111110000000000000000000000000000000\n"
    "0111111111111111111111111111111110000000000000000000000000000000\n"
    "0111111111111111111111111111111110000000000000000000000000000000\n"
    "0111111111111111111111111111111110000000000000000000000000000000\n"
    "0111111111111111111111111111111110000000000000000000000000000000\n"
    "0111111111111111111111111111111110000000000000000000000000000000\n"
    "0111111111111111111111111111111110000000000000000000000000000000\n"
    "0000000000000000000000000000010000000000000000000000000000000000\n"
    "                                                               1\n";
  REQUIRE_EQUAL(to_block_string(bm), str);
  CHECK_EQUAL(bm.size(), 2473901163905u);
  REQUIRE(bm == make_ewah1());
}

TEST(EWAH construction 2) {
  ewah_bitmap bm;
  bm.append_bit(false);
  bm.append_bit(true);
  bm.append_bits(false, 421);
  bm.append_bit(true);
  bm.append_bit(true);
  auto str =
    "0000000000000000000000000000000000000000000000000000000000000001\n"
    "0000000000000000000000000000000000000000000000000000000000000010\n"
    "0000000000000000000000000000001010000000000000000000000000000000\n"
    "                       11000000000000000000000000000000000000000\n";
  REQUIRE_EQUAL(to_block_string(bm), str);
  REQUIRE(bm == make_ewah2());
}

TEST(EWAH construction 3) {
  ewah_bitmap bm;
  bm.append_bits(true, 222);
  bm.append_bit(false);
  bm.append_bit(true);
  bm.append_bit(false);
  bm.append_block(0xcccccccccc);
  bm.append_bit(false);
  bm.append_bit(true);
  auto str =
    "1000000000000000000000000000000110000000000000000000000000000001\n"
    "1001100110011001100110011001100010111111111111111111111111111111\n"
    "                             10000000000000000000000000110011001\n";
  REQUIRE(to_block_string(bm), str);
  REQUIRE(bm == make_ewah3());
}

TEST(EWAH element access 1) {
  auto bm = make_ewah1();
  CHECK(bm[0]);
  CHECK(bm[9]);
  CHECK(!bm[10]);
  CHECK(bm[64]);
  CHECK(!bm[1024]);
  CHECK(bm[1344]);
  CHECK(bm[2473901163905 - 1]);
}

TEST(EWAH element access 2) {
  auto bm = make_ewah2();
  CHECK(!bm[0]);
  CHECK(bm[1]);
  CHECK(!bm[2]);
  CHECK(!bm[63]);
  CHECK(!bm[64]);
  CHECK(!bm[65]);
  CHECK(!bm[384]);
  CHECK(!bm[385]);
  CHECK(!bm[422]);
  CHECK(bm[423]);
  CHECK(bm[424]);
}

TEST(EWAH bitwise NOT) {
  ewah_bitmap bm;
  bm.append_bit(true);
  bm.append_bit(false);
  bm.append_bits(true, 30);
  bm.append_bit(false);
  ewah_bitmap comp;
  comp.append_bit(false);
  comp.append_bit(true);
  comp.append_bits(false, 30);
  comp.append_bit(true);
  auto str =
    "0000000000000000000000000000000000000000000000000000000000000000\n"
    "                               100000000000000000000000000000010\n";
  CHECK(~bm == comp);
  CHECK(bm == ~comp);
  CHECK(~~bm == bm);
  CHECK(to_block_string(~bm) == str);
  str =
    "0000000000000000000000000000000000000000000000000000000000000010\n"
    "0000000000000000000000000000000000111111111111111111110000000000\n"
    "1100000000000000000000000000000000000000000000000000000101000000\n"
    "0000000000000000000000000000000110000000000000000000000000000000\n"
    "1000000000000000000000000000100000000000000000000000000000000000\n"
    "0111111111111111111111111111111110000000000000000000000000000010\n"
    "0000000000000000000000000000000000000000000000000000000000000001\n"
    "1010101010101010101010101010101010101010101010101010101010101010\n"
    "1111111111111111111111111111111110000000000000000000000000000000\n"
    "1111111111111111111111111111111110000000000000000000000000000000\n"
    "1111111111111111111111111111111110000000000000000000000000000000\n"
    "1111111111111111111111111111111110000000000000000000000000000000\n"
    "1111111111111111111111111111111110000000000000000000000000000000\n"
    "1111111111111111111111111111111110000000000000000000000000000000\n"
    "1111111111111111111111111111111110000000000000000000000000000000\n"
    "1111111111111111111111111111111110000000000000000000000000000000\n"
    "1000000000000000000000000000010000000000000000000000000000000000\n"
    "                                                               0\n";
  CHECK_EQUAL(to_block_string(~make_ewah1()), str);
}

TEST(EWAH bitwise AND) {
  auto bm2 = make_ewah2();
  auto bm3 = make_ewah3();
  auto str =
      "0000000000000000000000000000000000000000000000000000000000000001\n"
      "0000000000000000000000000000000000000000000000000000000000000010\n"
      "0000000000000000000000000000001010000000000000000000000000000000\n"
      "                       00000000000000000000000000000000000000000\n";
  auto max_size = std::max(bm2.size(), bm3.size());
  CHECK_EQUAL(to_block_string(bm2 & bm3), str);
  CHECK((bm2 & bm3).size() == max_size);
  CHECK((bm3 & bm2).size() == max_size);
}

TEST(EWAH bitwise OR) {
  auto bm2 = make_ewah2();
  auto bm3 = make_ewah3();
  auto str =
    "1000000000000000000000000000000110000000000000000000000000000010\n"
    "1001100110011001100110011001100010111111111111111111111111111111\n"
    "0000000000000000000000000000010000000000000000000000000110011001\n"
    "0000000000000000000000000000000010000000000000000000000000000000\n"
    "                       11000000000000000000000000000000000000000\n";
  CHECK_EQUAL(to_block_string(bm2 | bm3), str);
}

TEST(EWAH bitwise XOR) {
  auto bm2 = make_ewah2();
  auto bm3 = make_ewah3();
  auto str =
    "0000000000000000000000000000000000000000000000000000000000000001\n"
    "1111111111111111111111111111111111111111111111111111111111111101\n"
    "1000000000000000000000000000000100000000000000000000000000000010\n"
    "1001100110011001100110011001100010111111111111111111111111111111\n"
    "0000000000000000000000000000010000000000000000000000000110011001\n"
    "0000000000000000000000000000000010000000000000000000000000000000\n"
    "                       11000000000000000000000000000000000000000\n";
  CHECK_EQUAL(to_block_string(bm2 ^ bm3), str);
}

TEST(EWAH bitwise NAND) {
  auto bm2 = make_ewah2();
  auto bm3 = make_ewah3();
  auto str =
    "0000000000000000000000000000001100000000000000000000000000000000\n"
    "                       11000000000000000000000000000000000000000\n";
  CHECK(to_block_string(bm2 - bm3), str);
}

TEST(EWAH block append) {
  ewah_bitmap bm;
  bm.append_bits(true, 10);
  bm.append_block(0xf00);
  CHECK_EQUAL(bm.size(), 10 + ewah_bitmap::word_type::width);
  CHECK(!bm[17]);
  CHECK(bm[18]);
  CHECK(bm[19]);
  CHECK(bm[20]);
  CHECK(bm[21]);
  CHECK(!bm[22]);
  bm.append_bits(true, 2048);
  bm.append_block(0xff00);
  auto str =
    "0000000000000000000000000000000000000000000000000000000000000010\n"
    "0000000000000000000000000000000000000000001111000000001111111111\n"
    "1111111111111111111111111111111111111111111111111111110000000000\n"
    "1000000000000000000000000000111110000000000000000000000000000001\n"
    "0000000000000000000000000000000000000011111111000000001111111111\n"
    "                                                      0000000000\n";
  CHECK_EQUAL(to_block_string(bm), str);
}

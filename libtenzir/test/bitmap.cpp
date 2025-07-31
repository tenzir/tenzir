//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/bitmap.hpp"

#include "tenzir/concept/printable/tenzir/bitmap.hpp"
#include "tenzir/concept/printable/to_string.hpp"
#include "tenzir/ewah_bitmap.hpp"
#include "tenzir/fbs/bitmap.hpp"
#include "tenzir/flatbuffer.hpp"
#include "tenzir/ids.hpp"
#include "tenzir/null_bitmap.hpp"
#include "tenzir/test/test.hpp"

using namespace tenzir;
using namespace std::string_literals;

namespace {

template <class Bitmap>
struct bitmap_test_harness {
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
    x.append_bit(false);
    x.append_block(0b0111000, 7);
    x.append_bits(true, 20);
    x.append_bit(true);
    x.append_block(0b0111000, 7);
    x.append_bits(true, 20);
    y.append_bits(true, 11);
    y.append_bits(false, 34);
    y.append_bit(true);
    y.append_bits(true, 6);
    auto s = to_string(x);
    CHECK_EQUAL(s, "00001110111111111111111111111000111011111111111111111111");
    s = to_string(y);
    CHECK_EQUAL(s, "1111111111100000000000000000000000000000000001111111");
    a.append_bit(false);
    a.append_bit(true);
    s = "01";
    CHECK_EQUAL(to_string(a), s);
    MESSAGE("longer sequence");
    a.append_bits(false, 421);
    s.append(421, '0');
    CHECK_EQUAL(to_string(a), s);
    a.append_bit(true);
    a.append_bit(true);
    s += "11";
    CHECK_EQUAL(to_string(a), s);
    CHECK_EQUAL(a.size(), 425u);
    std::string str;
    CHECK_EQUAL(to_string(a), s);
    s.clear();
    b.append_bits(true, 222);
    s.append(222, '1');
    CHECK_EQUAL(to_string(b), s);
    b.append_bit(false);
    b.append_bit(true);
    b.append_bit(false);
    s += "010";
    CHECK_EQUAL(to_string(b), s);
    b.append_block(0x000000cccccccccc);
    s += "0011001100110011001100110011001100110011000000000000000000000000";
    CHECK_EQUAL(to_string(b), s);
    b.append_bit(false);
    b.append_bit(true);
    s += "01";
    CHECK_EQUAL(to_string(b), s);
    auto xy = x;
    xy.append(y);
    s = "00001110111111111111111111111000111011111111111111111111"
        "1111111111100000000000000000000000000000000001111111";
    CHECK_EQUAL(to_string(xy), s);
  }

  void test_bitwise_simple() {
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
    auto zeros = Bitmap{bm1.size(), 0};
    CHECK_EQUAL(bm1 & Bitmap{}, zeros);
    CHECK_EQUAL(Bitmap{} & bm1, zeros);
  }

  void test_bitwise_or() {
    MESSAGE("bitwise OR");
    Bitmap bm1, bm2;
    bm1.append_bits(true, 50);
    bm2.append_bits(false, 50);
    bm2.append_bits(true, 50);
    CHECK_EQUAL(to_string(bm1 | bm2), std::string(100, '1'));
    CHECK_EQUAL(to_string(bm2 | bm1), std::string(100, '1'));
    CHECK_EQUAL(to_string(bm1 | Bitmap{}), to_string(bm1));
    CHECK_EQUAL(to_string(Bitmap{} | bm1), to_string(bm1));
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
    CHECK_EQUAL(to_string(bm1 - Bitmap{}), to_string(bm1));
    str = std::string(std::max(bm1.size(), bm2.size()), '0');
    CHECK_EQUAL(to_string(Bitmap{} - bm1), str);
  }

  void test_bitwise_nary() {
    MESSAGE("nary AND");
    Bitmap z0;
    z0.append_bits(false, 30);
    z0.append_bits(true, 30);
    Bitmap z1;
    z1.append_bits(false, 20);
    z1.append_bits(true, 50);
    auto bitmaps = std::vector<Bitmap>{x, y, z0, z1};
    auto begin = bitmaps.begin();
    auto end = bitmaps.end();
    CHECK_EQUAL(nary_and(begin, end), x & y & z0 & z1);
  }

  void test_rank() {
    MESSAGE("rank");
    Bitmap bm;
    CHECK_EQUAL(rank<0>(bm), 0u);
    CHECK_EQUAL(rank<1>(bm), 0u);
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
    CHECK_EQUAL(rank<0>(bm, 0), 0u);
    CHECK_EQUAL(rank<1>(bm, 0), 1u);
    CHECK_EQUAL(rank<0>(bm, 1), 1u);
    CHECK_EQUAL(rank<1>(bm, 1), 1u);
    CHECK_EQUAL(rank<0>(bm, 10), 1u);
    CHECK_EQUAL(rank<1>(bm, 10), 10u);
    CHECK_EQUAL(rank<0>(bm, 10), 1u);
    CHECK_EQUAL(rank<0>(bm, bm.size() - 1), 368u);
    CHECK_EQUAL(rank<1>(bm, bm.size() - 1), 575u);
    CHECK_EQUAL(rank<0>(bm, bm.size() - 2), 367u);
    CHECK_EQUAL(rank<1>(bm, bm.size() - 2), 575u);
    MESSAGE("partial rank - special cases");
    Bitmap bm2;
    bm2.append_bit(true);
    CHECK_EQUAL(rank<0>(bm2, 0), 0u);
    CHECK_EQUAL(rank<1>(bm2, 0), 1u);
    bm2.append_bit(false);
    CHECK_EQUAL(rank<0>(bm2, 1), 1u);
    CHECK_EQUAL(rank<1>(bm2, 1), 1u);
    Bitmap bm3;
    bm3.append_bit(false);
    CHECK_EQUAL(rank<0>(bm3, 0), 1u);
    CHECK_EQUAL(rank<1>(bm3, 0), 0u);
    bm3.append_bit(true);
    CHECK_EQUAL(rank<0>(bm3, 1), 1u);
    CHECK_EQUAL(rank<1>(bm3, 1), 1u);
  }

  void test_select() {
    MESSAGE("select - one-shot");
    CHECK_EQUAL(select<0>(b, 1), 222u);
    CHECK_EQUAL(select<0>(b, 2), 224u);
    CHECK_EQUAL(select<0>(b, 3), 225u);
    CHECK_EQUAL(select<0>(b, 4), 226u);
    CHECK_EQUAL(select<1>(b, 1), 0u);
    CHECK_EQUAL(select<1>(b, 100), 99u);
    CHECK_EQUAL(select<1>(b, 222), 221u);
    CHECK_EQUAL(select<1>(b, 223), 223u);
    CHECK_EQUAL(select<1>(b, 224), 227u);
    auto r = rank<1>(b);
    auto last = select<1>(b, r);
    CHECK_EQUAL(last, b.size() - 1);
    MESSAGE("select - maximum");
    CHECK_EQUAL(select<1>(b, -1), last);
    MESSAGE("select_range - increment");
    auto n = size_t{0};
    for (auto i : select(b)) {
      ++n;
      if (n == 1)
        CHECK_EQUAL(i, 0u);
      else if (n == 100)
        CHECK_EQUAL(i, 99u);
      else if (n == 222)
        CHECK_EQUAL(i, 221u);
      else if (n == 223)
        CHECK_EQUAL(i, 223u);
      else if (n == 224)
        CHECK_EQUAL(i, 227u);
      else if (n == r)
        CHECK_EQUAL(i, b.size() - 1);
    }
    CHECK_EQUAL(r, n);
    MESSAGE("bitwise_range::select(n)");
    auto rng = each(b);
    CHECK_EQUAL(rng.get(), 0u);
    rng.select(100); // #101
    REQUIRE(rng);
    CHECK_EQUAL(rng.get(), 100u);
    rng.select(122); // #101 + #122 = #223
    REQUIRE(rng);
    CHECK_EQUAL(rng.get(), 223u);
    rng.select(r - 223); // last one
    REQUIRE(rng);
    CHECK_EQUAL(rng.get(), last);
    rng.select(42); // nothing left
    CHECK(!rng);
    MESSAGE("bitwise_range::next(n)");
    rng = each(b);
    rng.next(b.size() - 1); // start at 0, then go to last bit.
    REQUIRE(rng);
    CHECK_EQUAL(rng.get(), b.size() - 1);
    rng = each(b);
    rng.next(225); // Position 225 has a 0-bit, the next 1-bit is at 227.
    REQUIRE(rng);
    CHECK_EQUAL(rng.get(), 225u);
    rng.select();
    REQUIRE(rng);
    CHECK_EQUAL(rng.get(), 227u);
    rng = each(b);
    rng.next(1024); // out of range
    CHECK(!rng);
    MESSAGE("bitwise_range::select_from(x)");
    rng = each(b);
    rng.select_from(225);
    REQUIRE(rng);
    CHECK_EQUAL(rng.get(), 227u);
  }

  void test_select_with() {
    MESSAGE("select_with");
    using half_open_interval = std::pair<id, id>;
    using intervals = std::vector<half_open_interval>;
    auto xs = intervals{
      {0, 10}, {10, 20}, {30, 40}, {40, 50}, {80, 90},
    };
    auto identity = [](auto x) {
      return x;
    };
    intervals ys;
    auto g = [&](auto& x) {
      ys.push_back(x);
      return caf::none;
    };
    // The very first ID.
    auto err = select_with(make_ids({0}), xs.begin(), xs.end(), identity, g);
    CHECK(!err);
    CHECK_EQUAL(ys, (intervals{{0, 10}}));
    ys.clear();
    // An intermediate ID in the first interval.
    err = select_with(make_ids({5}), xs.begin(), xs.end(), identity, g);
    CHECK(!err);
    CHECK_EQUAL(ys, (intervals{{0, 10}}));
    ys.clear();
    // The last ID in the first interval.
    err = select_with(make_ids({10}), xs.begin(), xs.end(), identity, g);
    CHECK(!err);
    CHECK_EQUAL(ys, (intervals{{10, 20}}));
    ys.clear();
    // The first ID in an intermediate interval.
    err = select_with(make_ids({30}), xs.begin(), xs.end(), identity, g);
    CHECK(!err);
    CHECK_EQUAL(ys, (intervals{{30, 40}}));
    ys.clear();
    // An intermediate ID in an intermediate interval.
    err = select_with(make_ids({42}), xs.begin(), xs.end(), identity, g);
    CHECK(!err);
    CHECK_EQUAL(ys, (intervals{{40, 50}}));
    ys.clear();
    // The last ID in an intermediate interval.
    err = select_with(make_ids({50}), xs.begin(), xs.end(), identity, g);
    CHECK(!err);
    CHECK(ys.empty());
    ys.clear();
    // An ID outside of the interval range.
    err = select_with(make_ids({100}), xs.begin(), xs.end(), identity, g);
    CHECK(!err);
    CHECK(ys.empty());
    ys.clear();
    // Multiple IDs in the first interval.
    err = select_with(make_ids({0, 1, 2}), xs.begin(), xs.end(), identity, g);
    CHECK(!err);
    CHECK_EQUAL(ys, (intervals{{0, 10}}));
    ys.clear();
    // Multiple IDs in several intervals.
    err = select_with(make_ids({5, 10, 42}), xs.begin(), xs.end(), identity, g);
    CHECK(!err);
    CHECK_EQUAL(ys, (intervals{{0, 10}, {10, 20}, {40, 50}}));
  }

  void test_frame() {
    MESSAGE("frame");
    // Empty bitmap.
    auto npos_pair
      = std::make_pair(Bitmap::word_type::npos, Bitmap::word_type::npos);
    CHECK_EQUAL(frame<0>(Bitmap{}), npos_pair);
    CHECK_EQUAL(frame<1>(Bitmap{}), npos_pair);
    Bitmap bm1;
    bm1.template append<1>(100);
    bm1.template append<0>();
    bm1.template append<1>();
    bm1.template append<0>(200);
    bm1.template append<1>();
    bm1.template append<1>(1000);
    bm1.template append<0>(500);
    Bitmap bm2;
    bm2.template append<1>(10);
    bm2.template append<0>();
    bm2.template append<1>(500);
    auto s0 = frame<0>(bm1);
    CHECK_EQUAL(s0.first, 100u);
    CHECK_EQUAL(s0.second, bm1.size() - 1);
    auto s1 = frame<1>(bm1);
    CHECK_EQUAL(s1.first, 0u);
    CHECK_EQUAL(s1.second, bm1.size() - 500 - 1);
    s0 = frame<0>(bm2);
    CHECK_EQUAL(s0.first, s0.second);
    CHECK_EQUAL(s0.first, 10u);
    s1 = frame<1>(bm2);
    CHECK_EQUAL(s1.first, 0u);
    CHECK_EQUAL(s1.second, bm2.size() - 1);
  }

  void test_all() {
    MESSAGE("all");
    CHECK(all<0>(Bitmap{}));
    CHECK(all<1>(Bitmap{}));
    CHECK(!all<0>(a));
    CHECK(!all<0>(b));
    CHECK(!all<1>(a));
    CHECK(!all<1>(b));
    CHECK(all<0>(Bitmap{10, false}));
    CHECK(all<0>(Bitmap{1000, false}));
    CHECK(!all<0>(Bitmap{10, true}));
    CHECK(!all<0>(Bitmap{1000, true}));
    CHECK(all<1>(Bitmap{10, true}));
    CHECK(all<1>(Bitmap{1000, true}));
    CHECK(!all<1>(Bitmap{10, false}));
    CHECK(!all<1>(Bitmap{1000, false}));
  }

  void test_any() {
    MESSAGE("any");
    CHECK(!any<0>(Bitmap{}));
    CHECK(!any<1>(Bitmap{}));
    CHECK(any<0>(a));
    CHECK(any<0>(b));
    CHECK(any<1>(a));
    CHECK(any<1>(b));
    CHECK(any<0>(Bitmap{10, false}));
    CHECK(any<0>(Bitmap{1000, false}));
    CHECK(!any<0>(Bitmap{10, true}));
    CHECK(!any<0>(Bitmap{1000, true}));
    CHECK(any<1>(Bitmap{10, true}));
    CHECK(any<1>(Bitmap{1000, true}));
    CHECK(!any<1>(Bitmap{10, false}));
    CHECK(!any<1>(Bitmap{1000, false}));
  }

  void test_flatbuffers() {
    for (const auto& ref : {a, b, x, y}) {
      auto builder = flatbuffers::FlatBufferBuilder{};
      const auto bm_offset = pack(builder, bitmap{ref});
      builder.Finish(bm_offset);
      auto fb = unbox(flatbuffer<fbs::Bitmap>::make(builder.Release()));
      REQUIRE(fb);
      auto bm = bitmap{};
      REQUIRE_EQUAL(unpack(*fb, bm), caf::none);
      CHECK_EQUAL(ref, bm);
    }
  }

  void execute() {
    CHECK(x.empty());
    CHECK(y.empty());
    CHECK_EQUAL(x.size(), 0u);
    CHECK_EQUAL(y.size(), 0u);
    test_append();
    test_construction();
    test_bitwise_simple();
    test_bitwise_and();
    test_bitwise_or();
    test_bitwise_nand();
    test_bitwise_nary();
    test_rank();
    test_select();
    test_select_with();
    test_frame();
    test_all();
    test_any();
    test_flatbuffers();
  }

  Bitmap a;
  Bitmap b;
  Bitmap x;
  Bitmap y;
};

} // namespace

WITH_FIXTURE(bitmap_test_harness<null_bitmap>) {
  TEST("null_bitmap") {
    execute();
  }
}

WITH_FIXTURE(bitmap_test_harness<ewah_bitmap>) {
  TEST("ewah_bitmap") {
    execute();
  }
}

WITH_FIXTURE(bitmap_test_harness<wah_bitmap>) {
  TEST("wah_bitmap") {
    execute();
  }
}

WITH_FIXTURE(bitmap_test_harness<bitmap>) {
  TEST("bitmap") {
    execute();
  }
}

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
  for (auto i = 0; i < 64; ++i) {
    bm.append_bit(i % 2 == 0);
  }
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

std::string to_block_string(const ewah_bitmap& bm) {
  using word_type = ewah_bitmap::word_type;
  std::string str;
  if (bm.blocks().empty()) {
    return str;
  }
  auto last = bm.blocks().end() - 1;
  auto partial = bm.size() % word_type::width;
  if (partial == 0) {
    ++last;
  }
  for (auto i = bm.blocks().begin(); i != last; ++i) {
    for (auto b = 0u; b < word_type::width; ++b) {
      str += word_type::test(*i, word_type::width - b - 1) ? '1' : '0';
    }
    str += '\n';
  }
  if (partial > 0) {
    str.append(word_type::width - partial, ' ');
    for (auto b = 0u; b < partial; ++b) {
      str += word_type::test(*last, partial - b - 1) ? '1' : '0';
    }
    str += '\n';
  }
  return str;
}

} // namespace

TEST("EWAH construction 1") {
  ewah_bitmap bm;
  bm.append_bits(true, 10);
  bm.append_bits(false, 20);
  auto str
    = "0000000000000000000000000000000000000000000000000000000000000000\n"
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
  str = "0000000000000000000000000000000000000000000000000000000000000010\n"
        "1111111111111111111111111111111111000000000000000000001111111111\n"
        "0011111111111111111111111111111111111111111111111111111010111111\n"
        "1111111111111111111111111111111111111111111111111111111111111111\n";
  REQUIRE_EQUAL(to_block_string(bm), str);
  // Appending anything now transforms the last block into a marker, because
  // it it turns out it was all 1s.
  bm.append_bit(true);
  str = "0000000000000000000000000000000000000000000000000000000000000010\n"
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
  str = "0000000000000000000000000000000000000000000000000000000000000010\n"
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
  str = "0000000000000000000000000000000000000000000000000000000000000010\n"
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
  str = "0000000000000000000000000000000000000000000000000000000000000010\n"
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
  for (auto i = 0; i < 64; ++i) {
    bm.append_bit(i % 2 == 0);
  }
  CHECK_EQUAL(bm.size(), 274877908352ull);
  // Now we add 2^3 full markers. Because the maximum clean count is 2^32-1,
  // we end up with 8 full markers and 7 clean blocks.
  bm.append_bits(false, (1ull << (32 + 3)) * 64);
  str = "0000000000000000000000000000000000000000000000000000000000000010\n"
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
  str = "0000000000000000000000000000000000000000000000000000000000000010\n"
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

TEST("EWAH construction 2") {
  ewah_bitmap bm;
  bm.append_bit(false);
  bm.append_bit(true);
  bm.append_bits(false, 421);
  bm.append_bit(true);
  bm.append_bit(true);
  auto str
    = "0000000000000000000000000000000000000000000000000000000000000001\n"
      "0000000000000000000000000000000000000000000000000000000000000010\n"
      "0000000000000000000000000000001010000000000000000000000000000000\n"
      "                       11000000000000000000000000000000000000000\n";
  REQUIRE_EQUAL(to_block_string(bm), str);
  REQUIRE(bm == make_ewah2());
}

TEST("EWAH construction 3") {
  ewah_bitmap bm;
  bm.append_bits(true, 222);
  bm.append_bit(false);
  bm.append_bit(true);
  bm.append_bit(false);
  bm.append_block(0xcccccccccc);
  bm.append_bit(false);
  bm.append_bit(true);
  auto str
    = "1000000000000000000000000000000110000000000000000000000000000001\n"
      "1001100110011001100110011001100010111111111111111111111111111111\n"
      "                             10000000000000000000000000110011001\n";
  REQUIRE_EQUAL(to_block_string(bm), str);
  REQUIRE(bm == make_ewah3());
}

TEST("EWAH element access 1") {
  auto bm = make_ewah1();
  CHECK(bm[0]);
  CHECK(bm[9]);
  CHECK(! bm[10]);
  CHECK(bm[64]);
  CHECK(! bm[1024]);
  CHECK(bm[1344]);
  CHECK(bm[2473901163905 - 1]);
}

TEST("EWAH element access 2") {
  auto bm = make_ewah2();
  CHECK(! bm[0]);
  CHECK(bm[1]);
  CHECK(! bm[2]);
  CHECK(! bm[63]);
  CHECK(! bm[64]);
  CHECK(! bm[65]);
  CHECK(! bm[384]);
  CHECK(! bm[385]);
  CHECK(! bm[422]);
  CHECK(bm[423]);
  CHECK(bm[424]);
}

TEST("EWAH bitwise NOT") {
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
  auto str
    = "0000000000000000000000000000000000000000000000000000000000000000\n"
      "                               100000000000000000000000000000010\n";
  CHECK(~bm == comp);
  CHECK(bm == ~comp);
  CHECK(~~bm == bm);
  CHECK(to_block_string(~bm) == str);
  str = "0000000000000000000000000000000000000000000000000000000000000010\n"
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
  auto complete_block = ewah_bitmap{};
  complete_block.append<1>(63);
  complete_block.append<0>();
  auto complement = ewah_bitmap{};
  complement.append<0>(63);
  complement.append<1>();
  CHECK_EQUAL(~complete_block, complement);
}

TEST("EWAH bitwise AND") {
  auto bm2 = make_ewah2();
  auto bm3 = make_ewah3();
  auto str
    = "0000000000000000000000000000000000000000000000000000000000000001\n"
      "0000000000000000000000000000000000000000000000000000000000000010\n"
      "0000000000000000000000000000001010000000000000000000000000000000\n"
      "                       00000000000000000000000000000000000000000\n";
  auto max_size = std::max(bm2.size(), bm3.size());
  CHECK_EQUAL(to_block_string(bm2 & bm3), str);
  CHECK((bm2 & bm3).size() == max_size);
  CHECK((bm3 & bm2).size() == max_size);
}

TEST("EWAH bitwise OR") {
  auto bm2 = make_ewah2();
  auto bm3 = make_ewah3();
  auto expected = make_ewah3();
  expected.append<0>(bm2.size() - bm3.size() - 2);
  expected.append<1>(2);
  CHECK_EQUAL((bm2 | bm3), expected);
  CHECK_EQUAL((bm3 | bm2), expected);
  auto str
    = "1000000000000000000000000000000110000000000000000000000000000010\n"
      "1001100110011001100110011001100010111111111111111111111111111111\n"
      "0000000000000000000000000000010000000000000000000000000110011001\n"
      "0000000000000000000000000000000010000000000000000000000000000000\n"
      "                       11000000000000000000000000000000000000000\n";
  CHECK_EQUAL(to_block_string(bm2 | bm3), str);
  CHECK_EQUAL(to_block_string(bm3 | bm2), str);
}

TEST("EWAH bitwise XOR") {
  auto bm2 = make_ewah2();
  auto bm3 = make_ewah3();
  auto str
    = "0000000000000000000000000000000000000000000000000000000000000001\n"
      "1111111111111111111111111111111111111111111111111111111111111101\n"
      "1000000000000000000000000000000100000000000000000000000000000010\n"
      "1001100110011001100110011001100010111111111111111111111111111111\n"
      "0000000000000000000000000000010000000000000000000000000110011001\n"
      "0000000000000000000000000000000010000000000000000000000000000000\n"
      "                       11000000000000000000000000000000000000000\n";
  CHECK_EQUAL(to_block_string(bm2 ^ bm3), str);
}

TEST("EWAH bitwise NAND") {
  auto bm2 = make_ewah2();
  auto bm3 = make_ewah3();
  auto str
    = "0000000000000000000000000000001100000000000000000000000000000000\n"
      "                       11000000000000000000000000000000000000000\n";
  CHECK_EQUAL(to_block_string(bm2 - bm3), str);
}

TEST("EWAH block append") {
  ewah_bitmap bm;
  bm.append_bits(true, 10);
  bm.append_block(0xf00);
  CHECK_EQUAL(bm.size(), 10 + ewah_bitmap::word_type::width);
  CHECK(! bm[17]);
  CHECK(bm[18]);
  CHECK(bm[19]);
  CHECK(bm[20]);
  CHECK(bm[21]);
  CHECK(! bm[22]);
  bm.append_bits(true, 2048);
  bm.append_block(0xff00);
  auto str
    = "0000000000000000000000000000000000000000000000000000000000000010\n"
      "0000000000000000000000000000000000000000001111000000001111111111\n"
      "1111111111111111111111111111111111111111111111111111110000000000\n"
      "1000000000000000000000000000111110000000000000000000000000000001\n"
      "0000000000000000000000000000000000000011111111000000001111111111\n"
      "                                                      0000000000\n";
  CHECK_EQUAL(to_block_string(bm), str);
}

TEST("EWAH RLE print 1") {
  ewah_bitmap bm;
  bm.append_bit(false);
  bm.append_block(0b0111000, 7);
  bm.append_bits(true, 20);
  bm.append_bit(true);
  bm.append_block(0b0111000, 7);
  bm.append_bits(true, 20);
  std::string str;
  printers::bitmap<ewah_bitmap, policy::rle>(str, bm);
  CHECK_EQUAL(str, "4F3T1F21T3F3T1F20T");
}

TEST("EWAH RLE print 2") {
  ewah_bitmap bm;
  bm.append_bit(false);
  bm.append_bit(true);
  bm.append_bits(false, 421);
  bm.append_bit(true);
  bm.append_bit(true);
  std::string str;
  printers::bitmap<ewah_bitmap, policy::rle>(str, bm);
  // TODO: we could change the filter so that consecutive runs never occur,
  // even at word boundaries.
  // CHECK_EQUAL(str, "1F1T421F2T");
  CHECK_EQUAL(str, "1F1T62F320F39F2T");
}

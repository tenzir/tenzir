#include "test.h"
#include "vast/convert.h"
#include "vast/bitstream.h"
#include "vast/io/serialization.h"

using namespace vast;

BOOST_AUTO_TEST_CASE(polymorphic_bitstream)
{
  bitstream empty;
  BOOST_CHECK(! empty);

  bitstream x{null_bitstream{}}, y;
  BOOST_REQUIRE(x);
  BOOST_CHECK(x.append(3, true));
  BOOST_CHECK_EQUAL(x.size(), 3);
  BOOST_CHECK_EQUAL(to_string(x),  "111");

  std::vector<uint8_t> buf;
  io::archive(buf, x);
  io::unarchive(buf, y);
  BOOST_CHECK_EQUAL(y.size(), 3);
  BOOST_CHECK_EQUAL(to_string(y),  "111");
}

BOOST_AUTO_TEST_CASE(null_bitstream_operations)
{
  null_bitstream x;
  BOOST_REQUIRE(x.append(3, true));
  BOOST_REQUIRE(x.append(7, false));
  BOOST_REQUIRE(x.push_back(true));
  BOOST_CHECK_EQUAL(to_string(x),  "11100000001");
  BOOST_CHECK_EQUAL(to_string(~x), "00011111110");

  null_bitstream y;
  BOOST_REQUIRE(y.append(2, true));
  BOOST_REQUIRE(y.append(4, false));
  BOOST_REQUIRE(y.append(3, true));
  BOOST_REQUIRE(y.push_back(false));
  BOOST_REQUIRE(y.push_back(true));
  BOOST_CHECK_EQUAL(to_string(y),  "11000011101");
  BOOST_CHECK_EQUAL(to_string(~y), "00111100010");

  BOOST_CHECK_EQUAL(to_string(x & y), "11000000001");
  BOOST_CHECK_EQUAL(to_string(x | y), "11100011101");
  BOOST_CHECK_EQUAL(to_string(x ^ y), "00100011100");
  BOOST_CHECK_EQUAL(to_string(x - y), "00100000000");
  BOOST_CHECK_EQUAL(to_string(y - x), "00000011100");

  std::vector<null_bitstream> v;
  v.push_back(x);
  v.push_back(y);
  v.emplace_back(x - y);

  // The original vector contains the following (from LSB to MSB):
  // 11100000001
  // 11000011101
  // 00100000000
  std::string str;
  auto t = transpose(v);
  for (auto& i : t)
    str += to_string(i);
  BOOST_CHECK_EQUAL(
      str,
      "110"
      "110"
      "101"
      "000"
      "000"
      "000"
      "010"
      "010"
      "010"
      "000"
      "110"
      );
}

BOOST_AUTO_TEST_CASE(ewah_bitstream_operations)
{
  ewah_bitstream ewah;
  ewah.append(10, true);
  ewah.append(20, false);

  // Cause the first dirty block to overflow and bumps the dirty counter of
  // the first marker to 1.
  ewah.append(40, true);

  // Fill up another dirty block.
  ewah.push_back(false);
  ewah.push_back(true);
  ewah.push_back(false);
  ewah.append(53, true);
  ewah.push_back(false);
  ewah.push_back(false);

  BOOST_CHECK_EQUAL(ewah.size(), 128);

  // Bump the dirty count to 2 and fill up the current dirty block.
  ewah.push_back(true);
  ewah.append(63, true);

  // Appending anything now transforms the last block into a marker, because it
  // it turns out it was all 1s.
  ewah.push_back(true);

  auto str =
    "0000000000000000000000000000000000000000000000000000000000000001\n"
    "1111111111111111111111111111111111000000000000000000001111111111\n"
    "0011111111111111111111111111111111111111111111111111111010111111\n"
    "1000000000000000000000000000000010000000000000000000000000000001\n"
    "                                                               1";

  BOOST_CHECK_EQUAL(to_string(ewah), str);
  BOOST_CHECK_EQUAL(ewah.size(), 193);

  // Fill up the dirty block and append another full block. This bumps the
  // clean count of the last marker to 2.
  ewah.append(63, true);
  ewah.append(64, true);

  // Now we'll add some 0 bits. We had a complete block left, so that make the
  // clean count of the last marker 3.
  ewah.append(64, false);

  BOOST_CHECK_EQUAL(ewah.size(), 384);

  // Add 15 clean blocks of 0, of which 14 get merged with the previous marker
  // and 1 remains a non-marker block. That yields a marker count of 1111 (15).
  ewah.append(64 * 15, false);

  str =
    "0000000000000000000000000000000000000000000000000000000000000001\n"
    "1111111111111111111111111111111111000000000000000000001111111111\n"
    "0011111111111111111111111111111111111111111111111111111010111111\n"
    "1000000000000000000000000000000110000000000000000000000000000000\n"
    "0000000000000000000000000000011110000000000000000000000000000001\n"
    "0000000000000000000000000000000000000000000000000000000000000000";

  BOOST_CHECK_EQUAL(to_string(ewah), str);
  BOOST_CHECK_EQUAL(ewah.size(), 384 + 64 * 15);


  // Now we're add the maximum number of new blocks with value 1. This amounts
  // to 64 * (2^32-1) = 274,877,906,880 bits in 2^32-2 blocks. Note that the
  // maximum value of a clean block is 2^32-1, but the invariant requires the
  // last block to be dirty, so we have to subtract yet another block.
  ewah.append(64ull * ((1ull << 32) - 1), true);

  // Appending a single bit here just triggers the coalescing of the last
  // block with the current marker, making the clean count have the maximum
  // value of 2^32-1.
  ewah.push_back(false);

  str =
    "0000000000000000000000000000000000000000000000000000000000000001\n"
    "1111111111111111111111111111111111000000000000000000001111111111\n"
    "0011111111111111111111111111111111111111111111111111111010111111\n"
    "1000000000000000000000000000000110000000000000000000000000000000\n"
    "0000000000000000000000000000100000000000000000000000000000000000\n"
    "1111111111111111111111111111111110000000000000000000000000000000\n"
    "                                                               0";

  BOOST_CHECK_EQUAL(to_string(ewah), str);
  BOOST_CHECK_EQUAL(ewah.size(), 1344 + 274877906880ull + 1);

  /// Complete the block as dirty.
  ewah.append(63, true);

  /// Create another full dirty block, just so that we can check that the dirty
  /// counter works properly.
  for (auto i = 0; i < 64; ++i)
    ewah.push_back(i % 2 == 0);

  BOOST_CHECK_EQUAL(ewah.size(), 274877908352ull);

  // Now we add 2^3 full markers. Because the maximum clean count is 2^32-1, we
  // end up with 8 full markers and 7 clean blocks.
  ewah.append((1ull << (32 + 3)) * 64, false);

  str =
    "0000000000000000000000000000000000000000000000000000000000000001\n"
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
    "0000000000000000000000000000000000000000000000000000000000000000";

  BOOST_CHECK_EQUAL(to_string(ewah), str);
  BOOST_CHECK_EQUAL(ewah.size(), 274877908352ull + 2199023255552ull);

  /// Adding another bit just consolidates the last clean block with the
  /// last marker.
  ewah.push_back(true);

  str =
    "0000000000000000000000000000000000000000000000000000000000000001\n"
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
    "                                                               1";

  BOOST_CHECK_EQUAL(to_string(ewah), str);
  BOOST_CHECK_EQUAL(ewah.size(), 2473901163905);
}

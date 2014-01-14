#include "test.h"
#include "vast/bitmap.h"
#include "vast/io/serialization.h"

using namespace vast;

BOOST_AUTO_TEST_CASE(basic_bitmap)
{
  bitmap<int, null_bitstream> bm, bm2;
  BOOST_REQUIRE(bm.push_back(42));
  BOOST_REQUIRE(bm.push_back(84));
  BOOST_REQUIRE(bm.push_back(42));
  BOOST_REQUIRE(bm.push_back(21));
  BOOST_REQUIRE(bm.push_back(30));

  BOOST_CHECK_EQUAL(to_string(*bm[21]), "00010");

  BOOST_CHECK_EQUAL(to_string(*bm[30]), "00001");
  BOOST_CHECK_EQUAL(to_string(*bm[42]), "10100");
  BOOST_CHECK_EQUAL(to_string(*bm[84]), "01000");
  BOOST_CHECK(! bm[39]);

  BOOST_CHECK_EQUAL(to_string(*bm.lookup(not_equal, 21)), "11101");
  BOOST_CHECK_EQUAL(to_string(*bm.lookup(not_equal, 30)), "11110");
  BOOST_CHECK_EQUAL(to_string(*bm.lookup(not_equal, 42)), "01011");
  BOOST_CHECK_EQUAL(to_string(*bm.lookup(not_equal, 84)), "10111");
  BOOST_CHECK(bm.lookup(not_equal, 13));
  BOOST_CHECK_EQUAL(to_string(*bm.lookup(not_equal, 13)), "11111");

  BOOST_CHECK(bm.append(5, false));
  BOOST_CHECK_EQUAL(bm.size(), 10);

  std::vector<uint8_t> buf;
  io::archive(buf, bm);
  io::unarchive(buf, bm2);
  // The default bitmap storage is unordered, so the the following commented
  // check may fail due to different underlying hash tables. However, the
  // bitmaps should still be equal.
  //BOOST_CHECK_EQUAL(to_string(bm), to_string(bm2));
  BOOST_CHECK_EQUAL(bm, bm2);
  BOOST_CHECK_EQUAL(bm.size(), bm2.size());
  BOOST_CHECK_EQUAL(to_string(*bm[21]), to_string(*bm2[21]));
  BOOST_CHECK_EQUAL(to_string(*bm[30]), to_string(*bm2[30]));
  BOOST_CHECK_EQUAL(to_string(*bm[42]), to_string(*bm2[42]));
  BOOST_CHECK_EQUAL(to_string(*bm[84]), to_string(*bm2[84]));
}

BOOST_AUTO_TEST_CASE(range_coding)
{
  range_bitslice_coder<uint8_t, null_bitstream> r;

  // Some manual tests first.
  BOOST_REQUIRE(r.encode(0));
  BOOST_REQUIRE(r.encode(6));
  BOOST_REQUIRE(r.encode(9));
  BOOST_REQUIRE(r.encode(77));
  BOOST_REQUIRE(r.encode(255));
  BOOST_REQUIRE(r.encode(254));

  //r.each(
  //    [&](size_t, uint8_t x, null_bitstream const& bs)
  //    {
  //      std::cout << (uint64_t)x << "\t" << bs << std::endl;
  //    });

  BOOST_CHECK_EQUAL(to_string(*r.decode(0, less_equal)), "100000");
  BOOST_CHECK_EQUAL(to_string(*r.decode(8, less_equal)), "110000");
  BOOST_CHECK_EQUAL(to_string(*r.decode(9, less_equal)), "111000");
  BOOST_CHECK_EQUAL(to_string(*r.decode(10, less_equal)), "111000");
  BOOST_CHECK_EQUAL(to_string(*r.decode(100, less_equal)), "111100");
  BOOST_CHECK_EQUAL(to_string(*r.decode(255, less_equal)), "111111");
  BOOST_CHECK_EQUAL(to_string(*r.decode(254, less_equal)), "111101");

  r = decltype(r){};

  for (size_t i = 0; i < 256; ++i)
    BOOST_CHECK(r.encode(i));

  BOOST_CHECK(r.size() == 256);

  std::string str(256, '0');
  for (size_t i = 0; i < 256; ++i)
  {
    str[i] = '1';
    BOOST_CHECK_EQUAL(to_string(*r.decode(i, less_equal)), str);
  }
}

BOOST_AUTO_TEST_CASE(range_encoded_bitmap)
{
  bitmap<int8_t, null_bitstream, range_bitslice_coder> bm, bm2;
  BOOST_REQUIRE(bm.push_back(42));
  BOOST_REQUIRE(bm.push_back(84));
  BOOST_REQUIRE(bm.push_back(42));
  BOOST_REQUIRE(bm.push_back(21));
  BOOST_REQUIRE(bm.push_back(30));

  BOOST_CHECK_EQUAL(to_string(*bm.lookup(not_equal, 13)), "11111");
  BOOST_CHECK_EQUAL(to_string(*bm.lookup(not_equal, 42)), "01011");
  BOOST_CHECK_EQUAL(to_string(*bm.lookup(equal, 21)), "00010");
  BOOST_CHECK_EQUAL(to_string(*bm.lookup(equal, 30)), "00001");
  BOOST_CHECK_EQUAL(to_string(*bm.lookup(equal, 42)), "10100");
  BOOST_CHECK_EQUAL(to_string(*bm.lookup(equal, 84)), "01000");
  BOOST_CHECK_EQUAL(to_string(*bm.lookup(less_equal, 21)), "00010");
  BOOST_CHECK_EQUAL(to_string(*bm.lookup(less_equal, 30)), "00011");
  BOOST_CHECK_EQUAL(to_string(*bm.lookup(less_equal, 42)), "10111");
  BOOST_CHECK_EQUAL(to_string(*bm.lookup(less_equal, 84)), "11111");
  BOOST_CHECK_EQUAL(to_string(*bm.lookup(less_equal, 25)), "00010");
  BOOST_CHECK_EQUAL(to_string(*bm.lookup(less_equal, 80)), "10111");
  BOOST_CHECK_EQUAL(to_string(*bm.lookup(not_equal, 30)), "11110");
  BOOST_CHECK_EQUAL(to_string(*bm.lookup(greater, 42)), "01000");
  BOOST_CHECK_EQUAL(to_string(*bm.lookup(greater, 13)), "11111");
  BOOST_CHECK_EQUAL(to_string(*bm.lookup(greater, 84)), "00000");
  BOOST_CHECK_EQUAL(to_string(*bm.lookup(less, 42)), "00011");
  BOOST_CHECK_EQUAL(to_string(*bm.lookup(less, 84)), "10111");
  BOOST_CHECK_EQUAL(to_string(*bm.lookup(greater_equal, 84)), "01000");
  BOOST_CHECK_EQUAL(to_string(*bm.lookup(greater_equal, -42)), "11111");
  BOOST_CHECK_EQUAL(to_string(*bm.lookup(greater_equal, 22)), "11101");

  std::vector<uint8_t> buf;
  io::archive(buf, bm);
  io::unarchive(buf, bm2);
  BOOST_CHECK_EQUAL(bm, bm2);
  BOOST_CHECK_EQUAL(to_string(bm), to_string(bm2));
  BOOST_CHECK_EQUAL(to_string(*bm2.lookup(greater, 84)), "00000");
  BOOST_CHECK_EQUAL(to_string(*bm2.lookup(less, 84)), "10111");
  BOOST_CHECK_EQUAL(to_string(*bm2.lookup(greater_equal, -42)), "11111");
}

BOOST_AUTO_TEST_CASE(binary_encoded_bitmap)
{
  bitmap<int8_t, null_bitstream, binary_bitslice_coder> bm, bm2;
  BOOST_REQUIRE(bm.push_back(0));
  BOOST_REQUIRE(bm.push_back(1));
  BOOST_REQUIRE(bm.push_back(1));
  BOOST_REQUIRE(bm.push_back(2));
  BOOST_REQUIRE(bm.push_back(3));
  BOOST_REQUIRE(bm.push_back(2));
  BOOST_REQUIRE(bm.push_back(2));

  BOOST_CHECK_EQUAL(to_string(*bm[0]), "1000000");
  BOOST_CHECK_EQUAL(to_string(*bm[1]), "0110000");
  BOOST_CHECK_EQUAL(to_string(*bm[2]), "0001011");
  BOOST_CHECK_EQUAL(to_string(*bm[3]), "0000100");
  BOOST_CHECK(! bm[-42]);
  BOOST_CHECK(! bm[4]);
  BOOST_CHECK(! bm[5]);

  BOOST_CHECK_EQUAL(to_string(*bm.lookup(not_equal, -42)), "1111111");
  BOOST_CHECK_EQUAL(to_string(*bm.lookup(not_equal, 0)), "0111111");
  BOOST_CHECK_EQUAL(to_string(*bm.lookup(not_equal, 1)), "1001111");
  BOOST_CHECK_EQUAL(to_string(*bm.lookup(not_equal, 2)), "1110100");
  BOOST_CHECK_EQUAL(to_string(*bm.lookup(not_equal, 3)), "1111011");

  std::vector<uint8_t> buf;
  io::archive(buf, bm);
  io::unarchive(buf, bm2);
  BOOST_CHECK_EQUAL(bm, bm2);
  BOOST_CHECK_EQUAL(to_string(bm), to_string(bm2));
  BOOST_CHECK_EQUAL(to_string(*bm2[0]), "1000000");
  BOOST_CHECK_EQUAL(to_string(*bm2[1]), "0110000");
  BOOST_CHECK_EQUAL(to_string(*bm2[2]), "0001011");
}

BOOST_AUTO_TEST_CASE(bitmap_precision_binning_integral)
{
  bitmap<int, null_bitstream, equality_coder, precision_binner> bm{2};
  BOOST_REQUIRE(bm.push_back(183));
  BOOST_REQUIRE(bm.push_back(215));
  BOOST_REQUIRE(bm.push_back(350));
  BOOST_REQUIRE(bm.push_back(253));
  BOOST_REQUIRE(bm.push_back(101));
  
  BOOST_CHECK_EQUAL(to_string(*bm[100]), "10001");
  BOOST_CHECK_EQUAL(to_string(*bm[200]), "01010");
  BOOST_CHECK_EQUAL(to_string(*bm[300]), "00100");
}

BOOST_AUTO_TEST_CASE(bitmap_precision_binning_double_negative)
{
  bitmap<double, null_bitstream, equality_coder, precision_binner> bm{-3}, bm2;

  // These end up in different bins...
  BOOST_REQUIRE(bm.push_back(42.001));
  BOOST_REQUIRE(bm.push_back(42.002));

  // ...whereas these in the same.
  BOOST_REQUIRE(bm.push_back(43.0014));
  BOOST_REQUIRE(bm.push_back(43.0013));

  BOOST_REQUIRE(bm.push_back(43.0005)); // This one is rounded up to the previous bin...
  BOOST_REQUIRE(bm.push_back(43.0015)); // ...and this one to the next.
  
  BOOST_CHECK_EQUAL(to_string(*bm[42.001]), "100000");
  BOOST_CHECK_EQUAL(to_string(*bm[42.002]), "010000");
  BOOST_CHECK_EQUAL(to_string(*bm[43.001]), "001110");
  BOOST_CHECK_EQUAL(to_string(*bm[43.002]), "000001");

  std::vector<uint8_t> buf;
  io::archive(buf, bm);
  io::unarchive(buf, bm2);
  BOOST_CHECK_EQUAL(to_string(*bm2[43.001]), "001110");
  BOOST_CHECK_EQUAL(to_string(*bm2[43.002]), "000001");

  // Check if the precision got serialized properly and that adding a new
  // element lands in the right bin.
  BOOST_REQUIRE(bm2.push_back(43.0022));
  BOOST_CHECK_EQUAL(to_string(*bm2[43.002]), "0000011");
}

BOOST_AUTO_TEST_CASE(bitmap_precision_binning_double_positive)
{
  bitmap<double, null_bitstream, equality_coder, precision_binner> bm{1};

  // These end up in different bins...
  BOOST_REQUIRE(bm.push_back(42.123));
  BOOST_REQUIRE(bm.push_back(53.9));

  // ...whereas these in the same.
  BOOST_REQUIRE(bm.push_back(41.02014));
  BOOST_REQUIRE(bm.push_back(44.91234543));

  BOOST_REQUIRE(bm.push_back(39.5)); // This one just makes it into the 40 bin.
  BOOST_REQUIRE(bm.push_back(49.5)); // ...and this in the 50.
  
  BOOST_CHECK_EQUAL(to_string(*bm[40.0]), "101110");
  BOOST_CHECK_EQUAL(to_string(*bm[50.0]), "010001");
}

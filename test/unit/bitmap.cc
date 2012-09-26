#include "test.h"
#include "vast/bitmap.h"
#include "vast/to_string.h"

using namespace vast;

BOOST_AUTO_TEST_CASE(basic_bitmap)
{
  bitmap<int> bm;
  bm.push_back(42);
  bm.push_back(84);
  bm.push_back(42);
  bm.push_back(21);
  bm.push_back(30);
  
  BOOST_CHECK_EQUAL(to_string(bm[21].bits()), "01000");
  BOOST_CHECK_EQUAL(to_string(bm[30].bits()), "10000");
  BOOST_CHECK_EQUAL(to_string(bm[42].bits()), "00101");
  BOOST_CHECK_EQUAL(to_string(bm[84].bits()), "00010");
}

BOOST_AUTO_TEST_CASE(range_encoded_bitmap)
{
  bitmap<int, null_bitstream, range_encoder> bm;
  bm.push_back(42);
  bm.push_back(84);
  bm.push_back(42);
  bm.push_back(21);
  bm.push_back(30);

  BOOST_CHECK_EQUAL(to_string(bm[21].bits()), "01000");
  BOOST_CHECK_EQUAL(to_string(bm[30].bits()), "11000");
  BOOST_CHECK_EQUAL(to_string(bm[42].bits()), "11101");
  BOOST_CHECK_EQUAL(to_string(bm[84].bits()), "11111");

  bm.append(2, false);
  BOOST_CHECK_EQUAL(to_string(bm[21].bits()), "0001000");
}

BOOST_AUTO_TEST_CASE(binary_encoded_bitmap)
{
  bitmap<int8_t, null_bitstream, binary_encoder> bm;
  bm.push_back(0);
  bm.push_back(1);
  bm.push_back(1);
  bm.push_back(2);
  bm.push_back(3);
  bm.push_back(2);
  bm.push_back(2);

  BOOST_CHECK_EQUAL(to_string(bm[0].bits()), "");
  BOOST_CHECK_EQUAL(to_string(bm[1].bits()), "0010110");
  BOOST_CHECK_EQUAL(to_string(bm[2].bits()), "1111000");
  BOOST_CHECK_EQUAL(to_string(bm[3].bits()), "1111110"); // 0x01 | 0x10.
  BOOST_CHECK_EQUAL(to_string(bm[4].bits()), "0000000");
}

BOOST_AUTO_TEST_CASE(bitmap_precision_binning_integral)
{
  bitmap<int, null_bitstream, equality_encoder, precision_binner> bm({}, 2);
  bm.push_back(183);
  bm.push_back(215);
  bm.push_back(350);
  bm.push_back(253);
  bm.push_back(101);
  
  BOOST_CHECK_EQUAL(to_string(bm[100].bits()), "10001");
  BOOST_CHECK_EQUAL(to_string(bm[200].bits()), "01010");
  BOOST_CHECK_EQUAL(to_string(bm[300].bits()), "00100");
}

BOOST_AUTO_TEST_CASE(bitmap_precision_binning_double_negative)
{
  bitmap<double, null_bitstream, equality_encoder, precision_binner> bm({}, -3);

  // These end up in different bins...
  bm.push_back(42.001);
  bm.push_back(42.002);

  // ...whereas these in the same.
  bm.push_back(43.0014);
  bm.push_back(43.0013);

  bm.push_back(43.0005); // This one is rounded up to the previous bin...
  bm.push_back(43.0015); // ...and this one to the next.
  
  BOOST_CHECK_EQUAL(to_string(bm[42.001].bits()), "000001");
  BOOST_CHECK_EQUAL(to_string(bm[42.002].bits()), "000010");
  BOOST_CHECK_EQUAL(to_string(bm[43.001].bits()), "011100");
  BOOST_CHECK_EQUAL(to_string(bm[43.002].bits()), "100000");
}

BOOST_AUTO_TEST_CASE(bitmap_precision_binning_double_positive)
{
  bitmap<double, null_bitstream, equality_encoder, precision_binner> bm({}, 1);

  // These end up in different bins...
  bm.push_back(42.123);
  bm.push_back(53.9);

  // ...whereas these in the same.
  bm.push_back(41.02014);
  bm.push_back(44.91234543);

  bm.push_back(39.5); // This one just makes it into the 40 bin.
  bm.push_back(49.5); // ...and this in the 50.
  
  BOOST_CHECK_EQUAL(to_string(bm[40.0].bits()), "011101");
  BOOST_CHECK_EQUAL(to_string(bm[50.0].bits()), "100010");
}

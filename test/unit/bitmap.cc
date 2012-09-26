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

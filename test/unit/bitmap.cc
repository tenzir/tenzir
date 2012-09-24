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
  
  BOOST_CHECK_EQUAL(to_string(bm[21]->bits()), "01000");
  BOOST_CHECK_EQUAL(to_string(bm[30]->bits()), "10000");
  BOOST_CHECK_EQUAL(to_string(bm[42]->bits()), "00101");
  BOOST_CHECK_EQUAL(to_string(bm[84]->bits()), "00010");
}

BOOST_AUTO_TEST_CASE(range_encoded_bitmap)
{
  bitmap<int, null_bitstream, range_encoder> bm;
  bm.push_back(42);
  bm.push_back(84);
  bm.push_back(42);
  bm.push_back(21);
  bm.push_back(30);

  BOOST_CHECK_EQUAL(to_string(bm[21]->bits()), "01000");
  BOOST_CHECK_EQUAL(to_string(bm[30]->bits()), "11000");
  BOOST_CHECK_EQUAL(to_string(bm[42]->bits()), "11101");
  BOOST_CHECK_EQUAL(to_string(bm[84]->bits()), "11111");

  bm.append(2, false);
  BOOST_CHECK_EQUAL(to_string(bm[21]->bits()), "0001000");
}

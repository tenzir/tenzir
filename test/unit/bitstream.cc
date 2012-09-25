#include "test.h"
#include "vast/bitstream.h"
#include "vast/to_string.h"

using namespace vast;

BOOST_AUTO_TEST_CASE(null_bitstream_operations)
{
  null_bitstream x;
  x.append(3, true);
  x.append(7, false);
  x.push_back(true);
  BOOST_CHECK_EQUAL(to_string(x),  "10000000111");
  BOOST_CHECK_EQUAL(to_string(~x), "01111111000");

  null_bitstream y;
  y.append(2, true);
  y.append(4, false);
  y.append(3, true);
  y.push_back(false);
  y.push_back(true);
  BOOST_CHECK_EQUAL(to_string(y),  "10111000011");
  BOOST_CHECK_EQUAL(to_string(~y), "01000111100");

  BOOST_CHECK_EQUAL(to_string(x & y), "10000000011");
  BOOST_CHECK_EQUAL(to_string(x | y), "10111000111");
  BOOST_CHECK_EQUAL(to_string(x ^ y), "00111000100");
  BOOST_CHECK_EQUAL(to_string(x - y), "00000000100");
  BOOST_CHECK_EQUAL(to_string(y - x), "00111000000");
}

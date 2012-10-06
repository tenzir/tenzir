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
  BOOST_CHECK_EQUAL(to_string(x),  "11100000001");
  BOOST_CHECK_EQUAL(to_string(~x), "00011111110");

  null_bitstream y;
  y.append(2, true);
  y.append(4, false);
  y.append(3, true);
  y.push_back(false);
  y.push_back(true);
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

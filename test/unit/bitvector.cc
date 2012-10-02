#include "test.h"
#include "vast/bitvector.h"
#include "vast/to_string.h"

using namespace vast;

BOOST_AUTO_TEST_CASE(bitvector_to_string)
{
  bitvector a;
  bitvector b(10);
  bitvector c(78, true);

  BOOST_CHECK_EQUAL(to_string(a), "");
  BOOST_CHECK_EQUAL(to_string(b), "0000000000");
  BOOST_CHECK_EQUAL(
      to_string(c), 
      "1111111111111111111111111111111111111111111111111111111111111111..");
}

BOOST_AUTO_TEST_CASE(bitvector_basic_ops)
{
  bitvector x;
  x.push_back(true);
  x.push_back(false);
  x.push_back(true);

  BOOST_CHECK_EQUAL(x.size(), 3);
  BOOST_CHECK_EQUAL(x.blocks(), 1);

  x.append(0xffff);

  BOOST_REQUIRE_EQUAL(x.blocks(), 2);
  BOOST_CHECK_EQUAL(x.size(), 3 + bitvector::bits_per_block);
}

BOOST_AUTO_TEST_CASE(bitvector_bitwise_ops)
{
  bitvector a(6);
  BOOST_CHECK_EQUAL(a.size(), 6);
  BOOST_CHECK_EQUAL(a.blocks(), 1);

  a.flip(3);
  BOOST_CHECK_EQUAL(to_string(a), "001000");
  BOOST_CHECK_EQUAL(to_string(a << 1), "010000");
  BOOST_CHECK_EQUAL(to_string(a << 2), "100000");
  BOOST_CHECK_EQUAL(to_string(a << 3), "000000");
  BOOST_CHECK_EQUAL(to_string(a >> 1), "000100");
  BOOST_CHECK_EQUAL(to_string(a >> 2), "000010");
  BOOST_CHECK_EQUAL(to_string(a >> 3), "000001");
  BOOST_CHECK_EQUAL(to_string(a >> 4), "000000");

  bitvector b(a);
  b[5] = b[1] = 1;
  BOOST_CHECK_EQUAL(to_string(b), "101010");
  BOOST_CHECK_EQUAL(to_string(~b), "010101");

  BOOST_CHECK_EQUAL(to_string(a | ~b), "011101");
  BOOST_CHECK_EQUAL(to_string((~a << 2) & b), to_string(a));

  BOOST_CHECK_EQUAL(b.count(), 3);

  BOOST_CHECK_EQUAL(to_string(b, false), "010101");
}

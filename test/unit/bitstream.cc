#include "test.h"
#include "vast/bitstream.h"
#include "vast/to_string.h"

using namespace vast;

BOOST_AUTO_TEST_CASE(null_bitstream_operations)
{
  null_bitstream b;
  b.append(3, true);
  b.append(7, false);
  b.push_back(true);
  BOOST_CHECK_EQUAL(to_string(b.bits()), "10000000111");
}

#include "test.h"
#include "vast/util/hash/crc.h"
#include "vast/util/hash/murmur.h"
#include "vast/util/hash/xxhash.h"

using namespace vast;
using namespace vast::util;

BOOST_AUTO_TEST_CASE(murmur_hashing)
{
  BOOST_CHECK_EQUAL(murmur3<32>::digest(42), 3160117731);
}

BOOST_AUTO_TEST_CASE(xxhash_hashing)
{
  BOOST_CHECK_EQUAL(xxhash::digest(42), 1161967057);

  xxhash xxh;
  xxh.add(0);
  xxh.add(1);
  xxh.add(2);
  BOOST_CHECK_EQUAL(xxh.get(), 964478135);
}

BOOST_AUTO_TEST_CASE(crc32_checksumming)
{
  BOOST_CHECK_EQUAL(crc32::digest('f'), 1993550816);
  BOOST_CHECK_EQUAL(crc32::digest('o'), 252678980);
  BOOST_CHECK_EQUAL(crc32::digest_bytes("foo", 3), 2356372769);

  crc32 crc;
  crc.add('f');
  BOOST_CHECK_EQUAL(crc.get(), 1993550816);
  crc.add('o');
  BOOST_CHECK_EQUAL(crc.get(), 2943590935);
  crc.add('o');
  BOOST_CHECK_EQUAL(crc.get(), 2356372769);
}

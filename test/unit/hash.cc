#include "framework/unit.h"

#include "vast/util/hash/crc.h"
#include "vast/util/hash/murmur.h"
#include "vast/util/hash/xxhash.h"

using namespace vast;
using namespace vast::util;

SUITE("util")

TEST("murmur hash")
{
  CHECK(murmur3<32>::digest(42) == 3160117731);
}

TEST("xxhash hash")
{
  CHECK(xxhash::digest(42) == 1161967057);

  xxhash xxh;
  xxh.add(0);
  xxh.add(1);
  xxh.add(2);
  CHECK(xxh.get() == 964478135);
}

TEST("crc32")
{
  CHECK(crc32::digest('f') == 1993550816);
  CHECK(crc32::digest('o') == 252678980);
  CHECK(crc32::digest_bytes("foo", 3) == 2356372769);

  crc32 crc;
  crc.add('f');
  CHECK(crc.get() == 1993550816);
  crc.add('o');
  CHECK(crc.get() == 2943590935);
  crc.add('o');
  CHECK(crc.get() == 2356372769);
}

#include <caf/all.hpp>

#include "vast/value.h"

#include "test.h"

using namespace vast;

TEST(caf_serialization)
{
  record r0{42, "foo", -8.3, record{nil, time::now()}};
  MESSAGE("serializing record");
  std::vector<uint8_t> buf;
  caf::binary_serializer bs{std::back_inserter(buf)};
  caf::uniform_typeid<record>()->serialize(&r0, &bs);
  MESSAGE("deserializing record");
  record r1;
  caf::binary_deserializer bd{buf.data(), buf.size()};
  caf::uniform_typeid<record>()->deserialize(&r1, &bd);
  CHECK(r0 == r1);
}

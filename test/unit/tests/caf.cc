#include "framework/unit.h"
#include <caf/all.hpp>
#include "vast/value.h"

using namespace vast;

SUITE("CAF")

TEST("serialization")
{
  record r0{42, "foo", -8.3, record{nil, now()}};

  std::vector<uint8_t> buf;
  caf::binary_serializer bs{std::back_inserter(buf)};
  caf::uniform_typeid<record>()->serialize(&r0, &bs);

  record r1;
  caf::binary_deserializer bd{buf.data(), buf.size()};
  caf::uniform_typeid<record>()->deserialize(&r1, &bd);

  CHECK(r0 == r1);
}

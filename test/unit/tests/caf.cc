#include "framework/unit.h"

#include <caf/all.hpp>

#include "vast/event.h"

using namespace vast;

TEST("libcaf serialization")
{
  event e0{42, "foo", -8.3, record{invalid, now()}};
  e0.id(101);

  std::vector<uint8_t> buf;
  caf::binary_serializer bs{std::back_inserter(buf)};
  caf::uniform_typeid<event>()->serialize(&e0, &bs);

  event e1;
  caf::binary_deserializer bd{buf.data(), buf.size()};
  caf::uniform_typeid<event>()->deserialize(&e1, &bd);

  CHECK(e0 == e1);
}

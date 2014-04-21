#include "test.h"
#include "cppa/cppa.hpp"
#include "cppa/binary_serializer.hpp"
#include "cppa/binary_deserializer.hpp"
#include "cppa/util/buffer.hpp"
#include "vast/event.h"

using namespace vast;

BOOST_AUTO_TEST_CASE(cppa_serialization)
{
  event e0{42, "foo", -8.3, record{invalid, address{"10.0.0.1"}}};
  e0.id(101);

  cppa::util::buffer buf;
  cppa::binary_serializer bs(&buf);
  cppa::uniform_typeid<event>()->serialize(&e0, &bs);

  event e1;
  cppa::binary_deserializer bd(buf.data(), buf.size());
  cppa::uniform_typeid<event>()->deserialize(&e1, &bd);

  BOOST_CHECK_EQUAL(e0, e1);
}

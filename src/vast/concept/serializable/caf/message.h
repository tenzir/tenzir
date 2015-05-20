#ifndef VAST_CONCEPT_SERIALIZABLE_CAF_MESSAGE_H
#define VAST_CONCEPT_SERIALIZABLE_CAF_MESSAGE_H

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>
#include <caf/message.hpp>

#include "vast/concept/serializable/caf/adapters.h"
#include "vast/concept/serializable/std/vector.h"

namespace vast {

template <typename Serializer>
void serialize(Serializer& sink, caf::message const& msg)
{
  vast_to_caf_serializer<Serializer> s{sink};
  caf::uniform_typeid<caf::message>()->serialize(&msg, &s);
}

template <typename Deserializer>
void deserialize(Deserializer& source, caf::message& msg)
{
  vast_to_caf_deserializer<Deserializer> d{source};
  caf::uniform_typeid<caf::message>()->deserialize(&msg, &d);
}

} // namespace vast

#endif

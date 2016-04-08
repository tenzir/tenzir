#ifndef VAST_CONCEPT_SERIALIZABLE_CAF_MESSAGE_HPP
#define VAST_CONCEPT_SERIALIZABLE_CAF_MESSAGE_HPP

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>
#include <caf/message.hpp>

#include "vast/concept/serializable/caf/adapters.hpp"
#include "vast/concept/serializable/std/vector.hpp"

namespace vast {

template <typename Serializer>
void serialize(Serializer& sink, caf::message const& msg) {
  vast_to_caf_serializer<Serializer> s{sink};
  caf::uniform_typeid<caf::message>()->serialize(&msg, &s);
}

template <typename Deserializer>
void deserialize(Deserializer& source, caf::message& msg) {
  vast_to_caf_deserializer<Deserializer> d{source};
  caf::uniform_typeid<caf::message>()->deserialize(&msg, &d);
}

} // namespace vast

#endif

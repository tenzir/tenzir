#ifndef VAST_CONCEPT_SERIALIZABLE_SCHEMA_H
#define VAST_CONCEPT_SERIALIZABLE_SCHEMA_H

#include "vast/schema.h"
#include "vast/concept/serializable/std/string.h"

namespace vast {

// TODO: we should figure out a better way to (de)serialize. Going through
// strings is not very efficient, although we currently have no other way to
// keep the pointer relationships of the types intact.

template <typename Serializer>
void serialize(Serializer& sink, schema const& sch)
{
  serialize(sink, to_string(sch));
}

template <typename Deserializer>
void deserialize(Deserializer& source, schema& sch)
{
  std::string str;
  deserialize(source, str);
  auto i = str.begin();
  if (auto s = parse<schema>(i, str.end()))
    sch = *s;
}

} // namespace vast

#endif

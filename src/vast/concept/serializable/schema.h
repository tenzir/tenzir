#ifndef VAST_CONCEPT_SERIALIZABLE_SCHEMA_H
#define VAST_CONCEPT_SERIALIZABLE_SCHEMA_H

#include <iostream>
#include "vast/schema.h"
#include "vast/concept/serializable/std/string.h"

namespace vast {

// TODO: we should figure out a better way to (de)serialize. Going through
// strings is not very efficient, although we currently have no other way to
// keep the pointer relationships of the types intact.

template <typename Serializer>
void serialize(Serializer& sink, schema const& sch)
{
  sink << to_string(sch);
}

template <typename Deserializer>
void deserialize(Deserializer& source, schema& sch)
{
  std::string str;
  source >> str;
  if (str.empty())
    return;
  sch.clear();
  auto i = str.begin();
  parse(sch, i, str.end());
}

} // namespace vast

#endif

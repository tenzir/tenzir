#ifndef VAST_CONCEPT_SERIALIZABLE_VAST_SCHEMA_H
#define VAST_CONCEPT_SERIALIZABLE_VAST_SCHEMA_H

#include <iostream>

#include "vast/schema.h"
#include "vast/concept/serializable/std/string.h"
#include "vast/concept/parseable/parse.h"
#include "vast/concept/parseable/vast/schema.h"
#include "vast/concept/printable/to_string.h"
#include "vast/concept/printable/vast/schema.h"

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
  parse(i, str.end(), sch);
}

} // namespace vast

#endif

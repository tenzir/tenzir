#ifndef VAST_ERROR_H
#define VAST_ERROR_H

#include "vast/util/error.h"

namespace vast {

using util::error;

template <typename Serializer>
void serialize(Serializer& sink, error const& e)
{
  sink << e.msg();
}

template <typename Deserializer>
void deserialize(Deserializer& source, error& e)
{
  std::string str;
  source >> str;
  e = error{std::move(str)};
}

} // namespace vast

#endif

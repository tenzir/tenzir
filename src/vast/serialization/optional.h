#ifndef VAST_SERIALIZATION_OPTIONAL_H
#define VAST_SERIALIZATION_OPTIONAL_H

#include "vast/serialization.h"
#include "vast/util/optional.h"

namespace vast {

template <typename T>
void serialize(serializer& sink, util::optional<T> const& opt)
{
  if (opt.valid())
    sink << true << opt.get();
  else
    sink << false;
}

template <typename T>
void deserialize(deserializer& source, util::optional<T>& opt)
{
  bool flag;
  source >> flag;
  if (! flag)
    return;

  T x;
  source >> x;
  opt = {std::move(x)};
}

} // namespace vast

#endif

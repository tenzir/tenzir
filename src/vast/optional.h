#ifndef VAST_OPTIONAL_HPP
#define VAST_OPTIONAL_HPP

#include "vast/serialization.h"
#include "vast/util/optional.h"

namespace vast {

using util::optional;

template <typename T>
void serialize(serializer& sink, optional<T> const& opt)
{
  if (opt.valid())
    sink << true << opt.get();
  else
    sink << false;
}

template <typename T>
void deserialize(deserializer& source, optional<T>& opt)
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

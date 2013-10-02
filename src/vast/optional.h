#ifndef VAST_OPTIONAL_HPP
#define VAST_OPTIONAL_HPP

#include <cppa/optional.hpp>
#include "vast/serialization.h"

namespace vast {

/// An optional value of `T` with similar semantics as `std::optional`.
template <typename T>
using optional = cppa::optional<T>;

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

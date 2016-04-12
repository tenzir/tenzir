#ifndef VAST_CONCEPT_SERIALIZABLE_MAYBE_HPP
#define VAST_CONCEPT_SERIALIZABLE_MAYBE_HPP

#include <caf/deserializer.hpp>
#include <caf/serializer.hpp>

#include "vast/maybe.hpp"

namespace vast {

template <class T>
void serialize(caf::serializer& sink, maybe<T> const& m) {
  if (m)
    sink << true << *m;
  else
    sink << false;
}

template <class T>
void serialize(caf::deserializer& source, maybe<T>& m) {
  bool flag;
  source >> flag;
  if (!flag)
    return;
  T x;
  source >> x;
  m = std::move(x);
}

} // namespace vast

#endif

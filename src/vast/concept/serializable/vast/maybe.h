#ifndef VAST_CONCEPT_SERIALIZABLE_MAYBE_H
#define VAST_CONCEPT_SERIALIZABLE_MAYBE_H

#include "vast/concept/serializable/builtin.h"
#include "vast/maybe.h"

namespace vast {

template <typename Serializer, typename T>
void serialize(Serializer& sink, maybe<T> const& opt) {
  if (opt)
    sink << true << *opt;
  else
    sink << false;
}

template <typename Deserializer, typename T>
void deserialize(Deserializer& source, maybe<T>& opt) {
  bool flag;
  source >> flag;
  if (!flag)
    return;
  T x;
  source >> x;
  opt = {std::move(x)};
}

} // namespace vast

#endif

#ifndef VAST_CONCEPT_SERIALIZABLE_OPTIONAL_H
#define VAST_CONCEPT_SERIALIZABLE_OPTIONAL_H

#include "vast/concept/serializable/builtin.h"
#include "vast/optional.h"

namespace vast {

template <typename Serializer, typename T>
void serialize(Serializer& sink, optional<T> const& opt) {
  if (opt.valid())
    sink << true << opt.get();
  else
    sink << false;
}

template <typename Deserializer, typename T>
void deserialize(Deserializer& source, optional<T>& opt) {
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

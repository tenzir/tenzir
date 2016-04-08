#ifndef VAST_CONCEPT_SERIALIZABLE_MAYBE_HPP
#define VAST_CONCEPT_SERIALIZABLE_MAYBE_HPP

#include "vast/concept/serializable/builtin.hpp"
#include "vast/maybe.hpp"

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
  opt = std::move(x);
}

} // namespace vast

#endif

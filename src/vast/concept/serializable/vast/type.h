#ifndef VAST_CONCEPT_SERIALIZABLE_VAST_TYPE_H
#define VAST_CONCEPT_SERIALIZABLE_VAST_TYPE_H

#include "vast/type.h"
#include "vast/concept/serializable/std/string.h"
#include "vast/concept/serializable/std/vector.h"
#include "vast/concept/serializable/vast/none.h"
#include "vast/concept/serializable/vast/util/hash.h"
#include "vast/concept/serializable/state.h"
#include "vast/concept/state/type.h"

namespace vast {

template <typename Serializer>
void serialize(Serializer& sink, type const& t) {
  auto tag = which(t);
  sink << tag;
  if (tag != type::tag::none)
    visit([&](auto& x) { sink << x; }, t);
}

template <typename Deserializer>
void deserialize(Deserializer& source, type& t) {
  type::tag tag = {};
  source >> tag;
  if (tag != type::tag::none) {
    t = type{util::make_intrusive<type::intrusive_info>(
      type::info::make(static_cast<type::tag>(tag)))};
    visit([&](auto& x) { source >> x; }, t);
  }
}

} // namespace vast

#endif

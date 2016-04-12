#ifndef VAST_CONCEPT_SERIALIZABLE_VAST_TYPE_HPP
#define VAST_CONCEPT_SERIALIZABLE_VAST_TYPE_HPP

#include <caf/serializer.hpp>
#include <caf/deserializer.hpp>

#include "vast/type.hpp"
#include "vast/concept/serializable/vast/util/hash.hpp"
#include "vast/concept/serializable/vast/util/variant.hpp"
#include "vast/concept/serializable/state.hpp"
#include "vast/concept/state/type.hpp"

namespace vast {

inline void serialize(caf::serializer& sink, type const& t) {
  auto tag = which(t);
  sink << tag;
  if (tag != type::tag::none)
    visit([&](auto& x) { sink << x; }, t);
}

inline void serialize(caf::deserializer& source, type& t) {
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

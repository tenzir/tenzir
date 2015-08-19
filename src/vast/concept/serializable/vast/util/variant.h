#ifndef VAST_CONCEPT_SERIALIZABLE_VAST_UTIL_VARIANT_H
#define VAST_CONCEPT_SERIALIZABLE_VAST_UTIL_VARIANT_H

#include "vast/concept/serializable/builtin.h"
#include "vast/util/variant.h"

namespace vast {

template <typename Serializer, typename Tag, typename... Ts>
void serialize(Serializer& sink, util::basic_variant<Tag, Ts...> const& v) {
  auto tag = which(v);
  sink << tag;
  visit([&](auto& x) { sink << x; }, v);
}

template <typename Deserializer, typename Tag, typename... Ts>
void deserialize(Deserializer& source, util::basic_variant<Tag, Ts...>& v) {
  Tag t = {};
  source >> t;
  v = util::basic_variant<Tag, Ts...>::make(t);
  visit([&](auto& x) { source >> x; }, v);
}

} // namespace vast

#endif

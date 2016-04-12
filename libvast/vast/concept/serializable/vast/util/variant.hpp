#ifndef VAST_CONCEPT_SERIALIZABLE_VAST_UTIL_VARIANT_HPP
#define VAST_CONCEPT_SERIALIZABLE_VAST_UTIL_VARIANT_HPP

#include <caf/serializer.hpp>
#include <caf/deserializer.hpp>

#include "vast/util/variant.hpp"

namespace vast {
namespace util {

template <typename Tag, typename... Ts>
void serialize(caf::serializer& sink, basic_variant<Tag, Ts...> const& v) {
  auto tag = which(v);
  sink << tag;
  visit([&](auto& x) { sink << x; }, v);
}

template <typename Tag, typename... Ts>
void serialize(caf::deserializer& source, basic_variant<Tag, Ts...>& v) {
  Tag t = {};
  source >> t;
  v = util::basic_variant<Tag, Ts...>::make(t);
  visit([&](auto& x) { source >> x; }, v);
}

} // namespace util
} // namespace vast

#endif

#ifndef VAST_SERIALIZATION_VARIANT_H
#define VAST_SERIALIZATION_VARIANT_H

#include "vast/serialization/enum.h"
#include "vast/util/variant.h"

namespace vast {

template <typename Tag, typename... Ts>
void serialize(serializer& sink, util::basic_variant<Tag, Ts...> const& v)
{
  sink << which(v);
  visit([&](auto&& x) { sink << x; }, v);
}

template <typename Tag, typename... Ts>
void deserialize(deserializer& source, util::basic_variant<Tag, Ts...>& v)
{
  Tag t;
  source >> t;
  v = util::basic_variant<Tag, Ts...>::make(t);
  visit([&](auto& x) { source >> x; }, v);
}

} // namespace vast

#endif

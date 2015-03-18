#ifndef VAST_ANNOUNCE_H
#define VAST_ANNOUNCE_H

#include <caf/announce.hpp>

#include "vast/concept/serializable/caf_type_info.h"
#include "vast/concept/serializable/hierarchy.h"

namespace vast {

/// Announces a type to CAF using VAST's serialization framework.
/// @tparam T The type to announce.
/// @param name The globally unique name for `T`
template <typename T, typename Name>
void announce(Name&& name)
{
  auto ti = std::make_unique<caf_type_info<T>>(name);
  caf::announce(typeid(T), std::move(ti));
}

template <typename T, typename... Ts, typename Name, typename... Names>
void announce(Name&& name, Names&&... names)
{
  announce<T>(std::forward<Name>(name));
  announce<Ts...>(std::forward<Names>(names)...);
}

/// Announces polymorphic class hierarchy.
/// @param names The names of the derived instances.
/// @see announce
template <typename Base, typename... Derived, typename... Names>
void announce_hierarchy(Names&&... names)
{
  announce<Derived...>(std::forward<Names>(names)...);
  add_opaque_hierarchy<Base, Derived...>();
}

/// Announces the builtin types of VAST.
void announce_types();

} // namespace vast

#endif

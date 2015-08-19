#ifndef VAST_CONCEPT_SERIALIZABLE_HIERARCHY_H
#define VAST_CONCEPT_SERIALIZABLE_HIERARCHY_H

#include <functional>
#include <map>
#include <type_traits>
#include <typeinfo>

#include <caf/serializer.hpp>
#include <caf/deserializer.hpp>

#include "vast/die.h"
#include "vast/concept/serializable/caf/adapters.h"
#include "vast/util/assert.h"

namespace vast {

//
// Type-safe hierarchies.
//

template <typename Base>
using hierarchy = std::map<caf::uniform_type_info const*,
                           std::function<Base*(caf::deserializer*)>>;

template <typename Base, typename Derived>
typename hierarchy<Base>::value_type make_hierarchy_entry() {
  static_assert(std::is_base_of<Base, Derived>::value,
                "hierarchy entries must have base-derived relationships");
  auto uti = caf::uniform_typeid<Derived>();
  auto factory = [uti](caf::deserializer* src) -> Base* {
    auto obj = std::make_unique<Derived>();
    uti->deserialize(obj.get(), src);
    return obj.release();
  };
  return {uti, factory};
}

template <typename Base, typename... Derived>
hierarchy<Base> make_hierarchy() {
  return {make_hierarchy_entry<Base, Derived>()...};
}

//
// Type-erased hierarchies.
//

using opaque_hierarchy = std::map<caf::uniform_type_info const*,
                                  std::function<void*(caf::deserializer*)>>;

namespace detail {

void register_opaque_hierarchy(opaque_hierarchy h, std::type_info const& base);

opaque_hierarchy const* find_opaque_hierarchy(std::type_info const& base);

template <typename Base, typename Derived>
opaque_hierarchy::value_type make_opaque_hierarchy_entry() {
  static_assert(std::is_base_of<Base, Derived>::value,
                "hierarchy entries must have base-derived relationships");
  auto uti = caf::uniform_typeid<Derived>();
  auto factory = [uti](caf::deserializer* src) -> void* {
    auto obj = std::make_unique<Derived>();
    uti->deserialize(obj.get(), src);
    return obj.release();
  };
  return {uti, factory};
}

template <typename Base, typename... Derived>
opaque_hierarchy make_opaque_hierarchy() {
  return {make_opaque_hierarchy_entry<Base, Derived>()...};
}

} // namespace detail

/// Registers an opaque hierarchy with the runtime.
/// @tparam Base The type of the class.
/// @tparam Derived The types of the children of *Base*
/// @warn This function is *not* thread-safe. Hierarchy registration should be
///       performed before using the serialization framework.
/// @see get_opaque_hierarchy
template <typename Base, typename... Derived>
void add_opaque_hierarchy() {
  auto oh = detail::make_opaque_hierarchy<Base, Derived...>();
  detail::register_opaque_hierarchy(std::move(oh), typeid(Base));
}

/// Retrieves an opaque hierarchy from the runtime.
/// @tparam Base The type of the class.
/// @returns A pointer to the class hierarchy for `Base` or `nullptr` if no
///          such a hierarchy exists.
/// @see add_opaque_hierarchy
template <typename Base>
opaque_hierarchy const* get_opaque_hierarchy() {
  return detail::find_opaque_hierarchy(typeid(Base));
}

//
// Serialization functions
//

namespace detail {

template <typename T>
void polymorphic_serialize(caf::serializer& sink, T const* x) {
  VAST_ASSERT(x != nullptr);
  auto uti = caf::uniform_typeid(typeid(*x), true);
  if (uti == nullptr)
    die("unannounced type");
  sink.begin_object(uti);
  uti->serialize(x, &sink);
  sink.end_object();
}

template <typename Base>
void polymorphic_deserialize(caf::deserializer& source, Base*& base,
                             hierarchy<Base>& h) {
  auto i = h.find(source.begin_object());
  if (i == h.end())
    die("missing hierarchy entry");
  base = (i->second)(&source);
  source.end_object();
}

template <typename Base>
void polymorphic_deserialize(caf::deserializer& source, Base*& base) {
  auto h = get_opaque_hierarchy<Base>();
  if (h == nullptr)
    die("no hierarchy");
  auto uti = source.begin_object();
  if (uti == nullptr)
    die("unannounced type");
  auto i = h->find(uti);
  if (i == h->end())
    die("missing hierarchy entry");
  base = reinterpret_cast<Base*>((i->second)(&source));
  source.end_object();
}

} // namespace detail

/// Serializes a polymorphic object instance.
/// @tparam Serializer A VAST serializer.
/// @tparam T the type of the instance to serialize.
/// @param sink The serializer to write into.
/// @param x The instance to serialize.
/// @see add_opaque_hierarchy
template <typename Serializer, typename T>
void polymorphic_serialize(Serializer& sink, T const* x) {
  if (x == nullptr)
    die("nullptr not allowed");
  vast_to_caf_serializer<Serializer> s{sink};
  detail::polymorphic_serialize(s, x);
}

/// Deserializes a polymorphic object instance.
/// @tparam Deserializer A VAST deserializer.
/// @tparam T the type of the instance to deserialize.
/// @param sink The deserializer to read from.
/// @param x The pointer to the base of a polymorphic hierarchy.
/// @see add_opaque_hierarchy
template <typename Deserializer, typename Base>
void polymorphic_deserialize(Deserializer& source, Base*& base) {
  vast_to_caf_deserializer<Deserializer> d{source};
  detail::polymorphic_deserialize(d, base);
}

} // namespace vast

#endif

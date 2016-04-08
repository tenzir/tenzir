#include <unordered_map>
#include <typeindex>

#include "vast/concept/serializable/hierarchy.hpp"

namespace vast {

namespace {

std::unordered_map<std::type_index, opaque_hierarchy> hierarchies;

} // namespace <anonymous>

namespace detail {

void register_opaque_hierarchy(opaque_hierarchy h, std::type_info const& base) {
  hierarchies[std::type_index(base)] = std::move(h);
}

opaque_hierarchy const* find_opaque_hierarchy(std::type_info const& base) {
  auto i = hierarchies.find(std::type_index(base));
  return i == hierarchies.end() ? nullptr : &i->second;
}

} // namespace detail
} // namespace vast

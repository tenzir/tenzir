#ifndef VAST_SYSTEM_TRACKER_HPP
#define VAST_SYSTEM_TRACKER_HPP

#include <unordered_set>
#include <unordered_map>
#include <string>

#include <caf/stateful_actor.hpp>
#include <caf/replies_to.hpp>
#include <caf/typed_actor.hpp>

#include "vast/optional.hpp"

#include "vast/system/atoms.hpp"
#include "vast/detail/radix_tree.hpp"

namespace vast {
namespace system {

using registry = std::unordered_multimap<std::string, caf::actor>;
using component_map = std::unordered_map<std::string, registry>;
using component_map_entry = component_map::value_type;

struct tracker_state {
  component_map components;
  const char* name = "tracker";
};

// TODO: once all major components have been converted to typed actors, we
// can get rid of the string-discriminator in put(type,actor) and use
// type-specific overloads instead put(actor_type).
using tracker_type = caf::typed_actor<
  // Add a component.
  caf::replies_to<put_atom, std::string, caf::actor>::with<ok_atom>,
  // Retrieves the component
  caf::replies_to<get_atom>::with<component_map>,
  // Connects two components identified by their name.
  //caf::replies_to<connect_atom, std::string, std::string>::with<ok_atom>,
  // Disconnects two connected components.
  //caf::replies_to<disconnect_atom, std::string, std::string>::with<ok_atom>,
  // Peering and state propagation.
  caf::replies_to<peer_atom, caf::actor, std::string>::with<state_atom,
                                                            component_map>,
  caf::replies_to<state_atom, component_map>::with<ok_atom>,
  caf::reacts_to<state_atom, component_map_entry>,
  caf::reacts_to<state_atom, std::string, std::string, caf::actor>
>;

/// Keeps track of the topology in a VAST deployment.
/// @param self The actor handle.
/// @param node The name of the local node.
tracker_type::behavior_type
tracker(tracker_type::stateful_pointer<tracker_state> self, std::string node);

} // namespace system
} // namespace vast

#endif

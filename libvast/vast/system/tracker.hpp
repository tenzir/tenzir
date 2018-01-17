/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

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

/// State maintained per component.
struct component_state {
  caf::actor actor;
  std::string label;
};

template <class Inspector>
auto inspect(Inspector& f, component_state& cs) {
  return f(cs.actor, cs.label);
}

/// Maps a component type ("archive", "index", etc.) to its state.
using component_state_map = std::unordered_multimap<std::string, component_state>;

/// Maps node names to component state.
using component_map = std::unordered_map<std::string, component_state_map>;

/// An entry of the `component_map`.
using component_map_entry = std::pair<std::string, component_state_map>;

/// The graph of connected components..
//using link_map = std::unordered_multimap<caf::actor, caf::actor>;

/// Tracker meta data: components and their links.
struct registry {
  component_map components;
  //link_map links;
};

template <class Inspector>
auto inspect(Inspector& f, registry& r) {
  //return f(r.components, r.links);
  return f(r.components);
}

struct tracker_state {
  vast::system::registry registry;
  const char* name = "tracker";
};

using tracker_type = caf::typed_actor<
  // Adds a component.
  caf::replies_to<put_atom, std::string, caf::actor, std::string>
    ::with<ok_atom>,
  // Propagated PUT received from peer.
  caf::reacts_to<put_atom, std::string, std::string, caf::actor, std::string>,
  // Retrieves the component registry.
  caf::replies_to<get_atom>::with<registry>,
  // The peering between two trackers A and B comprises 3 messages:
  // (1) B -> A: Respond to a peering request from new remote peer A.
  caf::replies_to<peer_atom, caf::actor, std::string>
    ::with<state_atom, registry>,
  // (2) A -> B: Confirm peering handshake after receiving state.
  caf::replies_to<state_atom, registry>::with<ok_atom>,
  // (3) A -> B: Broadcast own state to peers
  caf::reacts_to<state_atom, component_map_entry>
>;

/// Keeps track of the topology in a VAST deployment.
/// @param self The actor handle.
/// @param node The name of the local node.
tracker_type::behavior_type
tracker(tracker_type::stateful_pointer<tracker_state> self, std::string node);

} // namespace system
} // namespace vast

#endif

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

#pragma once

#include "vast/fwd.hpp"
#include "vast/optional.hpp"

#include <caf/replies_to.hpp>
#include <caf/stateful_actor.hpp>
#include <caf/typed_actor.hpp>

#include <map>
#include <string>

namespace vast::system {

// TODO: This had to be forward declared so we can send it over-the-wire. We do
// not want to include <map> and <string> in fwd.hpp, so this serves as a strong
// typedef around the actual inner type. We should clean this up.
struct component_state_map {
  std::multimap<std::string, component_state, std::less<>> value;

  template <class Inspector>
  friend auto inspect(Inspector& f, component_state_map& x) {
    return f(caf::meta::type_name("component_state_map"), x.value);
  }
};

// TODO: This had to be forward declared so we can send it over-the-wire. We do
// not want to include <map> and <string> in fwd.hpp, so this serves as a strong
// typedef around the actual inner type. We should clean this up.
struct component_map_entry {
  std::pair<std::string, component_state_map> value;

  template <class Inspector>
  friend auto inspect(Inspector& f, component_map_entry& x) {
    return f(caf::meta::type_name("component_map_entry"), x.value);
  }
};

// TODO: This had to be forward declared so we can send it over-the-wire. We do
// not want to include <map> and <string> in fwd.hpp, so this serves as a strong
// typedef around the actual inner type. We should clean this up.
struct component_map {
  std::map<std::string, component_state_map, std::less<>> value;

  template <class Inspector>
  friend auto inspect(Inspector& f, component_map& x) {
    return f(caf::meta::type_name("component_map"), x.value);
  }
};

/// State maintained per component.
struct component_state {
  caf::actor actor;
  std::string label;

  template <class Inspector>
  friend auto inspect(Inspector& f, component_state& x) {
    return f(caf::meta::type_name("component_state"), x.actor, x.label);
  }
};

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
  std::string node;
  vast::system::registry registry;
  static inline const char* name = "tracker";
};

using tracker_type = caf::typed_actor<
  // Adds a component.
  caf::replies_to<atom::put, std::string, caf::actor,
                  std::string>::with<atom::ok>,
  // Adds a component if it doesn't yet exist.
  caf::reacts_to<atom::try_put, std::string, caf::actor, std::string>,
  // Propagated PUT received from peer.
  caf::reacts_to<atom::put, std::string, std::string, caf::actor, std::string>,
  // Retrieves the component registry.
  caf::replies_to<atom::get>::with<registry>,
  // The peering between two trackers A and B comprises 3 messages:
  // (1) B -> A: Respond to a peering request from new remote peer A.
  caf::replies_to<atom::peer, caf::actor, std::string>::with<atom::state,
                                                             registry>,
  // (2) A -> B: Confirm peering handshake after receiving state.
  caf::replies_to<atom::state, registry>::with<atom::ok>,
  // (3) A -> B: Broadcast own state to peers
  caf::reacts_to<atom::state, component_map_entry>>;

/// Keeps track of the topology in a VAST deployment.
/// @param self The actor handle.
/// @param node The name of the local node.
tracker_type::behavior_type
tracker(tracker_type::stateful_pointer<tracker_state> self, std::string node);

} // namespace vast::system

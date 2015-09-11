#ifndef VAST_ACTOR_NODE_H
#define VAST_ACTOR_NODE_H

#include <map>
#include <string>

#include "vast/filesystem.h"
#include "vast/trial.h"
#include "vast/actor/actor.h"
#include "vast/actor/accountant.h"
#include "vast/actor/key_value_store.h"
#include "vast/util/system.h"

namespace vast {

/// A container for all other actors of a VAST process.
///
/// Each node stores its meta data in a key-value store.
///
/// The key space has the following structure:
///
///   - /actors/<node>/<label>/{actor, type}
///   - /peers/<node>/<node>
///   - /topology/<source>/<sink>
///
struct node : default_actor {
  /// Returns the path of the log directory relative to the base directory.
  /// @returns The directory where to write log and status messages to.
  static path const& log_path();

  /// Spawns a node.
  /// @param name The name of the node.
  /// @param dir The directory where to store persistent state.
  node(std::string const& name = util::hostname(), path const& dir = "vast");

  void on_exit() override;
  behavior make_behavior() override;

  //
  // Public message interface
  //

  behavior spawn_core(event_based_actor* self);
  behavior spawn_actor(event_based_actor* self);
  behavior send_run(event_based_actor* self);
  behavior send_flush(event_based_actor* self);
  behavior quit_actor(event_based_actor* self);
  behavior connect(event_based_actor* self);
  behavior disconnect(event_based_actor* self);
  behavior show(event_based_actor* self);
  behavior store_get_actor(event_based_actor* self);
  behavior request_peering(event_based_actor* self);
  behavior respond_to_peering(event_based_actor* self);

  //
  // Helpers
  //

  std::string qualify(std::string const& label) const;
  std::string make_actor_key(std::string const& label) const;
  std::string parse_actor_key(std::string const& key) const;

  behavior operating_;
  accountant::type accountant_;
  actor store_;
  std::string name_;
  path const dir_;
};

} // namespace vast

#endif

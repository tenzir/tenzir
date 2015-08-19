#ifndef VAST_ACTOR_NODE_H
#define VAST_ACTOR_NODE_H

#include <map>
#include <string>

#include "vast/filesystem.h"
#include "vast/trial.h"
#include "vast/actor/actor.h"
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
  caf::behavior make_behavior() override;

  //
  // Public message interface
  //

  caf::behavior spawn_actor(caf::event_based_actor* self);
  caf::behavior send_run(caf::event_based_actor* self);
  caf::behavior send_flush(caf::event_based_actor* self);
  caf::behavior quit_actor(caf::event_based_actor* self);
  caf::behavior connect(caf::event_based_actor* self);
  caf::behavior disconnect(caf::event_based_actor* self);
  caf::behavior show(caf::event_based_actor* self);
  caf::behavior store_get_actor(caf::event_based_actor* self);
  caf::behavior request_peering(caf::event_based_actor* self);
  caf::behavior respond_to_peering(caf::event_based_actor* self);

  //
  // Helpers
  //

  std::string qualify(std::string const& label) const;
  std::string make_actor_key(std::string const& label) const;
  std::string parse_actor_key(std::string const& key) const;

  caf::behavior operating_;
  caf::actor accountant_;
  caf::actor store_;
  std::map<std::string, caf::actor> peers_;
  std::map<std::string, caf::actor> actors_by_label_;
  std::multimap<std::string, caf::actor> actors_by_type_;
  std::string name_;
  path const dir_;
};

} // namespace vast

#endif

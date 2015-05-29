#ifndef VAST_ACTOR_NODE_H
#define VAST_ACTOR_NODE_H

#include "vast/filesystem.h"
#include "vast/actor/actor.h"
#include "vast/actor/key_value_store.h"
#include "vast/util/system.h"

namespace vast {

/// A container for all other actors of a VAST process.
///
/// Each node stores its meta data in a central store using the following key
/// space:
///
///   - /actors/<node>/handle/<label>
///   - /actors/<node>/type/<label>
///   - /peers/<node>/<label>
///   - /nodes/<node>/...
///   - /topology
///
struct node : default_actor
{
  /// Returns the path of the log directory relative to the base directory.
  /// @returns The directory where to write log and status messages to.
  static path const& log_path();

  /// Spawns a node.
  /// @param name The name of the node.
  /// @param dir The directory where to store persistent state.
  node(std::string const& name = util::hostname(), path const& dir = "vast");

  void on_exit();
  caf::behavior make_behavior() override;

  //
  // Public message interface
  //

  caf::message stop();
  caf::message request_peering(std::string const& endpoint);
  caf::message spawn_actor(caf::message const& msg);
  caf::message send_run(std::string const& arg);
  void send_flush(std::string const& arg);
  caf::message quit_actor(std::string const& arg);
  caf::message connect(std::string const& sources, std::string const& sinks);
  caf::message disconnect(std::string const& sources, std::string const& sinks);
  caf::message show(std::string const& arg);

  // Factored to reduce compiler memory footprint. See actor/node_spawn.cc.
  caf::message spawn_source(std::string const& label,
                            caf::message const& params);
  caf::message spawn_sink(std::string const& label, caf::message const& params);

  //
  // Helper functions to synchronously interact with the key-value store.
  //

  struct actor_state
  {
    caf::actor actor;
    std::string fqn;
    std::string type;
  };

  actor_state get(std::string const& label);
  caf::message put(actor_state const& state);
  bool has_topology_entry(std::string const& src, std::string const& snk);

  caf::actor accountant_;
  caf::actor store_;
  std::string name_;
  path const dir_;
};

} // namespace vast

#endif

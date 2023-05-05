#pragma once

#include "vast/fwd.hpp"

#include "vast/pipeline.hpp"
#include "vast/system/actors.hpp"

#include <caf/typed_event_based_actor.hpp>

namespace vast {

struct pipeline_executor_state {
  static constexpr auto name = "pipeline-executor";

  /// A pointer to the parent actor.
  system::pipeline_executor_actor::pointer self;

  /// The currently running pipeline.
  std::optional<pipeline> pipe;

  /// Number of execution nodes still alive. If this count goes to zero, we can
  /// finish the *rp_complete* promise that tracks whether the request is
  /// completed.
  size_t nodes_alive{0};

  /// A response promise responsible for delivering the result of *run*,
  caf::typed_response_promise<void> rp_complete;

  /// Temporary storage for the execution nodes, grouped by their host (local or
  /// remote). Used to transfer information between *spawn_execution_nodes* and
  /// *continue_if_done_spawning*.
  std::vector<std::vector<caf::actor>> hosts;

  // A mapping of execution node address to a string describing the execution
  // node, which enables better log messages.
  std::unordered_map<caf::actor_addr, std::string> node_descriptions;

  /// The number of in-flight remote spawn requests for execution nodes.
  size_t remote_spawn_count{0};

  /// Spawns a set of execution nodes, creating one execution node actor per
  /// operator. The operators must form a valid pipeline. The *remote* node
  /// actor may be nullptr if all operators' locations are local or anywhere.
  void spawn_execution_nodes(system::node_actor remote,
                             std::vector<operator_ptr> ops);

  /// A continuation of *spawn_execution_nodes* that must be called after all
  /// remote execution nodes were spawned.
  void continue_if_done_spawning();

  /// Start the pipeline execution. Assumes that all execution nodes were
  /// spawned successfully.
  auto run() -> caf::result<void>;
};

/// Start a pipeline executor for a given pipeline.
auto pipeline_executor(
  system::pipeline_executor_actor::stateful_pointer<pipeline_executor_state>
    self,
  pipeline p) -> system::pipeline_executor_actor::behavior_type;

} // namespace vast

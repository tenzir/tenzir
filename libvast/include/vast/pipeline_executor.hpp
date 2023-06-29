#pragma once

#include "vast/fwd.hpp"

#include "vast/actors.hpp"
#include "vast/diagnostics.hpp"
#include "vast/pipeline.hpp"

#include <caf/typed_event_based_actor.hpp>

namespace vast {

struct pipeline_executor_state {
  static constexpr auto name = "pipeline-executor";

  /// A pointer to the parent actor.
  pipeline_executor_actor::pointer self = {};

  // A handle to the node actor.
  node_actor node = {};

  /// The currently running pipeline.
  std::optional<pipeline> pipe = {};
  std::vector<exec_node_actor> exec_nodes = {};
  caf::typed_response_promise<void> start_rp = {};

  // The diagnostic handler that receives diagnostics from all the execution
  // nodes.
  std::unique_ptr<diagnostic_handler> diagnostics = {};

  /// Flag for allowing unsafe pipelines.
  bool allow_unsafe_pipelines = {};

  auto start() -> caf::result<void>;

  void start_nodes_if_all_spawned();

  void spawn_execution_nodes(pipeline pipe, node_actor remote);
};

/// Start a pipeline executor for a given pipeline.
auto pipeline_executor(
  pipeline_executor_actor::stateful_pointer<pipeline_executor_state> self,
  pipeline pipe, std::unique_ptr<diagnostic_handler> diagnostics,
  node_actor node) -> pipeline_executor_actor::behavior_type;

} // namespace vast

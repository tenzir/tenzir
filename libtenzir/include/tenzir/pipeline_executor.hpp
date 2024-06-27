#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/actors.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/pipeline.hpp"

#include <caf/typed_event_based_actor.hpp>

namespace tenzir {

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
  receiver_actor<diagnostic> diagnostics = {};

  // The metric handler that receives metrics from all the execution
  // nodes.
  metrics_receiver_actor metrics = {};

  /// Flag for disallowing location overrides.
  bool no_location_overrides = {};

  /// True if the locally-run nodes shall have access to the terminal.
  bool has_terminal = {};

  auto start() -> caf::result<void>;
  auto pause() -> caf::result<void>;
  auto resume() -> caf::result<void>;

  void start_nodes_if_all_spawned();

  void abort_start(caf::error reason);
  void abort_start(diagnostic reason);

  void finish_start();

  void spawn_execution_nodes(pipeline pipe);
};

/// Start a pipeline executor for a given pipeline.
auto pipeline_executor(
  pipeline_executor_actor::stateful_pointer<pipeline_executor_state> self,
  pipeline pipe, receiver_actor<diagnostic> diagnostics,
  metrics_receiver_actor metrics, node_actor node, bool has_terminal)
  -> pipeline_executor_actor::behavior_type;

} // namespace tenzir

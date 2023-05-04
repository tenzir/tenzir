#pragma once

#include "vast/fwd.hpp"

#include "vast/pipeline.hpp"
#include "vast/system/actors.hpp"

#include <caf/typed_event_based_actor.hpp>

namespace vast {

struct pipeline_executor_state {
  static constexpr auto name = "pipeline-executor";

  system::pipeline_executor_actor::pointer self;
  std::optional<pipeline> pipe;
  size_t nodes_alive{0};
  caf::typed_response_promise<void> rp_complete;
  std::vector<std::vector<caf::actor>> hosts;
  std::unordered_map<caf::actor_addr, std::string> node_descriptions;
  size_t remote_spawn_count{0};

  void continue_if_done_spawning();

  void spawn_execution_nodes(system::node_actor remote,
                             std::vector<operator_ptr> ops);

  auto run() -> caf::result<void>;
};

/// Start a pipeline executor for a given pipeline.
auto pipeline_executor(
  system::pipeline_executor_actor::stateful_pointer<pipeline_executor_state>
    self,
  pipeline p) -> system::pipeline_executor_actor::behavior_type;

} // namespace vast

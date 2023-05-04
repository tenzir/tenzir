#pragma once

#include "vast/fwd.hpp"

#include "vast/operator_control_plane.hpp"
#include "vast/pipeline.hpp"
#include "vast/system/actors.hpp"

#include <caf/typed_event_based_actor.hpp>

namespace vast {

struct execution_node_state {
  static constexpr auto name = "execution-node";

  /// Entry point for the source.
  auto start(std::vector<caf::actor> next) -> caf::result<void>;

  /// Entry point for stages and the sink.
  template <class Input>
  auto start(caf::stream<Input> in, std::vector<caf::actor> next)
    -> caf::result<caf::inbound_stream_slot<Input>>;

  operator_ptr op;
  system::execution_node_actor::pointer self;
  std::unique_ptr<operator_control_plane> ctrl;
  std::function<void(execution_node_state&, caf::error)> shutdown;
  bool is_shutting_down = false;
};

/// Start an execution node that wraps an operator for asynchronous execution.
/// Before spawning this actor, check whether `op->detached()` returns true, and
/// spawn the actor as a detached actor if desired.
auto execution_node(
  system::execution_node_actor::stateful_pointer<execution_node_state> self,
  operator_ptr op) -> system::execution_node_actor::behavior_type;

} // namespace vast

#pragma once

#include "vast/fwd.hpp"

#include "vast/operator_control_plane.hpp"
#include "vast/pipeline.hpp"
#include "vast/system/actors.hpp"

#include <caf/typed_event_based_actor.hpp>

namespace vast {

struct execution_node_state {
  static constexpr auto name = "execution-node";

  /// A pointer to the parent actor.
  system::execution_node_actor::pointer self;

  /// The operator owned by this execution node.
  operator_ptr op;

  /// A pointer to the control plane passed to this operator during execution,
  /// which allows operators to control this actor.
  std::unique_ptr<operator_control_plane> ctrl;

  /// The node actor (iff available).
  system::node_actor node;

  /// Entry point for the source.
  auto start(std::vector<caf::actor> next) -> caf::result<void>;

  /// Entry point for stages and the sink.
  /// @note This function is defined for all possible inputs in the
  /// corresponding execution_node.cpp file.
  template <class Input>
  auto start(caf::stream<framed<Input>> in, std::vector<caf::actor> next)
    -> caf::result<caf::inbound_stream_slot<framed<Input>>>;
};

/// Start an execution node that wraps an operator for asynchronous execution.
/// Before spawning this actor, check whether `op->detached()` returns true, and
/// spawn the actor as a detached actor if desired.
auto execution_node(
  system::execution_node_actor::stateful_pointer<execution_node_state> self,
  operator_ptr op, system::node_actor node)
  -> system::execution_node_actor::behavior_type;

} // namespace vast

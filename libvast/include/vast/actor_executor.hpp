#pragma once

#include "vast/operator_control_plane.hpp"

#include <vast/fwd.hpp>
#include <vast/pipeline.hpp>
#include <vast/system/actors.hpp>

#include <caf/typed_event_based_actor.hpp>

namespace vast {

///
void start_actor_executor(caf::event_based_actor* self, pipeline p,
                          std::function<void(caf::expected<void>)> callback);

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
  std::unique_ptr<operator_control_plane> ctrl; // TODO: remove unique_ptr
};

auto execution_node(
  system::execution_node_actor::stateful_pointer<execution_node_state> self,
  operator_ptr op) -> system::execution_node_actor::behavior_type;

} // namespace vast

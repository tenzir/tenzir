#pragma once

#include "vast/fwd.hpp"

#include "vast/actors.hpp"
#include "vast/pipeline.hpp"

#include <caf/typed_event_based_actor.hpp>

#include <queue>

namespace vast {

/// Spawns an execution node for the given operator and a known input type.
/// @param sys The actor system to spawn the actor in.
/// @param op The operator to execute.
/// @param input_type The input type to assume for the operator.
/// @param previous The previous execution node actor in the pipeline to pull
/// events from. Must be set if the input type is not void.
/// @param node The node actor to expose in the control plane. Must be set if
/// the operator runs at a remote node.
/// @param diagnostics_handler The handler asked to spawn diagnostics.
/// @returns The execution node actor and its output type, or an error.
/// @pre op != nullptr
/// @pre node != nullptr or not (op->location() == operator_location::remote)
/// @pre previous != nullptr or input_type.is<void>()
/// @pre diagnostics_handler != nullptr
auto spawn_exec_node(caf::actor_system& sys, operator_ptr op,
                     operator_type input_type, exec_node_actor previous,
                     node_actor node,
                     receiver_actor<diagnostic> diagnostics_handler)
  -> caf::expected<std::pair<exec_node_actor, operator_type>>;

} // namespace vast

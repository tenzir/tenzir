#pragma once

#include "vast/fwd.hpp"

#include "vast/actors.hpp"
#include "vast/pipeline.hpp"

#include <caf/typed_event_based_actor.hpp>

#include <queue>

namespace vast {

/// Spawns and monitors an execution node for the given operator and a known
/// input type.
/// @param self The actor that spawns and monitors the execution node.
/// @param op The operator to execute.
/// @param input_type The input type to assume for the operator.
/// @param node The node actor to expose in the control plane. Must be set if
/// the operator runs at a remote node.
/// @param diagnostics_handler The handler asked to spawn diagnostics.
/// @returns The execution node actor and its output type, or an error.
/// @pre op != nullptr
/// @pre node != nullptr or not (op->location() == operator_location::remote)
/// @pre diagnostics_handler != nullptr
auto spawn_exec_node(caf::scheduled_actor* self, operator_ptr op,
                     operator_type input_type, node_actor node,
                     receiver_actor<diagnostic> diagnostics_handler)
  -> caf::expected<std::pair<exec_node_actor, operator_type>>;

} // namespace vast

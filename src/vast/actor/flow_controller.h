#ifndef VAST_ACTOR_FLOW_CONTROLLER_H
#define VAST_ACTOR_FLOW_CONTROLLER_H

#include <map>

#include "vast/actor/atoms.h"
#include "vast/actor/basic_state.h"
#include "vast/util/flat_set.h"

namespace vast {

/// A flow controller maintains a DAG representing the data flow over a chain
/// of actors. By default, it forwards overload message to the root of the DAG
/// in order to throttle the sending rate of of the data source.
///
/// Intermediate nodes can inject themselves as *deflector* into the signal
/// processing, in which case each signal is instead sent to the intermediate
/// node. Only if the intermediate node sends it back to the flow controller,
/// it will flow upstream to the next.
///
/// For example, consider the topology:
///
///     A --> B --> C --> D
///
/// Actor *A* sends data to downstream to *B*, *B* to *C*, and *C* to *D*. As
/// soon as the flow controller becomes aware of the edges *(A,B)*, *(B,C)*,
/// and *(C,D)*, it forwards signals from *B/C/D* in one hop to the source *A*.
/// If *B* injects itself as deflector into the processing, then the flow
/// controller would deflect the overload signal to *B*. Only if *B* reflects
/// it back to the flow controller, it will propagate the signal to source *A*.
///
/// Note that all actors processing flow control signals should be spawned with
/// the `priority_aware` flag to minimize response times.
namespace flow_controller {

struct state : basic_state {
  state(event_based_actor* self);

  util::flat_set<actor> deflectors;
  std::multimap<actor, actor> graph; // Reverse edges: sink -> source.
};

// TODO: use type-safe edition once the state ctor issue has been resolved.
// wait until that gets resolved.
//using actor_type = typed_actor<
//  reacts_to<add_atom, actor, actor>,
//  reacts_to<add_atom, deflector_atom, actor>,
//  reacts_to<overload_atom>,
//  reacts_to<underload_atom>,
//  reacts_to<overload_atom, actor>,
//  reacts_to<underload_atom, actor>
//>;
//using behavior_type = actor_type::behavior_type;
//using stateful_pointer = actor_type::stateful_pointer<state>;
//
//behavior_type actor(stateful_pointer self);

behavior actor(stateful_actor<state>* self);

} // namespace flow_controller
} // namespace vast

#endif

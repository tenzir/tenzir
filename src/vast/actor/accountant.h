#ifndef VAST_ACTOR_ACCOUNTANT_H
#define VAST_ACTOR_ACCOUNTANT_H

#include <fstream>
#include <unordered_map>

#include "vast/filesystem.h"
#include "vast/optional.h"
#include "vast/time.h"
#include "vast/actor/basic_state.h"
#include "vast/util/accumulator.h"

namespace vast {
namespace accountant {

using value_type = uint64_t;

/// Writes out accounting data into a log file.
struct state : basic_state {
  struct context {
    value_type  x = 0;
    time::point begin{time::duration::zero()};
    time::moment last{time::extent::zero()};
    util::accumulator<value_type> accumulator;
  };

  path filename_;
  time::duration resolution_;
  std::ofstream file_;
  std::unordered_map<actor_addr, std::string> actors_;
  std::unordered_map<std::string, context> contexts_;

  state(local_actor* self);
  optional<value_type> accumulate(context& ctx, value_type x, time::moment t);
  void record(std::string const& context, value_type x, time::moment t);
};

using actor_type =
  typed_actor<
    reacts_to<std::string, time::point>,
    reacts_to<value_type, time::moment>,
    reacts_to<std::string, value_type, time::moment>
  >;
using behavior_type = actor_type::behavior_type;
using pointer = actor_type::pointer;
using stateful_pointer = actor_type::stateful_pointer<state>;

/// Spawns an accountant.
/// @param self The actor handle.
/// @param filename The path of the file containing the accounting details.
/// @param resolution The granularity at which to track values which get
///                   submitted incrementally.
behavior_type actor(stateful_pointer self, path filename,
                    time::duration resolution);

} // namespace accountant
} // namespace vast

#endif

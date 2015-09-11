#ifndef VAST_ACTOR_ACCOUNTANT_H
#define VAST_ACTOR_ACCOUNTANT_H

#include <fstream>
#include <unordered_map>

#include "vast/filesystem.h"
#include "vast/time.h"
#include "vast/actor/basic_state.h"

namespace vast {
namespace accountant {

/// Writes out accounting data into a log file.
struct state : basic_state {
  state(local_actor* self);

  void init(path const& filename);

  std::ofstream file_;
};

using actor_type =
  typed_actor<
    reacts_to<std::string, std::string, std::string>,
    reacts_to<std::string, std::string, time::extent>,
    reacts_to<std::string, std::string, int64_t>,
    reacts_to<std::string, std::string, uint64_t>,
    reacts_to<std::string, std::string, double>
  >;
using behavior_type = actor_type::behavior_type;
using pointer = actor_type::pointer;
using stateful_pointer = actor_type::stateful_pointer<state>;

/// Spawns an accountant.
/// @param self The actor handle.
/// @param filename The path of the file containing the accounting details.
behavior_type actor(stateful_pointer self, path const& filename);

} // namespace accountant
} // namespace vast

#endif

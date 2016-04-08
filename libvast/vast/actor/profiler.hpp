#ifndef VAST_ACTOR_PROFILER_HPP
#define VAST_ACTOR_PROFILER_HPP

#include <chrono>
#include <fstream>

#include "vast/filesystem.hpp"
#include "vast/actor/basic_state.hpp"

namespace vast {

struct profiler {
  struct state : basic_state {
    state(local_actor* self);
    ~state();
  };

  /// Profiles CPU and heap via gperftools.
  /// @param self The actor context.
  /// @param log_dir The directory where to write profiler output to.
  /// @param secs The number of seconds between subsequent measurements.
  static behavior make(stateful_actor<state>* self,
                       path log_dir, std::chrono::seconds secs);
};

} // namespace vast

#endif

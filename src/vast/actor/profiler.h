#ifndef VAST_ACTOR_PROFILER_H
#define VAST_ACTOR_PROFILER_H

#include <chrono>
#include <fstream>

#include "vast/filesystem.h"
#include "vast/actor/basic_state.h"

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

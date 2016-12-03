#ifndef VAST_SYSTEM_PROFILER_HPP
#define VAST_SYSTEM_PROFILER_HPP

#include <chrono>

#include <caf/stateful_actor.hpp>

namespace vast {

class path;

namespace system {

struct profiler_state {
  const char* name = "profiler";
};

/// Profiles CPU and heap usage via gperftools.
/// @param self The actor handle.
/// @param dir The directory where to write profiler output to.
/// @param secs The number of seconds between subsequent measurements.
caf::behavior profiler(caf::stateful_actor<profiler_state>* self, path dir,
                       std::chrono::seconds secs);

} // namespace system
} // namespace vast

#endif

#ifndef VAST_ACTOR_PROFILER_H
#define VAST_ACTOR_PROFILER_H

#include <chrono>
#include <fstream>
#include "vast/filesystem.h"
#include "vast/actor/actor.h"

namespace vast {

/// Profiles CPU and heap via gperftools.
struct profiler : default_actor
{
  /// Spawns the profiler.
  /// @param log_dir The directory where to write profiler output to.
  /// @param secs The number of seconds between subsequent measurements.
  profiler(path log_dir, std::chrono::seconds secs);

  void on_exit() override;
  caf::behavior make_behavior() override;

  path const log_dir_;
  std::chrono::seconds secs_;
};

} // namespace vast

#endif

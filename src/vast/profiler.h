#ifndef VAST_PROFILER_H
#define VAST_PROFILER_H

#include <chrono>
#include <fstream>
#include "vast/actor.h"
#include "vast/file_system.h"

namespace vast {

/// A simple CPU profiler.
class profiler : public actor_base
{
public:
  /// A resoure measurement.
  struct measurement
  {
    /// Measures the current system usage at construction time.
    measurement();

    double clock; ///< Current wall clock time (`gettimeofday`).
    double usr;   ///< Time spent in the process.
    double sys;   ///< Time spent in the kernel.

    friend std::ostream& operator<<(std::ostream& out, measurement const& s);
  };

  /// Spawns the profiler.
  /// @param log_dir The directory where to write profiler output to.
  /// @param secs The number of seconds between subsequent measurements.
  profiler(path log_dir, std::chrono::seconds secs);

  cppa::behavior act() final;
  std::string describe() const final;

private:
  path const log_dir_;
  std::ofstream file_;
  std::chrono::seconds secs_;
};

} // namespace vast

#endif

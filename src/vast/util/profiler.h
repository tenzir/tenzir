#ifndef VAST_UTIL_PROFILER_H
#define VAST_UTIL_PROFILER_H

#include <chrono>
#include <cppa/cppa.hpp>
#include "vast/fs/fstream.h"

namespace vast {
namespace util {

/// A simple CPU profiler.
class profiler : public cppa::sb_actor<profiler>
{
  friend class cppa::sb_actor<profiler>;

  /// A resoure measurement.
  struct measurement
  {
    /// Measures the current system usage at construction time.
    measurement();

    double clock; ///< Current wall clock time (`gettimeofday`).
    double usr;   ///< Time spent in the process.
    double sys;   ///< Time spent in the kernel.
  };

  friend std::ostream& operator<<(std::ostream& out, measurement const& s);

public:
  /// Spawns the profiler.
  /// @param filename The log file where to write measurements to.
  /// @param secs The number of seconds between subsequent measurements.
  profiler(std::string const& filename, std::chrono::seconds secs);

private:
  fs::ofstream file_;
  cppa::behavior init_state;
};

} // namespace util
} // namespace vast

#endif

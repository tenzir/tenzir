#ifndef VAST_UTIL_PROFILER_H
#define VAST_UTIL_PROFILER_H

#include <boost/asio/basic_waitable_timer.hpp>
#include <boost/operators.hpp>
#include "vast/fs/fstream.h"
#include "vast/fs/path.h"
#include "vast/util/time.h"

namespace vast {
namespace util {

/// A resoure measurement.
struct measurement : boost::addable<measurement>
                   , boost::subtractable<measurement>
{
    /// Measures the current system usage at construction time.
    measurement();

    measurement& operator+=(measurement const& rhs);
    measurement& operator-=(measurement const& rhs);

    double clock;           ///< Current wall clock time (@c gettimeofday).
    double usr_time;        ///< Time spent in the process.
    double sys_time;        ///< Time spent in the kernel.
};

std::ostream& operator<<(std::ostream& out, measurement const& s);

/// A simple CPU profiler.
class profiler
{
public:
    /// Constructs the profiler.
    /// @param io_service The Asio I/O service object.
    profiler(boost::asio::io_service& io_service);

    /// Initializes the profiler.
    /// @param filename The log file where to write measurements to.
    /// @param interval How often to take a measurment.
    void init(fs::path const& filename,
              util::duration interval = std::chrono::seconds(1));

    /// Starts the profiler.
    void start();

    /// Stops the profiler.
    void stop();

private:
    void handle_timer(boost::system::error_code const& ec,
                      measurement const& previous);

    util::duration interval_;
    boost::asio::basic_waitable_timer<util::clock> timer_;
    fs::ofstream file_;
};

} // namespace util
} // namespace vast

#endif

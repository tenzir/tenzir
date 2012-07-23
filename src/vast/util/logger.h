#ifndef VAST_UTIL_LOGGER_H
#define VAST_UTIL_LOGGER_H

#include <mutex>
#include <sstream>
#include <vast/fs/fstream.h>
#include <vast/fs/path.h>

namespace vast {
namespace util {
class logger;
extern logger* LOGGER;
}
}

/// Basic logging macro.
#define LOG(level, facility)                                        \
    if (::vast::util::LOGGER->takes(vast::util::logger::level))     \
        ::vast::util::logger::record(*::vast::util::LOGGER,         \
                                     vast::util::logger::level,     \
                                     vast::util::logger::facility)

/// Debugging logging macro.
#ifdef VAST_DEBUG
#define DBG(facility)                                               \
    if (::vast::util::LOGGER->takes(vast::util::logger::debug))     \
        ::vast::util::logger::record(*::vast::util::LOGGER,         \
                                     vast::util::logger::debug,     \
                                     vast::util::logger::facility)
#else
#undef DBG
#endif

namespace vast {
namespace log {

} // namespace log
} // namespace vast

namespace vast {
namespace util {

/// A multi-sink logger.
class logger
{
    logger(logger const&) = delete;
    logger& operator=(logger) = delete;

public:
    enum facility : int
    {
        core     = 0,
        broccoli = 1,
        comm     = 2,
        event    = 3,
        ingest   = 4,
        meta     = 5,
        query    = 6,
        store    = 7
    };

    enum level : int
    {
        quiet   = 0,
        fatal   = 1,
        error   = 2,
        warn    = 3,
        info    = 4,
        verbose = 5,
        debug   = 6
    };

    /// A generic sink referencing an existing output stream.
    class sink
    {
        friend class logger;

    public:
        sink(level lvl, std::ostream& out);

        bool takes(level lvl);

        template <typename Source>
        void write(Source const& src)
        {
            std::lock_guard<std::mutex> lock(mutex_);
            out_ << src << std::endl;
        }

    private:
        level level_;
        std::ostream& out_;
        std::mutex mutex_;
    };

    /// A sink that writes records to a file.
    class file_sink : public sink
    {
    public:
        file_sink(level lvl, fs::path file);
        ~file_sink();

    private:
        fs::ofstream file_;
    };

    /// A single formatted log line flushed upon destruction.
    class record
    {
        friend class logger;

    public:
        record(logger& log, level lvl, facility fac);
        ~record();

        template <typename T>
        record& operator<<(T const& x)
        {
            stream_ << x;
            return *this;
        }

    private:
        logger& logger_;
        level level_;
        std::stringstream stream_;
    };

    /// Constructs the logger.
    /// @param console_verbosity The console verbosity.
    /// @param logfile_verbosity The logfile verbosity.
    /// @param logfile The file where to log to.
    logger(level console_verbosity,
           level logfile_verbosity,
           fs::path const& logfile);

    /// Tests whether the logger processes a certain log level.
    ///
    /// @param lvl The level to test.
    ///
    /// @return `true` if the logger has at least one sink that operates at
    /// @a lvl.
    bool takes(level lvl);

    /// Gets a reference to the console output stream.
    /// @return A reference to the `std::ostream` of the console.
    std::ostream& console() const;

private:
    /// Writes a record to the relevant sinks.
    /// @param rec The record to dispatch.
    void write(record const& rec);

    sink console_;
    file_sink logfile_;
};

bool operator<(logger::level x, logger::level y);
bool operator<=(logger::level x, logger::level y);
bool operator>=(logger::level x, logger::level y);
bool operator>(logger::level x, logger::level y);

std::ostream& operator<<(std::ostream& out, logger::facility f);
std::ostream& operator<<(std::ostream& out, logger::level l);

} // namespace util
} // namespace vast

#endif

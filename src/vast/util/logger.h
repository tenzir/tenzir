#ifndef VAST_UTIL_LOGGER_H
#define VAST_UTIL_LOGGER_H

#include <memory>
#include <boost/log/sinks.hpp>
#include <boost/log/sources/record_ostream.hpp>
#include <boost/log/sources/severity_channel_logger.hpp>
#include <boost/noncopyable.hpp>
#include "vast/fs/path.h"
#include "config.h"

namespace vast {
namespace util {
class logger;
extern logger* LOGGER;
}
}

// Standard logging macro.
#define LOG(level, facility)                                                 \
    BOOST_LOG_SEV(                                                           \
            ::vast::util::LOGGER->get(vast::log::facility), vast::log::level)

/// Conditional logging. Only execute the subsequent statement when the given
/// severity level is active.
#define LOG_BLOCK(level, facility)                                      \
    if (::vast::util::LOGGER->get(log::facility)                        \
        .open_record(boost::log::keywords::severity = vast::log::level))

namespace vast {
namespace log {

/// Logging facility.
enum facility
{
    core,
    broccoli,
    comm,
    event,
    fastbit,
    meta,
    query,
    store
};

/// Logging severity level.
/// \note Must be kept in sync with the corresponding ostream operator.
enum level
{
    fatal,      ///< Unrecoverable errors.
    error,      ///< General errors.
    warn,       ///< Warnings.
    info,       ///< Informational messages.
    verbose,    ///< Verbose.
    debug       ///< Debug information.
};

/// String description of the log levels.
static char const* const str_lvl[] =
{
    "fatal",
    "error",
    "warning",
    "info",
    "verbose",
    "debug"
};

// The formatting logic for the severity level.
template <typename CharT, typename TraitsT>
inline std::basic_ostream<CharT, TraitsT>& operator<<(
std::basic_ostream<CharT, TraitsT>& out, level l)
{
    if (static_cast<std::size_t>(l) < (sizeof(str_lvl) / sizeof(*str_lvl)))
        out << str_lvl[l];
    else
        out << static_cast<int>(l);

    return out;
}

} // namespace log
} // namespace vast

namespace vast {
namespace util {

/// A flexible logger supporting multiple sinks.
class logger : boost::noncopyable
{
    typedef boost::log::sinks::text_file_backend file_backend;
#ifdef BOOST_LOG_NO_THREADS
    typedef boost::log::sinks::unlocked_sink<file_backend> file_sink;
#else
    typedef boost::log::sinks::synchronous_sink<file_backend> file_sink;
#endif

public:
    /// A logger type that supports both levels and facilities.
#ifdef BOOST_LOG_NO_THREADS
    typedef boost::log::sources::severity_channel_logger<
        log::level, std::string> logger_t;
#else
    typedef boost::log::sources::severity_channel_logger_mt<
        log::level, std::string> logger_t;
#endif

public:
    /// Constructor. Create thread-safe logging sources for each facility and
    /// initialize the sinks.
    /// \param cverb The console verbosity.
    /// \param fverb The logfile verbosity.
    /// \param log_dir The logging directory.
    logger(int cverb, int fverb, fs::path const& log_dir);

    /// Get a logger object for a given facility.
    /// \param fac The facility.
    /// \return The logger object of type logger_t.
    logger_t& get(log::facility fac);

private:
    logger_t core_;
    logger_t broccoli_;
    logger_t comm_;
    logger_t event_;
    logger_t fastbit_;
    logger_t meta_;
    logger_t query_;
    logger_t store_;
};

} // namespace util
} // namespace vast

#endif

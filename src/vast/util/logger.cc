#include "vast/util/logger.h"

#include <boost/assert.hpp>
#include <boost/log/attributes/clock.hpp>
#include <boost/log/formatters/attr.hpp>
#include <boost/log/formatters/date_time.hpp>
#include <boost/log/formatters/format.hpp>
#include <boost/log/formatters/message.hpp>
#include <boost/log/utility/init/to_console.hpp>
#include "vast/fs/operations.h"

namespace vast {
namespace util {

logger::logger(int cverb, int fverb, fs::path const& log_dir)
  : core_(boost::log::keywords::channel = "core")
  , broccoli_(boost::log::keywords::channel = "broccoli")
  , comm_(boost::log::keywords::channel = "comm")
  , event_(boost::log::keywords::channel = "event")
  , ingest_(boost::log::keywords::channel = "ingest")
  , meta_(boost::log::keywords::channel = "meta")
  , query_(boost::log::keywords::channel = "query")
  , store_(boost::log::keywords::channel = "store")
{
    namespace fmt = boost::log::formatters;

    auto core = boost::log::core::get();

    // Add a timestamps to both console and logfiles.
    boost::log::attributes::local_clock timestamp;
    core->add_global_attribute("TimeStamp", timestamp);

    // Initialize the console.
    auto console = boost::log::init_log_to_console();
    console->set_filter(
            boost::log::filters::attr<log::level>("Severity") <= cverb);
    console->locked_backend()->auto_flush(true);
    console->set_formatter(
            fmt::format("%1% %|2$-10| %3%")
                % fmt::date_time<boost::posix_time::ptime>("TimeStamp",
                    "%Y-%m-%d %H:%M:%S.%f")
                % fmt::attr<std::string>("Channel", "[%x]")
                % fmt::message()
            );

    // Initialize the logfiles.
    if (! fs::exists(log_dir))
        fs::mkdir(log_dir);

    // FIXME: Why does std::make_shared does not work here?
    auto backend = boost::make_shared<file_backend>(
        boost::log::keywords::file_name = log_dir / "vast.log",
        boost::log::keywords::open_mode =
        (std::ios_base::out | std::ios_base::app));

    auto log_file = boost::make_shared<file_sink>(backend);

    log_file->set_filter(
        boost::log::filters::attr<log::level>("Severity") <= fverb);
    log_file->set_formatter(
        fmt::format("%1% %|2$-10| %3%")
        % fmt::date_time<boost::posix_time::ptime>("TimeStamp",
                                                   "%Y-%m-%d %H:%M:%S.%f")
        % fmt::attr<std::string>("Channel", "[%x]")
        % fmt::message()
        );

    core->add_sink(log_file);
}

logger::logger_t& logger::get(log::facility f)
{
    switch (f)
    {
        case log::core:
            return core_;
            break;
        case log::broccoli:
            return broccoli_;
            break;
        case log::comm:
            return comm_;
            break;
        case log::event:
            return event_;
            break;
        case log::ingest:
            return ingest_;
            break;
        case log::meta:
            return meta_;
            break;
        case log::query:
            return query_;
            break;
        case log::store:
            return store_;
            break;
        default:
            BOOST_ASSERT(! "invalid logging facility");
    };
}

} // namespace util
} // namespace vast

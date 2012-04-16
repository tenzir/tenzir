#include "vast/program.h"

#include <cstdlib>
#include <iostream>
#include <boost/exception/diagnostic_information.hpp>
#include <ze/event.h>   // FIXME: debugging only
#include "vast/exception.h"
#include "vast/fs/path.h"
#include "vast/fs/operations.h"
#include "vast/meta/taxonomy.h"
#include "vast/query/query.h"
#include "vast/util/logger.h"
#include "config.h"

#ifdef USE_PERFTOOLS
#include "google/profiler.h"
#include "google/heap-profiler.h"
#endif

namespace vast {

/// Declaration of global (extern) variables.
namespace util {
logger* LOGGER;
}

program::program()
  : terminating_(false)
  , return_(EXIT_SUCCESS)
  , ingestor_(io_)
  , archive_(io_, ingestor_)
  , search_(io_, archive_)
  , query_client_(io_)
  , profiler_(io_.service())
{
}

program::~program()
{
}

bool program::init(std::string const& filename)
{
    try
    {
        config_.load(filename);
        do_init();
        return true;
    }
    catch (boost::program_options::unknown_option const& e)
    {
        std::cerr << e.what();
    }

    return false;
}

bool program::init(int argc, char *argv[])
{
    try
    {
        config_.load(argc, argv);

        if (argc < 2 || config_.check("help") || config_.check("advanced"))
        {
            config_.print(std::cerr, config_.check("advanced"));
            return false;
        }

        do_init();
        return true;
    }
    catch (config_exception const& e)
    {
        std::cerr << e.what() << std::endl;
    }
    catch (boost::program_options::unknown_option const& e)
    {
        std::cerr << e.what() << ", try -h or --help" << std::endl;
    }
    catch (boost::exception const& e)
    {
        std::cerr << boost::diagnostic_information(e);
    }

    return false;
}

void program::start()
{
    try
    {
        auto vast_dir = config_.get<fs::path>("vast-dir");
        auto log_dir = config_.get<fs::path>("log-dir");

        if (config_.check("profile"))
        {
            auto const& filename = log_dir / "profiler.log";

            auto interval = config_.get<unsigned>("profiler-interval");
            profiler_.init(filename, std::chrono::milliseconds(interval));

            profiler_.start();
        }

#ifdef USE_PERFTOOLS
        if (config_.check("perftools-heap"))
        {
            LOG(info, core) << "starting perftools CPU profiler";
            ::HeapProfilerStart((log_dir / "heap.profile").string().c_str());
        }
        if (config_.check("perftools-cpu"))
        {
            LOG(info, core) << "starting perftools heap profiler";
            ::ProfilerStart((log_dir / "cpu.profile").string().c_str());
        }
#endif

        if (config_.check("taxonomy"))
        {
            auto const& taxonomy = config_.get<fs::path>("taxonomy");
            tax_manager_.init(taxonomy);

            if (config_.check("dump-taxonomy"))
            {
                std::cout << tax_manager_.get()->to_string();
                return;
            }
        }

        comm::broccoli::init(config_.check("broccoli-messages"),
                             config_.check("broccoli-calltrace"));

        if (config_.check("comp-archive"))
        {
            archive_.init(
                vast_dir / "archive",
                config_.get<size_t>("archive.max-events-per-chunk"),
                config_.get<size_t>("archive.max-segment-size") * 1000,
                config_.get<size_t>("archive.max-segments"));
        }

        if (config_.check("comp-ingestor"))
        {
            std::vector<std::string> events;
            if (config_.check("ingestor.events"))
                events = config_.get<std::vector<std::string>>("ingestor.events");

            ingestor_.source.init(config_.get<std::string>("ingestor.host"),
                                  config_.get<unsigned>("ingestor.port"));

            for (const auto& event : events)
            {
                LOG(verbose, store) << "subscribing to event " << event;
                ingestor_.source.subscribe(event);
            }
        }

        if (config_.check("comp-search"))
        {
            search_.init(config_.get<std::string>("search.host"),
                         config_.get<unsigned>("search.port"));
        }

        if (config_.check("query"))
        {
            query_client_.init(config_.get<std::string>("search.host"),
                               config_.get<unsigned>("search.port"));

            query_client_.submit(config_.get<std::string>("query"));
        }

        auto threads = config_.get<unsigned>("threads");
        io_.start(errors_, threads);

        std::exception_ptr error = errors_.pop();
        if (error)
            std::rethrow_exception(error);
    }
    catch (...)
    {
        LOG(fatal, core)
            << "exception details:" << std::endl
            << boost::current_exception_diagnostic_information();

        return_ = EXIT_FAILURE;
    }

    if (! terminating_)
        stop();
}

void program::stop()
{
    if (! terminating_)
    {
        terminating_ = true;
        LOG(verbose, core) << "writing out in-memory state";

#ifdef USE_PERFTOOLS
        if (config_.check("perftools-cpu"))
        {
            LOG(info, core) << "stopping perftools CPU profiler";
            ::ProfilerStop();
        }
        if (config_.check("perftools-heap") && ::IsHeapProfilerRunning())
        {
            LOG(info, core) << "stopping perftools heap profiler";
            ::HeapProfilerDump("cleanup");
            ::HeapProfilerStop();
        }
#endif

        if (config_.check("query"))
            query_client_.stop();

        if (config_.check("comp-search"))
            search_.stop();

        if (config_.check("comp-ingestor"))
            ingestor_.source.stop();

        if (config_.check("comp-archive"))
            archive_.stop();

        if (config_.check("profile"))
            profiler_.stop();

        io_.stop();

        LOG(verbose, core) << "state saved";

        if (errors_.empty())
            errors_.push(std::exception_ptr());
    }
    else
    {
        io_.terminate();
    }
}

int program::end()
{
    switch (return_)
    {
        case EXIT_SUCCESS:
            LOG(verbose, core) << "VAST terminated cleanly";
            break;

        case EXIT_FAILURE:
            LOG(verbose, core) << "VAST terminated with errors";
            break;

        default:
            assert(! "invalid return code");
    }

    return return_;
}

void program::do_init()
{
    auto vast_dir = config_.get<fs::path>("vast-dir");
    if (! fs::exists(vast_dir))
        fs::mkdir(vast_dir);

    util::LOGGER = new util::logger(
        config_.get<int>("console-verbosity"),
        config_.get<int>("log-verbosity"),
        config_.get<fs::path>("log-dir"));

    LOG(info, core) << " _   _____   __________";
    LOG(info, core) << "| | / / _ | / __/_  __/";
    LOG(info, core) << "| |/ / __ |_\\ \\  / / ";
    LOG(info, core) << "|___/_/ |_/___/ /_/  " << VAST_VERSION;
    LOG(info, core) << "";
}

} // namespace vast

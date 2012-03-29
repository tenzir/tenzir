#include "vast/program.h"

#include <cstdlib>
#include <iostream>
#include <boost/exception/diagnostic_information.hpp>
#include "vast/exception.h"
#include "vast/fs/path.h"
#include "vast/fs/operations.h"
#include "vast/meta/taxonomy.h"
#include "vast/util/logger.h"
#include "vast/util/make_unique.h"
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
  , emit_(io_)
  , ingest_(io_)
  , query_(io_)
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

        if (config_.check("comp-ingest"))
        {
            std::vector<std::string> events;
            if (config_.check("ingest.events"))
                events = config_.get<std::vector<std::string>>("ingest.events");

            ingest_.source.init(config_.get<std::string>("ingest.ip"),
                                config_.get<unsigned>("ingest.port"));

            for (const auto& event : events)
            {
                LOG(verbose, store) << "subscribing to event " << event;
                ingest_.source.subscribe(event);
            }

            ingest_.archiver.init(
                vast_dir / "archive",
                config_.get<size_t>("ingest.max-chunk-events"),
                config_.get<size_t>("ingest.max-segment-size") * 1000);
        }


        if (config_.check("comp-query"))
        {
            query_.processor.init();

            if (config_.check("comp-emit"))
            {
                LOG(verbose, core) << "linking event loader with query processor";
                query_.link(emit_.loader, query_.processor);
            }
        }

        io_.start(errors_);

        if (config_.check("comp-emit"))
        {
            emit_.loader.init(vast_dir / "archive");
            emit_.loader.run();
        }

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

        if (config_.check("comp-ingest"))
            ingest_.source.stop();

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

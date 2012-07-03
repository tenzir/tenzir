#include "vast/program.h"

#include <cstdlib>
#include <iostream>
#include <boost/exception/diagnostic_information.hpp>
#include "vast/exception.h"
#include "vast/fs/path.h"
#include "vast/fs/operations.h"
#include "vast/ingest/reader.h"
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
  , archive_(io_)
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

            if (config_.check("print-taxonomy"))
            {
                std::cout << tax_manager_.get().to_string();
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
            ingestor_.source.to(archive_.subscriber());
            ingestor_.source.init(config_.get<std::string>("ingestor.host"),
                                  config_.get<unsigned>("ingestor.port"));

            if (config_.check("ingestor.file"))
            {
                auto files = config_.get<std::vector<fs::path>>("ingestor.file");
                for (auto& file : files)
                {
                    LOG(info, core) << "ingesting " << file;
                    auto reader = ingestor_.make_reader(file);
                    reader->to(archive_.subscriber());
                    reader->start();
                }
            }

            if (config_.check("ingestor.events"))
            {
                auto events = config_.get<std::vector<std::string>>("ingestor.events");
                for (auto& event : events)
                {
                    LOG(verbose, store) << "subscribing to event " << event;
                    ingestor_.source.subscribe(event);
                }
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
                               config_.get<unsigned>("search.port"),
                               config_.get<std::string>("query"),
                               config_.get<unsigned>("client.batch-size"));
        }

        auto threads = config_.get<unsigned>("threads");
        io_.start(threads, errors_);

        if (config_.check("query"))
        {
            query_client_.wait_for_input();
            errors_.push(std::exception_ptr());
        }

        std::exception_ptr error = errors_.pop();
        if (error)
            std::rethrow_exception(error);
    }
    catch (...)
    {
        LOG(fatal, core)
            << "exception details:\n"
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
        {
            LOG(debug, core) << "stopping queries";
            query_client_.stop();
        }

        if (config_.check("comp-search"))
        {
            LOG(debug, core) << "stopping search component";
            search_.stop();
        }

        if (config_.check("comp-ingestor"))
        {
            LOG(debug, core) << "stopping ingestor component";
            ingestor_.stop();
        }

        // This extra time allows in-flight events from the ingestor to be
        // processed by the archive.
        sleep(1);

        if (config_.check("comp-archive"))
        {
            LOG(debug, core) << "stopping archive component";
            archive_.stop();
        }

        if (config_.check("profile"))
        {
            LOG(debug, core) << "stopping profiler";
            profiler_.stop();
        }

        LOG(debug, core) << "stopping I/O";
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
        static_cast<util::logger::level>(config_.get<int>("console-verbosity")),
        static_cast<util::logger::level>(config_.get<int>("logfile-verbosity")),
        config_.get<fs::path>("log-dir") / "vast.log");

    LOG(verbose, core) << " _   _____   __________";
    LOG(verbose, core) << "| | / / _ | / __/_  __/";
    LOG(verbose, core) << "| |/ / __ |_\\ \\  / / ";
    LOG(verbose, core) << "|___/_/ |_/___/ /_/  " << VAST_VERSION;
    LOG(verbose, core) << "";
}

} // namespace vast

#include <vast/program.h>

#include <cstdlib>
#include <iostream>
#include <boost/exception/diagnostic_information.hpp>
#include <vast/exception.h>
#include <vast/comm/broccoli.h>
#include <vast/detail/cppa_type_info.h>
#include <vast/fs/path.h>
#include <vast/fs/operations.h>
#include <vast/ingest/ingestor.h>
#include <vast/meta/schema_manager.h>
#include <vast/query/client.h>
#include <vast/query/search.h>
#include <vast/store/archive.h>
#include <vast/util/logger.h>
#include <config.h>


#ifdef USE_PERFTOOLS
#include <google/profiler.h>
#include <google/heap-profiler.h>
#endif

namespace vast {

/// Declaration of global (extern) variables.
namespace util {
logger* LOGGER;
}

program::program()
  : terminating_(false)
  , return_(EXIT_SUCCESS)
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
  using namespace cppa;
  detail::cppa_announce_types();

  try
  {
    auto log_dir = config_.get<fs::path>("log-dir");

#ifdef USE_PERFTOOLS
    if (config_.check("perftools-heap"))
    {
      LOG(info, core) << "starting perftools CPU profiler";
      ::HeapProfilerStart((log_dir / "heap.profile").string().data());
    }
    if (config_.check("perftools-cpu"))
    {
      LOG(info, core) << "starting perftools heap profiler";
      ::ProfilerStart((log_dir / "cpu.profile").string().data());
    }
#endif

    if (config_.check("profile"))
    {
      auto const& filename = log_dir / "profiler.log";
      auto interval = config_.get<unsigned>("profiler-interval");
      profiler_.init(filename, std::chrono::milliseconds(interval));
      profiler_.start();
    }

    schema_manager_ = spawn<meta::schema_manager>();
    if (config_.check("schema"))
    {
      send(schema_manager_, atom("load"), config_.get<std::string>("schema"));

      if (config_.check("print-schema"))
      {
        send(schema_manager_, atom("print"));
        receive(
            on(atom("schema"), arg_match) >> [](std::string const& schema)
            {
              std::cout << schema << std::endl;
            });

        return;
      }
    }

    comm::broccoli::init(config_.check("broccoli-messages"),
                         config_.check("broccoli-calltrace"));

    if (config_.check("comp-archive"))
    {
      archive_ = spawn<store::archive>(
          (config_.get<fs::path>("vast-dir") / "archive").string(),
          config_.get<size_t>("archive.max-events-per-chunk"),
          config_.get<size_t>("archive.max-segment-size") * 1000,
          config_.get<size_t>("archive.max-segments"));
    }
    else
    {
      archive_ = remote_actor(
          config_.get<std::string>("archive.host"),
          config_.get<unsigned>("archive.port"));
    }

    if (config_.check("comp-ingestor"))
    {
      ingestor_ = spawn<ingest::ingestor>(archive_);
      send(ingestor_,
           atom("initialize"),
           config_.get<std::string>("ingestor.host"),
           config_.get<unsigned>("ingestor.port"));

      if (config_.check("ingestor.events"))
      {
        auto events = config_.get<std::vector<std::string>>("ingestor.events");
        for (auto& event : events)
        {
          LOG(verbose, store) << "subscribing to event " << event;
          send(ingestor_, atom("subscribe"), event);
        }
      }

      if (config_.check("ingestor.file"))
      {
        auto files = config_.get<std::vector<std::string>>("ingestor.file");
        for (auto& file : files)
        {
          LOG(info, core) << "ingesting " << file;
          send(ingestor_, atom("read_file"), file);
        }
      }
    }
    else
    {
      ingestor_ = remote_actor(
          config_.get<std::string>("ingestor.host"),
          config_.get<unsigned>("ingestor.port"));
    }

    if (config_.check("comp-search"))
    {
      search_ = spawn<query::search>(archive_);
      send(search_,
           atom("publish"),
           config_.get<std::string>("search.host"),
           config_.get<unsigned>("search.port"));
    }
    else
    {
      search_ = remote_actor(
          config_.get<std::string>("search.host"),
          config_.get<unsigned>("search.port"));
    }

    if (config_.check("query"))
    {
      query_client_ = spawn<query::client>(
          search_,
          config_.get<unsigned>("client.batch-size"));

      send(query_client_,
           atom("query"),
           atom("create"),
           config_.get<std::string>("query"));
    }

    await_all_others_done();
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
  using namespace cppa;

  if (terminating_)
  {
    return_ = EXIT_FAILURE;
    return;
  }

  terminating_ = true;
  LOG(verbose, core) << "writing out in-memory state";

  auto shutdown = make_any_tuple(atom("shutdown"));

  if (config_.check("query"))
  {
    LOG(debug, core) << "stopping queries";
    query_client_ << shutdown;
  }

  if (config_.check("comp-search"))
  {
    LOG(debug, core) << "stopping search component";
    search_ << shutdown;
  }

  if (config_.check("comp-ingestor"))
  {
    LOG(debug, core) << "stopping ingestor component";
    ingestor_ << shutdown;
  }

  if (config_.check("comp-archive"))
  {
    LOG(debug, core) << "stopping archive component";
    archive_ << shutdown;
  }

  if (config_.check("profile"))
  {
    LOG(debug, core) << "stopping profiler";
    profiler_.stop();
  }

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

  LOG(verbose, core) << "state saved";

  return_ = EXIT_SUCCESS;

  await_all_others_done();
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

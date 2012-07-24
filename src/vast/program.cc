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
#include <vast/store/index.h>
#include <vast/util/logger.h>
#include <config.h>


#ifdef USE_PERFTOOLS_CPU_PROFILER
#include <google/profiler.h>
#endif
#ifdef USE_PERFTOOLS_HEAP_PROFILER
#include <google/heap-profiler.h>
#endif

namespace vast {

/// Declaration of global (extern) variables.
namespace util {
logger* LOGGER;
}

program::program()
  : terminating_(false)
  , return_(EXIT_FAILURE)
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
  catch (boost::program_options::invalid_command_line_syntax const& e)
  {
    std::cerr << "invalid command line: " << e.what() << std::endl;
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

  auto log_dir = config_.get<fs::path>("directory") / "log";
  assert(fs::exists(log_dir));

  try
  {
#ifdef USE_PERFTOOLS_HEAP_PROFILER
    if (config_.check("perftools-heap"))
    {
      LOG(info, core) << "starting Gperftools CPU profiler";
      HeapProfilerStart((log_dir / "heap.profile").string().data());
    }
#endif
#ifdef USE_PERFTOOLS_CPU_PROFILER
    if (config_.check("perftools-cpu"))
    {
      LOG(info, core) << "starting Gperftools heap profiler";
      ProfilerStart((log_dir / "cpu.profile").string().data());
    }
#endif

    if (config_.check("profile"))
    {
      auto const& filename = log_dir / "profiler.log";
      auto ms = config_.get<unsigned>("profile");
      profiler_ = spawn<util::profiler>(filename.string(),
                                        std::chrono::seconds(ms));
      send(profiler_, atom("run"));
    }

    comm::broccoli::init(config_.check("broccoli-messages"),
                         config_.check("broccoli-calltrace"));

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

    if (config_.check("archive-actor"))
      archive_ = spawn<store::archive>(
          (config_.get<fs::path>("directory") / "archive").string(),
          config_.get<size_t>("archive.max-events-per-chunk"),
          config_.get<size_t>("archive.max-segment-size") * 1000,
          config_.get<size_t>("archive.max-segments"));

    if (config_.check("index-actor"))
        index_ = spawn<store::index>(
            archive_,
            (config_.get<fs::path>("directory") / "index").string());

    if (config_.check("ingestor-actor"))
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
          send(ingestor_, atom("subscribe"), event);
      }

      if (config_.check("ingestor.file-names"))
      {
        auto type = config_.get<std::string>("ingestor.file-type");
        auto files = config_.get<std::vector<std::string>>("ingestor.file-names");
        for (auto& file : files)
        {
          if (fs::exists(file))
            send(ingestor_, atom("ingest"), type, file);
          else
            LOG(error, core) << "no such file: " << file;
        }
      }
    }

    if (config_.check("search-actor"))
    {
      search_ = spawn<query::search>(archive_, index_);

      //config_.get<std::string>("search.host")
      LOG(verbose, core) << "publishing search at *:"
          << config_.get<unsigned>("search.port");
      publish(search_, config_.get<unsigned>("search.port"));
    }
    else
    {
      LOG(verbose, core) << "connecting to search at "
          << config_.get<std::string>("search.host") << ":"
          << config_.get<unsigned>("search.port");
      search_ = remote_actor(
          config_.get<std::string>("search.host"),
          config_.get<unsigned>("search.port"));
    }

    if (config_.check("query"))
    {
      auto paginate = config_.get<unsigned>("client.paginate");
      auto& expression = config_.get<std::string>("query");
      query_client_ = spawn<query::client>(search_, paginate);
      send(query_client_, atom("query"), atom("create"), expression);
    }

    await_all_others_done();
    return_ = EXIT_SUCCESS;
  }
  catch (network_error const& e)
  {
      LOG(error, core) << "network error: " << e.what();
  }
  catch (...)
  {
    LOG(fatal, core)
      << "exception details:\n"
      << boost::current_exception_diagnostic_information();
  }
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

  auto shutdown = make_any_tuple(atom("shutdown"));

  if (config_.check("query"))
    query_client_ << shutdown;

  if (config_.check("search-actor"))
    search_ << shutdown;

  if (config_.check("ingestor-actor"))
    ingestor_ << shutdown;

  if (config_.check("index-actor"))
    index_ << shutdown;

  if (config_.check("archive-actor"))
    archive_ << shutdown;

  schema_manager_ << shutdown;

  if (config_.check("profile"))
    profiler_ << shutdown;

#ifdef USE_PERFTOOLS_CPU_PROFILER

  if (config_.check("perftools-cpu"))
  {
    ProfilerState state;
    ProfilerGetCurrentState(&state);
    LOG(info, core)
      << "Gperftools CPU profiler gathered "
      <<  state.samples_gathered << " samples"
      << " in file " << state.profile_name;

    LOG(info, core) << "stopping Gperftools CPU profiler";
    ProfilerStop();
  }
#endif
#ifdef USE_PERFTOOLS_HEAP_PROFILER
  if (config_.check("perftools-heap") && IsHeapProfilerRunning())
  {
    LOG(info, core) << "stopping Gperftools heap profiler";
    HeapProfilerDump("cleanup");
    HeapProfilerStop();
  }
#endif

  return_ = EXIT_SUCCESS;
}

int program::end()
{
  switch (return_)
  {
    case EXIT_SUCCESS:
      LOG(info, core) << "vast terminated cleanly";
      break;

    case EXIT_FAILURE:
      LOG(info, core) << "vast terminated with errors";
      break;

    default:
      assert(! "invalid return code");
  }

  return return_;
}

void program::do_init()
{
  auto vast_dir = config_.get<fs::path>("directory");
  if (! fs::exists(vast_dir))
    fs::mkdir(vast_dir);

  auto log_dir = vast_dir / "log";
  if (! fs::exists(log_dir))
      fs::mkdir(log_dir);

  util::LOGGER = new util::logger(
      static_cast<util::logger::level>(config_.get<int>("console-verbosity")),
      static_cast<util::logger::level>(config_.get<int>("logfile-verbosity")),
      log_dir / "vast.log");

  LOG(verbose, core) << " _   _____   __________";
  LOG(verbose, core) << "| | / / _ | / __/_  __/";
  LOG(verbose, core) << "| |/ / __ |_\\ \\  / / ";
  LOG(verbose, core) << "|___/_/ |_/___/ /_/  " << VAST_VERSION;
  LOG(verbose, core) << "";
}

} // namespace vast

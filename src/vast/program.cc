#include "vast/program.h"

#include <cstdlib>
#include <iostream>
#include "vast/archive.h"
#include "vast/exception.h"
#include "vast/id_tracker.h"
#include "vast/index.h"
#include "vast/ingestor.h"
#include "vast/logger.h"
#include "vast/query_client.h"
#include "vast/search.h"
#include "vast/comm/broccoli.h"
#include "vast/detail/cppa_type_info.h"
#include "vast/fs/path.h"
#include "vast/fs/operations.h"
#include "vast/meta/schema_manager.h"
#include "vast/util/profiler.h"
#include "config.h"


#ifdef USE_PERFTOOLS_CPU_PROFILER
#include <google/profiler.h>
#endif
#ifdef USE_PERFTOOLS_HEAP_PROFILER
#include <google/heap-profiler.h>
#endif

namespace vast {

/// Declaration of global (extern) variables.
logger* LOGGER;

program::program(configuration const& config)
  : config_(config)
{
  detail::cppa_announce_types();
}

bool program::start()
{
  auto vast_dir = config_.get<fs::path>("directory");
  if (! fs::exists(vast_dir))
    fs::mkdir(vast_dir);

  auto log_dir = vast_dir / "log";
  if (! fs::exists(log_dir))
      fs::mkdir(log_dir);

  LOGGER = new logger(
      static_cast<logger::level>(config_.get<int>("console-verbosity")),
      static_cast<logger::level>(config_.get<int>("logfile-verbosity")),
      log_dir / "vast.log");

  LOG(verbose, core) << " _   _____   __________";
  LOG(verbose, core) << "| | / / _ | / __/_  __/";
  LOG(verbose, core) << "| |/ / __ |_\\ \\  / / ";
  LOG(verbose, core) << "|___/_/ |_/___/ /_/  " << VAST_VERSION;
  LOG(verbose, core) << "";

  try
  {
    using namespace cppa;
    if (config_.check("profile"))
    {
      auto ms = config_.get<unsigned>("profile");
      profiler_ = spawn<util::profiler>(log_dir.string(), std::chrono::seconds(ms));
      send(profiler_,
           atom("run"),
           // FIXME: use 'bool' instead 'int' after the libcppa bug has been fixed.
           static_cast<int>(config_.check("profile-cpu")),
           static_cast<int>(config_.check("profile-heap")));
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
            },
            after(std::chrono::seconds(1)) >> [=]
            {
              LOG(error, meta) 
                << "schema manager did not answer after one second";
            });

        return false;
      }
    }

    if (config_.check("tracker-actor") || config_.check("all-server"))
    {
      tracker_ = spawn<id_tracker>(
          (config_.get<fs::path>("directory") / "id").string());

      LOG(verbose, core) << "publishing tracker at *:"
          << config_.get<unsigned>("tracker.port");

      publish(tracker_, config_.get<unsigned>("tracker.port"));
    }
    else
    {
      LOG(verbose, core) << "connecting to tracker at "
          << config_.get<std::string>("tracker.host") << ":"
          << config_.get<unsigned>("tracker.port");

      tracker_ = remote_actor(
          config_.get<std::string>("tracker.host"),
          config_.get<unsigned>("tracker.port"));
    }

    if (config_.check("archive-actor") || config_.check("all-server"))
    {
      archive_ = spawn<archive>(
          (config_.get<fs::path>("directory") / "archive").string(),
          config_.get<size_t>("archive.max-segments"));

      LOG(verbose, core) << "publishing archive at *:"
          << config_.get<unsigned>("archive.port");

      publish(archive_, config_.get<unsigned>("archive.port"));
    }
    else
    {
      LOG(verbose, core) << "connecting to archive at "
          << config_.get<std::string>("archive.host") << ":"
          << config_.get<unsigned>("archive.port");

      archive_ = remote_actor(
          config_.get<std::string>("archive.host"),
          config_.get<unsigned>("archive.port"));
    }

    if (config_.check("index-actor") || config_.check("all-server"))
    {
      index_ = spawn<index>(
          archive_,
          (config_.get<fs::path>("directory") / "index").string());

      LOG(verbose, core) << "publishing index at *:"
          << config_.get<unsigned>("index.port");

      publish(index_, config_.get<unsigned>("index.port"));
    }
    else
    {
      LOG(verbose, core) << "connecting to index at "
          << config_.get<std::string>("index.host") << ":"
          << config_.get<unsigned>("index.port");

      index_ = remote_actor(
          config_.get<std::string>("index.host"),
          config_.get<unsigned>("index.port"));
    }


    if (config_.check("ingestor-actor"))
    {
      comm::broccoli::init(config_.check("broccoli-messages"),
                           config_.check("broccoli-calltrace"));

      ingestor_ = spawn<ingestor>(tracker_, archive_, index_);

      send(ingestor_, atom("initialize"),
          config_.get<size_t>("ingest.max-events-per-chunk"),
          config_.get<size_t>("ingest.max-segment-size") * 1000000);

      if (config_.check("ingest.events"))
      {
        auto host = config_.get<std::string>("ingest.host");
        auto port = config_.get<unsigned>("ingest.port");
        auto events = config_.get<std::vector<std::string>>("ingest.events");
        send(ingestor_, atom("ingest"), atom("broccoli"), host, port, events);
      }

      if (config_.check("ingest.file-names"))
      {
        auto type = config_.get<std::string>("ingest.file-type");
        auto files = config_.get<std::vector<std::string>>("ingest.file-names");
        for (auto& file : files)
        {
          if (fs::exists(file))
            send(ingestor_, atom("ingest"), type, file);
          else
            LOG(error, core) << "no such file: " << file;
        }
      }
    }

    if (config_.check("search-actor") || config_.check("all-server"))
    {
      search_ = spawn<search>(archive_, index_);

      LOG(verbose, core) << "publishing search at *:"
          << config_.get<unsigned>("search.port");

      publish(search_, config_.get<unsigned>("search.port"));
    }
    else if (config_.check("expression"))
    {
      LOG(verbose, core) << "connecting to search at "
          << config_.get<std::string>("search.host") << ":"
          << config_.get<unsigned>("search.port");

      search_ = remote_actor(
          config_.get<std::string>("search.host"),
          config_.get<unsigned>("search.port"));

      auto paginate = config_.get<unsigned>("query.paginate");
      auto& expression = config_.get<std::string>("expression");
      query_client_ = spawn<query_client>(search_, paginate);
      send(query_client_, atom("query"), atom("create"), expression);
    }

    return true;
  }
  catch (cppa::network_error const& e)
  {
      LOG(error, core) << "network error: " << e.what();
  }

  return false;
}

void program::stop()
{
  using namespace cppa;
  auto shutdown = make_any_tuple(atom("shutdown"));

  if (config_.check("query"))
    query_client_ << shutdown;

  if (config_.check("search-actor") || config_.check("all-server"))
    search_ << shutdown;

  if (config_.check("ingestor-actor"))
    ingestor_ << shutdown;

  if (config_.check("index-actor") || config_.check("all-server"))
    index_ << shutdown;

  if (config_.check("archive-actor") || config_.check("all-server"))
    archive_ << shutdown;

  if (config_.check("tracker-actor") || config_.check("all-server"))
    tracker_ << shutdown;

  schema_manager_ << shutdown;

  if (config_.check("profile"))
    profiler_ << shutdown;

  await_all_others_done();
}

} // namespace vast

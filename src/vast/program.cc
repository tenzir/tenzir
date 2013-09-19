#include "vast/program.h"

#include <cstdlib>
#include <iostream>
#include "vast/archive.h"
#include "vast/exception.h"
#include "vast/config.h"
#include "vast/file_system.h"
#include "vast/id_tracker.h"
#include "vast/index.h"
#include "vast/ingestor.h"
#include "vast/logger.h"
#include "vast/query_client.h"
#include "vast/receiver.h"
#include "vast/schema.h"
#include "vast/schema_manager.h"
#include "vast/search.h"
#include "vast/shutdown.h"
#include "vast/system_monitor.h"
#include "vast/detail/cppa_type_info.h"
#include "vast/detail/type_manager.h"
#include "vast/util/profiler.h"

#ifdef VAST_HAVE_BROCCOLI
#include "vast/util/broccoli.h"
#endif

namespace vast {

using namespace cppa;

program::program(configuration const& config)
  : config_(config)
{ }

bool program::run()
{
  if (! start())
    return false;

  bool done = false;
  do_receive(
      on(atom("system"), atom("key"), arg_match) >> [&](char key)
      {
        switch (key)
        {
          default:
          {
            std::string desc;
            if (key == '\n')
              desc = "<enter>";
            else if (key == ' ')
              desc = "<space>";
            else
              desc = std::string{"'"} + key + "'";
            VAST_LOG_ACT_VERBOSE(
                "program", "got invalid key " << desc << " (press Q to quit)");
          }
          break;
          case 'Q':
            done = true;
            break;
        }
      },
      on(atom("system"), atom("signal"), arg_match) >> [&](int signal)
      {
        VAST_LOG_ACT_VERBOSE("program", "received signal " << signal);
        if (signal == SIGINT || signal == SIGTERM)
          done = true;
      },
      others() >> [=]
      {
        VAST_LOG_ACT_ERROR("program", "received unexpected message from @" <<
                           self->last_sender()->id() << ": " <<
                           to_string(self->last_dequeued()));
      })
  .until(gref(done));

  stop();
  await_all_others_done();
  cppa::shutdown();
  vast::shutdown();

  return true;
}

bool program::start()
{
  detail::cppa_announce_types();

  path vast_dir = string(config_.get("directory"));
  if (! exists(vast_dir))
    if (! mkdir(vast_dir))
      return false;

  logger::instance()->init(
      static_cast<logger::level>(config_.as<uint32_t>("log.console-verbosity")),
      static_cast<logger::level>(config_.as<uint32_t>("log.file-verbosity")),
      config_.check("log.function-names"),
      vast_dir / "log");

  VAST_LOG_VERBOSE(" _   _____   __________");
  VAST_LOG_VERBOSE("| | / / _ | / __/_  __/");
  VAST_LOG_VERBOSE("| |/ / __ |_\\ \\  / / ");
  VAST_LOG_VERBOSE("|___/_/ |_/___/ /_/  " << VAST_VERSION);
  VAST_LOG_VERBOSE("");

  // FIXME: We call the type manager here because it is not thread-safe and
  // later invocations would lead to reaces. The call creates the singleton
  // and announces the known types during initialize(). Thereafter, the type
  // manager can only tolerate const access. We may need to figure out a
  // cleaner way to handle this shared state.
  size_t n_types = 0;
  detail::type_manager::instance()->each(
      [&](global_type_info const&) { ++n_types; });
  VAST_LOG_DEBUG("type manager announced " << n_types << " types");

  try
  {
    VAST_LOG_ACT_VERBOSE("program", "spawned");
    system_monitor_ = spawn<system_monitor, detached>(self);
    self->monitor(system_monitor_);
    send(system_monitor_, atom("act"));

    if (config_.check("profile"))
    {
      auto ms = config_.as<unsigned>("profile");
      profiler_ = spawn<util::profiler>(to_string(vast_dir / "log"),
                                        std::chrono::seconds(ms));
      if (config_.check("profile-cpu"))
        send(profiler_, atom("start"), atom("perftools"), atom("cpu"));
      if (config_.check("profile-heap"))
        send(profiler_, atom("start"), atom("perftools"), atom("heap"));
      send(profiler_, atom("start"), atom("rusage"));
    }

    schema_manager_ = spawn<schema_manager, monitored>();
    if (config_.check("schema.file"))
    {
      send(schema_manager_, atom("load"), config_.get("schema.file"));
      if (config_.check("schema.print"))
      {
        send(schema_manager_, atom("schema"));
        receive(
            on_arg_match >> [](schema const& s) { std::cout << to_string(s); },
            after(std::chrono::seconds(1)) >> [=]
            {
              VAST_LOG_ACT_ERROR("program",
                  "schema manager " << schema_manager_->id() <<
                  " did not answer after one second");
            });
        return false;
      }
    }

    auto tracker_host = config_.get("tracker.host");
    auto tracker_port = config_.as<unsigned>("tracker.port");
    if (config_.check("tracker-actor") || config_.check("all-server"))
    {
      tracker_ = spawn<id_tracker>(to_string(vast_dir / "id"));
      VAST_LOG_ACT_VERBOSE("program", "publishes tracker at " <<
                           tracker_host << ':' << tracker_port);
      publish(tracker_, tracker_port, tracker_host.c_str());
    }
    else
    {
      VAST_LOG_ACT_VERBOSE("program", "connects to tracker at " <<
                           tracker_host << ':' << tracker_port);

      tracker_ = remote_actor(tracker_host, tracker_port);
      VAST_LOG_ACT_VERBOSE("program",
                           "connected to tracker actor @" << tracker_->id());
    }

    auto archive_host = config_.get("archive.host");
    auto archive_port = config_.as<unsigned>("archive.port");
    if (config_.check("archive-actor") || config_.check("all-server"))
    {
      archive_ = spawn<archive>(
          to_string(vast_dir / "archive"),
          config_.as<size_t>("archive.max-segments"));
      send(archive_, atom("init"));
      VAST_LOG_ACT_VERBOSE("program", "publishes archive at " <<
                           archive_host << ':' << archive_port);
      publish(archive_, archive_port, archive_host.c_str());
    }
    else
    {
      VAST_LOG_ACT_VERBOSE("program", "connects to tracker at " <<
                           tracker_host << ':' << tracker_port);
      archive_ = remote_actor(archive_host, archive_port);
      VAST_LOG_ACT_VERBOSE("program",
                           "connected to archive actor @" << archive_->id());
    }

    auto index_host = config_.get("index.host");
    auto index_port = config_.as<unsigned>("index.port");
    if (config_.check("index-actor") || config_.check("all-server"))
    {
      index_ = spawn<index>(vast_dir / "index");
      VAST_LOG_ACT_VERBOSE("program", "publishes index " <<
                           index_host << ':' << index_port);
      publish(index_, index_port, index_host.c_str());
    }
    else
    {
      VAST_LOG_ACT_VERBOSE("program", "connects to index at " <<
                           index_host << ":" << index_port);
      index_ = remote_actor(index_host, index_port);
      VAST_LOG_ACT_VERBOSE("program",
                           "connected to index actor @" << index_->id());
    }

    auto receiver_host = config_.get("receiver.host");
    auto receiver_port = config_.as<unsigned>("receiver.port");
    if (config_.check("archive-actor")
        || config_.check("index-actor")
        || config_.check("all-server"))
    {
      receiver_ = spawn<receiver>(tracker_, archive_, index_);
      VAST_LOG_ACT_VERBOSE("program", "publishes receiver at " <<
                           receiver_host << ':' << receiver_port);
      publish(receiver_, receiver_port, receiver_host.c_str());
    }
    else
    {
      VAST_LOG_ACT_VERBOSE("program", "connects to receiver at " <<
                           receiver_host << ":" << receiver_port);
      receiver_ = remote_actor(receiver_host, receiver_port);
      VAST_LOG_ACT_VERBOSE("program",
                           "connected to receiver actor @" << receiver_->id());
    }

    if (config_.check("ingestor-actor"))
    {
      ingestor_ = spawn<ingestor, monitored>(receiver_,
          config_.as<size_t>("ingest.max-events-per-chunk"),
          config_.as<size_t>("ingest.max-segment-size") * 1000000,
          config_.as<size_t>("ingest.batch-size"));

#ifdef VAST_HAVE_BROCCOLI
      util::broccoli::init(config_.check("broccoli-messages"),
                           config_.check("broccoli-calltrace"));
      if (config_.check("ingest.broccoli-events"))
      {
        auto host = config_.get("ingest.broccoli-host");
        auto port = config_.as<unsigned>("ingest.broccoli-port");
        auto events =
          config_.as<std::vector<std::string>>("ingest.broccoli-events");
        send(ingestor_,
             atom("ingest"), atom("broccoli"),
             host, port,
             std::move(events));
      }
#endif

      if (config_.check("ingest.file-names"))
      {
        auto type = config_.get("ingest.file-type");
        auto files = config_.as<std::vector<std::string>>("ingest.file-names");
        for (auto& file : files)
        {
          if (exists(string(file)))
            send(ingestor_, atom("ingest"), type, file);
          else
            VAST_LOG_ACT_ERROR("program",
                               "could not find file to ingest: " << file);
        }
      }

      send(ingestor_, atom("run"));
    }

    auto search_host = config_.get("search.host");
    auto search_port = config_.as<unsigned>("search.port");
    if (config_.check("search-actor") || config_.check("all-server"))
    {
      search_ = spawn<search>(archive_, index_, schema_manager_);
      VAST_LOG_ACT_VERBOSE("program", "publishes search at " <<
                       search_host << ':' << search_port);
      publish(search_, search_port, search_host.c_str());
    }
    else if (config_.check("client.expression"))
    {
      VAST_LOG_ACT_VERBOSE("program", "connects to search at " <<
                           search_host << ":" << search_port);
      search_ = remote_actor(search_host, search_port);
      VAST_LOG_ACT_VERBOSE("program",
                           "connected to search actor @" << search_->id());

      auto paginate = config_.as<unsigned>("client.paginate");
      auto& expression = config_.get("client.expression");
      query_client_ = spawn<query_client, monitored>(search_, expression,
                                                     paginate);
      send(query_client_, atom("start"));
    }

    return true;
  }
  catch (network_error const& e)
  {
    VAST_LOG_ACT_ERROR("program", "encountered network error: " << e.what());
  }

  return false;
}

void program::stop()
{
  auto kill = make_any_tuple(atom("kill"));

  if (query_client_)
    query_client_ << kill;

  if (config_.check("search-actor") || config_.check("all-server"))
    search_ << kill;

  if (config_.check("ingestor-actor"))
    ingestor_ << kill;

  if (config_.check("archive-actor") ||
      config_.check("index-actor") ||
      config_.check("all-server"))
    receiver_ << kill;

  if (config_.check("index-actor") || config_.check("all-server"))
    index_ << kill;

  if (config_.check("archive-actor") || config_.check("all-server"))
    archive_ << kill;

  if (config_.check("tracker-actor") || config_.check("all-server"))
    tracker_ << kill;

  if (schema_manager_)
    schema_manager_ << kill;

  if (profiler_)
    profiler_ << kill;

  if (system_monitor_)
    system_monitor_ << kill;

}

} // namespace vast

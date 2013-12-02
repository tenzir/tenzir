#include "vast/program.h"

#include <cstdlib>
#include <iostream>
#include "vast/archive.h"
#include "vast/exception.h"
#include "vast/config.h"
#include "vast/console.h"
#include "vast/file_system.h"
#include "vast/id_tracker.h"
#include "vast/index.h"
#include "vast/ingestor.h"
#include "vast/logger.h"
#include "vast/receiver.h"
#include "vast/schema.h"
#include "vast/schema_manager.h"
#include "vast/search.h"
#include "vast/signal_monitor.h"
#include "vast/detail/type_manager.h"
#include "vast/util/profiler.h"

#ifdef VAST_HAVE_BROCCOLI
#include "vast/util/broccoli.h"
#endif

using namespace cppa;

namespace vast {

program::program(configuration const& config)
  : config_{config},
    server_{! config_.check("console-actor")}
{
}

void program::act()
{
  chaining(false);
  path vast_dir = string(config_.get("directory"));
  try
  {
    if (config_.check("profile"))
    {
      auto ms = config_.as<unsigned>("profile");
      profiler_ = spawn<util::profiler, linked>(
          to_string(vast_dir / "log"), std::chrono::seconds(ms));
      if (config_.check("profile-cpu"))
        send(profiler_, atom("start"), atom("perftools"), atom("cpu"));
      if (config_.check("profile-heap"))
        send(profiler_, atom("start"), atom("perftools"), atom("heap"));
      send(profiler_, atom("start"), atom("rusage"));
    }

    schema_manager_ = spawn<schema_manager, linked>();
    if (config_.check("schema.file"))
    {
      send(schema_manager_, atom("load"), config_.get("schema.file"));
      if (config_.check("schema.print"))
      {
        sync_send(schema_manager_, atom("schema")).then(
            on_arg_match >> [](schema const& s)
            {
              std::cout << to_string(s);
            });
        quit();
      }
    }

    auto& tracker_host = config_.get("tracker.host");
    auto tracker_port = config_.as<unsigned>("tracker.port");
    if (config_.check("tracker-actor") || config_.check("all-server"))
    {
      tracker_ = spawn<id_tracker_actor, linked>(vast_dir);
      VAST_LOG_ACTOR_VERBOSE("publishes tracker at " <<
                           tracker_host << ':' << tracker_port);
      publish(tracker_, tracker_port, tracker_host.c_str());
    }
    else
    {
      VAST_LOG_ACTOR_VERBOSE("connects to tracker at " <<
                           tracker_host << ':' << tracker_port);
      tracker_ = remote_actor(tracker_host, tracker_port);
    }

    auto& archive_host = config_.get("archive.host");
    auto archive_port = config_.as<unsigned>("archive.port");
    if (config_.check("archive-actor") || config_.check("all-server"))
    {
      archive_ = spawn<archive_actor, linked>(
          to_string(vast_dir / "archive"),
          config_.as<size_t>("archive.max-segments"));
      VAST_LOG_ACTOR_VERBOSE("publishes archive at " <<
                           archive_host << ':' << archive_port);
      publish(archive_, archive_port, archive_host.c_str());
    }
    else
    {
      VAST_LOG_ACTOR_VERBOSE("connects to archive at " <<
                           archive_host << ':' << archive_port);
      archive_ = remote_actor(archive_host, archive_port);
    }

    auto& index_host = config_.get("index.host");
    auto index_port = config_.as<unsigned>("index.port");
    if (config_.check("index-actor") || config_.check("all-server"))
    {
      index_ = spawn<index, linked>(vast_dir / "index");
      VAST_LOG_ACTOR_VERBOSE("publishes index " <<
                           index_host << ':' << index_port);
      publish(index_, index_port, index_host.c_str());
    }
    else
    {
      VAST_LOG_ACTOR_VERBOSE("connects to index at " <<
                           index_host << ":" << index_port);
      index_ = remote_actor(index_host, index_port);
    }

    auto& receiver_host = config_.get("receiver.host");
    auto receiver_port = config_.as<unsigned>("receiver.port");
    if (config_.check("archive-actor")
        || config_.check("index-actor")
        || config_.check("all-server"))
    {
      receiver_ = spawn<receiver, linked>(tracker_, archive_, index_);
      VAST_LOG_ACTOR_VERBOSE("publishes receiver at " <<
                           receiver_host << ':' << receiver_port);
      publish(receiver_, receiver_port, receiver_host.c_str());
    }
    else
    {
      VAST_LOG_ACTOR_VERBOSE("connects to receiver at " <<
                           receiver_host << ":" << receiver_port);
      receiver_ = remote_actor(receiver_host, receiver_port);
    }

    if (config_.check("ingestor-actor"))
    {
      ingestor_ = spawn<ingestor, linked>(receiver_,
          config_.as<size_t>("ingest.max-events-per-chunk"),
          config_.as<size_t>("ingest.max-segment-size") * 1000000,
          config_.as<size_t>("ingest.batch-size"));

#ifdef VAST_HAVE_BROCCOLI
      util::broccoli::init(config_.check("broccoli-messages"),
                           config_.check("broccoli-calltrace"));
      if (config_.check("ingest.broccoli-events"))
      {
        auto& host = config_.get("ingest.broccoli-host");
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
        auto& type = config_.get("ingest.file-type");
        auto files = config_.as<std::vector<std::string>>("ingest.file-names");
        for (auto& file : files)
        {
          if (exists(string(file)))
            send(ingestor_, atom("ingest"), type, file);
          else
            VAST_LOG_ACTOR_ERROR("no such file: " << file);
        }
      }

      send(ingestor_, atom("run"));
    }

    auto& search_host = config_.get("search.host");
    auto search_port = config_.as<unsigned>("search.port");
    if (config_.check("search-actor") || config_.check("all-server"))
    {
      search_ = spawn<search_actor, linked>(archive_, index_, schema_manager_);
      VAST_LOG_ACTOR_VERBOSE("publishes search at " <<
                       search_host << ':' << search_port);
      publish(search_, search_port, search_host.c_str());
    }

    if (! server_)
    {
      VAST_LOG_ACTOR_VERBOSE("connects to search at " <<
                           search_host << ":" << search_port);
      search_ = remote_actor(search_host, search_port);
      console_ = spawn<console, detached+linked>(search_, vast_dir / "console");
      delayed_send(console_, std::chrono::milliseconds(200), atom("prompt"));
    }

    signal_monitor_ = spawn<signal_monitor, detached+linked>(self);
    send(signal_monitor_, atom("act"));
  }
  catch (network_error const& e)
  {
    VAST_LOG_ACTOR_ERROR("encountered network error: " << e.what());
    send_exit(self, exit::error);
  }

  become(
      on(atom("signal"), arg_match) >> [&](int signal)
      {
        VAST_LOG_ACTOR_VERBOSE("received signal " << signal);
        if (signal == SIGINT || signal == SIGTERM)
          quit(exit::stop);
      },
      others() >> [=]
      {
        quit(exit::error);
        VAST_LOG_ACTOR_ERROR("terminated after unexpected message from @" <<
                             last_sender()->id() << ": " <<
                             to_string(self->last_dequeued()));
      });
}

char const* program::description() const
{
  return "program";
}

} // namespace vast

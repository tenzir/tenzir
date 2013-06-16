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
#include "vast/schema.h"
#include "vast/schema_manager.h"
#include "vast/search.h"
#include "vast/shutdown.h"
#include "vast/system_monitor.h"
#include "vast/to_string.h"
#include "vast/detail/cppa_type_info.h"
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
        if (! query_client_)
          return;

        switch (key)
        {
          default:
            VAST_LOG_VERBOSE("invalid key: '" << key << "'");
          case '?':
            VAST_LOG_VERBOSE("available commands: "
                             "<space> for results, (s)tatistics, (Q)uit");
            break;
          case ' ':
            send(query_client_, atom("results"));
            break;
          case 'Q':
            done = true;
            break;
          case 's':
            send(query_client_, atom("statistics"));
            break;
        }
      },
      on(atom("DOWN"), arg_match) >> [&](uint32_t /* reason */)
      {
        done = true;
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
  assert(exists(vast_dir));

  logger::instance()->init(
      static_cast<logger::level>(config_.as<uint32_t>("console-verbosity")),
      static_cast<logger::level>(config_.as<uint32_t>("file-verbosity")),
      vast_dir / "log");

  VAST_LOG_VERBOSE(" _   _____   __________");
  VAST_LOG_VERBOSE("| | / / _ | / __/_  __/");
  VAST_LOG_VERBOSE("| |/ / __ |_\\ \\  / / ");
  VAST_LOG_VERBOSE("|___/_/ |_/___/ /_/  " << VAST_VERSION);
  VAST_LOG_VERBOSE("");

  try
  {
    system_monitor_ = spawn<system_monitor, detached+monitored>(self);

    if (config_.check("profile"))
    {
      auto ms = config_.as<unsigned>("profile");
      profiler_ = spawn<util::profiler>(to_string(vast_dir / "log"),
                                        std::chrono::seconds(ms));
      send(profiler_,
           atom("run"),
           config_.check("profile-cpu"),
           config_.check("profile-heap"));
    }

    schema_manager_ = spawn<schema_manager>();
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
              VAST_LOG_ERROR(
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
      VAST_LOG_VERBOSE("publishing tracker at " << tracker_host <<
                       ':' << tracker_port);
      publish(tracker_, tracker_port, tracker_host.data());
    }
    else
    {
      VAST_LOG_VERBOSE("connecting to tracker at " << tracker_host <<
                       ':' << tracker_port);

      tracker_ = remote_actor(tracker_host, tracker_port);
      VAST_LOG_VERBOSE("connected to tracker actor @" << tracker_->id());
    }

    auto archive_host = config_.get("archive.host");
    auto archive_port = config_.as<unsigned>("archive.port");
    if (config_.check("archive-actor") || config_.check("all-server"))
    {
      archive_ = spawn<archive>(
          to_string(vast_dir / "archive"),
          config_.as<size_t>("archive.max-segments"));

      send(archive_, atom("load"));
      VAST_LOG_VERBOSE("publishing archive at *:" << archive_port);
      publish(archive_, archive_port);
    }
    else
    {
      VAST_LOG_VERBOSE("connecting to archive at " <<
                       archive_host << ":" << archive_port);

      archive_ = remote_actor(archive_host, archive_port);
      VAST_LOG_VERBOSE("connected to archive actor @" << archive_->id());
    }

    if (config_.check("index-actor") || config_.check("all-server"))
    {
      index_ = spawn<index>(to_string(vast_dir / "index"));
      send(index_, atom("load"));

      VAST_LOG_VERBOSE("publishing index at *:" <<
                       config_.as<unsigned>("index.port"));

      publish(index_, config_.as<unsigned>("index.port"));
    }
    else
    {
      VAST_LOG_VERBOSE("connecting to index at " <<
                       config_.get("index.host") << ":" <<
                       config_.as<unsigned>("index.port"));

      index_ = remote_actor(
          config_.get("index.host"),
          config_.as<unsigned>("index.port"));

      VAST_LOG_VERBOSE("connected to index actor @" << index_->id());
    }


    if (config_.check("ingestor-actor"))
    {
      ingestor_ = spawn<ingestor>(
          tracker_, archive_, index_,
          config_.as<size_t>("ingest.max-events-per-chunk"),
          config_.as<size_t>("ingest.max-segment-size") * 1000000,
          config_.as<size_t>("ingest.batch-size"));
      self->monitor(ingestor_);

#ifdef VAST_HAVE_BROCCOLI
      util::broccoli::init(config_.check("broccoli-messages"),
                           config_.check("broccoli-calltrace"));

      if (config_.check("ingest.broccoli-events"))
      {
        auto host = config_.get("ingest.broccoli-host");
        auto port = config_.as<unsigned>("ingest.broccoli-port");
        auto events = config_.as<std::vector<std::string>>("ingest.broccoli-events");
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
            VAST_LOG_ERROR("no such file: " << file);
        }
      }

      send(ingestor_, atom("extract"));
    }

    if (config_.check("search-actor") || config_.check("all-server"))
    {
      search_ = spawn<search>(archive_, index_, schema_manager_);

      VAST_LOG_VERBOSE("publishing search at *:" <<
                       config_.as<unsigned>("search.port"));

      publish(search_, config_.as<unsigned>("search.port"));
    }
    else if (config_.check("client.expression"))
    {
      VAST_LOG_VERBOSE("connecting to search at " <<
                       config_.get("search.host") << ":" <<
                       config_.as<unsigned>("search.port"));

      search_ = remote_actor(
          config_.get("search.host"),
          config_.as<unsigned>("search.port"));

      VAST_LOG_VERBOSE("connected to search actor @" << search_->id());

      auto paginate = config_.as<unsigned>("client.paginate");
      auto& expression = config_.get("client.expression");
      query_client_ = spawn<query_client>(search_, expression, paginate);
      self->monitor(query_client_);
      send(query_client_, atom("start"));
    }

    return true;
  }
  catch (network_error const& e)
  {
    VAST_LOG_ERROR("network error: " << e.what());
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

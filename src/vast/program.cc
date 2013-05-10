#include "vast/program.h"

#include <cstdlib>
#include <iostream>
#include <ze/event.h>
#include <ze/file_system.h>
#include "vast/archive.h"
#include "vast/exception.h"
#include "vast/config.h"
#include "vast/id_tracker.h"
#include "vast/index.h"
#include "vast/ingestor.h"
#include "vast/logger.h"
#include "vast/query_client.h"
#include "vast/schema.h"
#include "vast/schema_manager.h"
#include "vast/search.h"
#include "vast/system_monitor.h"
#include "vast/to_string.h"
#include "vast/util/profiler.h"

#ifdef VAST_HAVE_BROCCOLI
#include "vast/util/broccoli.h"
#endif

namespace vast {

using namespace cppa;

program::program(configuration const& config)
  : config_(config)
{
}

bool program::run()
{
  if (! start())
    return false;

  bool done = false;
  do_receive(
      on(atom("system"), atom("key"), arg_match) >> [=, &done](char key)
      {
        if (! query_client_)
          return;

        switch (key)
        {
          default:
            LOG(info, core) << "invalid key: '" << key << "'";
          case '?':
            LOG(info, core)
              << "available commands: "
                 "<space> for results, (s)tatistics, (Q)uit";
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
      on(atom("DOWN"), arg_match) >> [&done](uint32_t reason)
      {
        done = true;
      })
  .until(gref(done));

  stop();
  await_all_others_done();
  shutdown();

  return true;
}

bool program::start()
{
  LOG(verbose, core) << " _   _____   __________";
  LOG(verbose, core) << "| | / / _ | / __/_  __/";
  LOG(verbose, core) << "| |/ / __ |_\\ \\  / / ";
  LOG(verbose, core) << "|___/_/ |_/___/ /_/  " << VAST_VERSION;
  LOG(verbose, core) << "";

  auto& vast_dir = config_.get("directory");
  assert(ze::exists(vast_dir));
  assert(ze::exists(ze::path(vast_dir) / "log"));

  try
  {
    system_monitor_ = spawn<system_monitor,detached>(self);
    self->monitor(system_monitor_);

    if (config_.check("profile"))
    {
      auto ms = config_.as<unsigned>("profile");
      profiler_ = spawn<util::profiler>((ze::path(vast_dir) / "log").string(),
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
            on_arg_match >> [](schema const& s)
            {
              std::cout << to_string(s);
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
      tracker_ = spawn<id_tracker>((ze::path(vast_dir) / "id").string());
      send(tracker_, atom("initialize"));
      LOG(verbose, core) << "publishing tracker at *:"
          << config_.as<unsigned>("tracker.port");

      publish(tracker_, config_.as<unsigned>("tracker.port"));
    }
    else
    {
      LOG(verbose, core) << "connecting to tracker at "
          << config_.get("tracker.host") << ":"
          << config_.as<unsigned>("tracker.port");

      tracker_ = remote_actor(
          config_.get("tracker.host"),
          config_.as<unsigned>("tracker.port"));

      LOG(verbose, core) << "connected to tracker actor @" << tracker_->id();
    }

    if (config_.check("archive-actor") || config_.check("all-server"))
    {
      archive_ = spawn<archive>((ze::path(vast_dir) / "archive").string(),
          config_.as<size_t>("archive.max-segments"));
      send(archive_, atom("load"));

      LOG(verbose, core) << "publishing archive at *:"
          << config_.as<unsigned>("archive.port");

      publish(archive_, config_.as<unsigned>("archive.port"));
    }
    else
    {
      LOG(verbose, core) << "connecting to archive at "
          << config_.get("archive.host") << ":"
          << config_.as<unsigned>("archive.port");

      archive_ = remote_actor(
          config_.get("archive.host"),
          config_.as<unsigned>("archive.port"));

      LOG(verbose, core) << "connected to archive actor @" << archive_->id();
    }

    if (config_.check("index-actor") || config_.check("all-server"))
    {
      index_ = spawn<index>((ze::path(vast_dir) / "index").string());
      send(index_, atom("load"));

      LOG(verbose, core) << "publishing index at *:"
          << config_.as<unsigned>("index.port");

      publish(index_, config_.as<unsigned>("index.port"));
    }
    else
    {
      LOG(verbose, core) << "connecting to index at "
          << config_.get("index.host") << ":"
          << config_.as<unsigned>("index.port");

      index_ = remote_actor(
          config_.get("index.host"),
          config_.as<unsigned>("index.port"));

      LOG(verbose, core) << "connected to index actor @" << index_->id();
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
          if (ze::exists(file))
            send(ingestor_, atom("ingest"), type, file);
          else
            LOG(error, core) << "no such file: " << file;
        }
      }

      send(ingestor_, atom("extract"));
    }

    if (config_.check("search-actor") || config_.check("all-server"))
    {
      search_ = spawn<search>(archive_, index_, schema_manager_);

      LOG(verbose, core) << "publishing search at *:"
          << config_.as<unsigned>("search.port");

      publish(search_, config_.as<unsigned>("search.port"));
    }
    else if (config_.check("client.expression"))
    {
      LOG(verbose, core) << "connecting to search at "
          << config_.get("search.host") << ":"
          << config_.as<unsigned>("search.port");

      search_ = remote_actor(
          config_.get("search.host"),
          config_.as<unsigned>("search.port"));

      LOG(verbose, core) << "connected to search actor @" << search_->id();

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
      LOG(error, core) << "network error: " << e.what();
  }

  return false;
}

void program::stop()
{
  auto shutdown = make_any_tuple(atom("kill"));

  if (query_client_)
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

  if (schema_manager_)
    schema_manager_ << shutdown;

  if (profiler_)
    profiler_ << shutdown;

  if (system_monitor_)
    system_monitor_ << shutdown;

}

} // namespace vast

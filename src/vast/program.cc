#include "vast/program.h"

#include <cstdlib>
#include <csignal>
#include <iostream>
#include "vast/archive.h"
#include "vast/file_system.h"
#include "vast/id_tracker.h"
#include "vast/index.h"
#include "vast/ingestor.h"
#include "vast/logger.h"
#include "vast/profiler.h"
#include "vast/receiver.h"
#include "vast/search.h"
#include "vast/signal_monitor.h"
#include "vast/type_info.h"
#include "vast/detail/cppa_type_info.h"
#include "vast/detail/type_manager.h"

#ifdef VAST_HAVE_BROCCOLI
#include "vast/util/broccoli.h"
#endif

#ifdef VAST_HAVE_EDITLINE
#include "vast/console.h"
#endif

using namespace cppa;

namespace vast {

program::program(configuration config)
  : config_{std::move(config)}
{
}

behavior program::act()
{
  behavior default_behavior = (
      on(atom("signal"), arg_match) >> [&](int signal)
      {
        VAST_LOG_ACTOR_VERBOSE("received signal " << signal);
        if (signal == SIGINT || signal == SIGTERM)
          quit(exit::stop);
      },
      others() >> [=]
      {
        quit(exit::error);
        VAST_LOG_ACTOR_ERROR("terminated after unexpected message from " <<
                             last_sender() << ": " <<
                             to_string(last_dequeued()));
      });

  auto vast_dir = path{*config_.get("directory")}.complete();

  auto initialized = logger::instance()->init(
      *logger::parse_level(*config_.get("log.console-verbosity")),
      *logger::parse_level(*config_.get("log.file-verbosity")),
      ! config_.check("log.no-colors"),
      config_.check("log.function-names"),
      vast_dir / "log");

  if (! initialized)
  {
    std::cerr << "failed to initialize logger" << std::endl;
    send_exit(this, exit::error);
    return default_behavior;
  }

  VAST_LOG_VERBOSE(" _   _____   __________");
  VAST_LOG_VERBOSE("| | / / _ | / __/_  __/");
  VAST_LOG_VERBOSE("| |/ / __ |_\\ \\  / / ");
  VAST_LOG_VERBOSE("|___/_/ |_/___/ /_/  " << VAST_VERSION);
  VAST_LOG_VERBOSE("");

  announce_builtin_types();

  detail::type_manager::instance()->each(
      [&](global_type_info const& gti)
      {
        VAST_LOG_DEBUG("registered type " << gti.id() << ": " << gti.name());
      });

  max_msg_size(512 * 1024 * 1024);
  VAST_LOG_ACTOR_DEBUG("set cppa maximum message size to " <<
                       cppa::max_msg_size() / 1024 << " KB");

  try
  {
    if (config_.check("all-server"))
    {
      *config_["receiver-actor"] = true;
      *config_["tracker-actor"] = true;
      *config_["archive-actor"] = true;
      *config_["index-actor"] = true;
      *config_["search-actor"] = true;
    }

    auto monitor = spawn<signal_monitor, detached+linked>(this);
    send(monitor, atom("act"));

    if (config_.check("profile"))
    {
      auto ms = *config_.as<unsigned>("profile");
      auto prof = spawn<profiler, detached+linked>(
          vast_dir / "log", std::chrono::seconds(ms));

      if (config_.check("profile-cpu"))
        send(prof, atom("start"), atom("perftools"), atom("cpu"));

      if (config_.check("profile-heap"))
        send(prof, atom("start"), atom("perftools"), atom("heap"));

      send(prof, atom("start"), atom("rusage"));
    }

    actor tracker;
    auto tracker_host = *config_.get("tracker.host");
    auto tracker_port = *config_.as<unsigned>("tracker.port");
    if (config_.check("tracker-actor"))
    {
      tracker = spawn<id_tracker_actor>(vast_dir);
      VAST_LOG_ACTOR_INFO(
          "publishes tracker at " << tracker_host << ':' << tracker_port);

      publish(tracker, tracker_port, tracker_host.c_str());
    }
    else if (config_.check("receiver-actor"))
    {
      VAST_LOG_ACTOR_VERBOSE(
          "connects to tracker at " << tracker_host << ':' << tracker_port);
      tracker = remote_actor(tracker_host, tracker_port);
    }

    actor archive;
    auto archive_host = *config_.get("archive.host");
    auto archive_port = *config_.as<unsigned>("archive.port");
    if (config_.check("archive-actor"))
    {
      archive = spawn<archive_actor>(
          vast_dir,
          *config_.as<size_t>("archive.max-segments"));

      VAST_LOG_ACTOR_INFO(
          "publishes archive at " << archive_host << ':' << archive_port);

      publish(archive, archive_port, archive_host.c_str());
    }
    else if (config_.check("receiver-actor")
             || config_.check("search-actor")
             || config_.check("index.rebuild"))
    {
      VAST_LOG_ACTOR_VERBOSE(
          "connects to archive at " << archive_host << ':' << archive_port);

      archive = remote_actor(archive_host, archive_port);
    }

    actor index;
    auto index_host = *config_.get("index.host");
    auto index_port = *config_.as<unsigned>("index.port");
    if (config_.check("index-actor"))
    {
      index = spawn<index_actor>(
          vast_dir, *config_.as<size_t>("index.batch-size"));

      VAST_LOG_ACTOR_INFO(
          "publishes index at " << index_host << ':' << index_port);

      publish(index, index_port, index_host.c_str());
    }
    else if (config_.check("receiver-actor")
             || config_.check("search-actor")
             || config_.check("index.rebuild"))
    {
      VAST_LOG_ACTOR_VERBOSE("connects to index at " <<
                           index_host << ":" << index_port);

      index = remote_actor(index_host, index_port);
    }

    if (auto partition = config_.get("index.partition"))
    {
      send(index, atom("partition"), *partition);
    }
    else if (config_.check("index.rebuild"))
    {
      become(
        [=](segment const& s)
        {
          event_id next = s.base() + s.events();
          send(archive, atom("segment"), next);
          forward_to(index);
        },
        on(atom("no segment"), arg_match) >> [=](event_id eid)
        {
          VAST_LOG_INFO("sent all segments to index (" << eid - 1 << " events)");
          become(default_behavior);
        });

      VAST_LOG_INFO("begins rebuilding index");
      send(index, atom("delete"));
      send(archive, atom("segment"), event_id{1});
    }

    actor search;
    auto search_host = *config_.get("search.host");
    auto search_port = *config_.as<unsigned>("search.port");
    if (config_.check("search-actor"))
    {
      search = spawn<search_actor, linked>(vast_dir, archive, index);
      VAST_LOG_ACTOR_INFO(
          "publishes search at " << search_host << ':' << search_port);

      publish(search, search_port, search_host.c_str());
    }
#ifdef VAST_HAVE_EDITLINE
    else if (config_.check("receiver-actor") || config_.check("console-actor"))
#else
    else if (config_.check("receiver-actor"))
#endif
    {
      VAST_LOG_ACTOR_VERBOSE(
          "connects to search at " << search_host << ":" << search_port);

      search = remote_actor(search_host, search_port);

#ifdef VAST_HAVE_EDITLINE
      if (config_.check("console-actor"))
      {
        auto c = spawn<console, detached+linked>(search, vast_dir / "console");
        delayed_send(c, std::chrono::milliseconds(200), atom("prompt"));
      }
#endif
    }

    actor receiver;
    auto receiver_host = *config_.get("receiver.host");
    auto receiver_port = *config_.as<unsigned>("receiver.port");
    if (config_.check("receiver-actor"))
    {
      receiver = spawn<receiver_actor>(tracker, archive, index, search);
      VAST_LOG_ACTOR_INFO(
          "publishes receiver at " << receiver_host << ':' << receiver_port);

      receiver->link_to(tracker);
      receiver->link_to(archive);
      receiver->link_to(index);

      // If we're running in "one-shot" mode where both ingestor and core
      // actors share the same program, then we initiate the teardown via the
      // ingestor.
      if (! config_.check("ingestor-actor"))
        receiver->link_to(*this);

      publish(receiver, receiver_port, receiver_host.c_str());
    }
    else if (config_.check("ingestor-actor"))
    {
      VAST_LOG_ACTOR_VERBOSE(
          "connects to receiver at " << receiver_host << ":" << receiver_port);

      receiver = remote_actor(receiver_host, receiver_port);
    }

    actor ingestor;
    if (config_.check("ingestor-actor"))
    {
      ingestor = spawn<ingestor_actor>(
          vast_dir,
          receiver,
          *config_.as<size_t>("ingest.max-events-per-chunk"),
          *config_.as<size_t>("ingest.max-segment-size") * 1000000,
          *config_.as<size_t>("ingest.batch-size"));

      // If we have a receiver in the same program, we're propagating the exit
      // to it rather than the program. This ensures proper delivery of
      // inflight segments from ingestor to receiver.
      if (config_.check("receiver-actor"))
        ingestor->link_to(receiver);
      else
        ingestor->link_to(*this);

#ifdef VAST_HAVE_BROCCOLI
      util::broccoli::init(config_.check("broccoli-messages"),
                           config_.check("broccoli-calltrace"));

      if (config_.check("ingest.broccoli-events"))
      {
        auto host = *config_.get("ingest.broccoli-host");
        auto port = *config_.as<unsigned>("ingest.broccoli-port");
        auto events =
          *config_.as<std::vector<std::string>>("ingest.broccoli-events");

        send(ingestor, atom("ingest"), atom("broccoli"), host, port,
             std::move(events));
      }
#endif

      if (config_.check("ingest.submit"))
        send(ingestor, atom("submit"));
      else if (auto file = config_.get("ingest.file-name"))
        send(ingestor, atom("ingest"), *config_.get("ingest.file-type"), *file,
             *config_.as<int32_t>("ingest.time-field"));
      else
        send_exit(ingestor, exit::done);
    }
  }
  catch (network_error const& e)
  {
    VAST_LOG_ACTOR_ERROR("encountered network error: " << e.what());
    send_exit(this, exit::error);
  }

  return default_behavior;
}

std::string program::describe() const
{
  return "program";
}

} // namespace vast

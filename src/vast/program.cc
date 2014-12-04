#include "vast/program.h"

#include <caf/io/all.hpp>
#include <cstdlib>
#include <csignal>
#include <iostream>
#include "vast/archive.h"
#include "vast/exporter.h"
#include "vast/file_system.h"
#include "vast/index.h"
#include "vast/importer.h"
#include "vast/logger.h"
#include "vast/profiler.h"
#include "vast/receiver.h"
#include "vast/serialization.h"
#include "vast/signal_monitor.h"
#include "vast/search.h"
#include "vast/tracker.h"
#include "vast/detail/type_manager.h"
#include "vast/sink/bro.h"
#include "vast/sink/json.h"
#include "vast/source/bro.h"
#include "vast/source/test.h"

#ifdef VAST_HAVE_PCAP
#include "vast/sink/pcap.h"
#include "vast/source/pcap.h"
#endif

#ifdef VAST_HAVE_EDITLINE
#include "vast/console.h"
#endif

using namespace caf;

namespace vast {

program::program(configuration config)
  : config_{std::move(config)}
{
}

message_handler program::make_handler()
{
  attach_functor(
      [=](uint32_t)
      {
        receiver_ = invalid_actor;
        tracker_ = invalid_actor;
        archive_ = invalid_actor;
        index_ = invalid_actor;
        search_ = invalid_actor;
        importer_ = invalid_actor;
        exporter_ = invalid_actor;
      });

  return
  {
    on(atom("run")) >> [=]
    {
      run();
    },
    on(atom("tracker")) >> [=]
    {
      return tracker_;
    },
    on(atom("signal"), arg_match) >> [=](int signal)
    {
      VAST_LOG_ACTOR_VERBOSE("received signal " << signal);
      if (signal == SIGINT || signal == SIGTERM)
        quit(exit::stop);
    },
    on(atom("success")) >> [] { /* nothing to do */ }
  };
}

std::string program::name() const
{
  return "program";
}

void program::run()
{
  auto dir = path{*config_.get("directory")}.complete();

  try
  {
    if (config_.check("core"))
    {
      *config_["receiver"] = true;
      *config_["tracker"] = true;
      *config_["archive"] = true;
      *config_["index"] = true;
      *config_["search"] = true;
    }

    auto monitor = spawn<signal_monitor, detached+linked>(this);
    send(monitor, atom("act"));

    if (config_.check("profiler.rusage")
        || config_.check("profiler.cpu")
        || config_.check("profiler.heap"))
    {
      auto secs = *config_.as<unsigned>("profiler.interval");
      auto prof = spawn<profiler, detached+linked>(
          dir / "log", std::chrono::seconds(secs));

      if (config_.check("profiler.cpu"))
      {
#ifdef VAST_USE_PERFTOOLS_CPU_PROFILER
        send(prof, atom("start"), atom("perftools"), atom("cpu"));
#else
        VAST_LOG_ACTOR_ERROR("not compiled with perftools CPU support");
        quit(exit::error);
        return;
#endif
      }

      if (config_.check("profiler.heap"))
      {
#ifdef VAST_USE_PERFTOOLS_HEAP_PROFILER
        send(prof, atom("start"), atom("perftools"), atom("heap"));
#else
        VAST_LOG_ACTOR_ERROR("not compiled with perftools heap support");
        quit(exit::error);
        return;
#endif
      }

      if (config_.check("profiler.rusage"))
        send(prof, atom("start"), atom("rusage"));
    }

    auto host = *config_.get("tracker.host");
    auto port = *config_.as<unsigned>("tracker.port");
    if (config_.check("tracker"))
    {
      VAST_LOG_ACTOR_INFO("publishes tracker at " <<
                          host << ':' << port);

      tracker_ = spawn<tracker, linked>(dir);
      caf::io::publish(tracker_, port, host.c_str());
    }
    else
    {
      VAST_LOG_ACTOR_INFO("connects to tracker at " <<
                          host << ':' << port);

      tracker_ = caf::io::remote_actor(host, port);
    }

    auto archive_name = *config_.get("archive.name");
    if (config_.check("archive"))
    {
      archive_ = spawn<archive, linked>(
          dir,
          *config_.as<size_t>("archive.max-segments"),
          *config_.as<size_t>("archive.max-segment-size") * 1000000);

      send(tracker_, atom("put"), "archive", archive_, archive_name);
    }

    auto index_name = *config_.get("index.name");
    if (config_.check("index"))
    {
      auto batch_size = *config_.as<size_t>("index.batch-size");
      auto max_events = *config_.as<size_t>("index.max-events");
      auto max_parts = *config_.as<size_t>("index.max-parts");
      auto active_parts = *config_.as<size_t>("index.active-parts");
      index_ = spawn<index, linked>(dir, batch_size, max_events, max_parts,
                                    active_parts);

      send(tracker_, atom("put"), "index", index_, index_name);
    }

    auto receiver_name = *config_.get("receiver.name");
    if (config_.check("receiver"))
    {
      receiver_ = spawn<receiver, linked>();
      send(tracker_, atom("put"), "receiver", receiver_, receiver_name);

      scoped_actor self;
      self->sync_send(tracker_, atom("identifier")).await(
          [=](actor const& identifier)
          {
            send(receiver_, atom("link"), atom("identifier"), identifier);
          });

      if (config_.check("archive"))
      {
        unlink_from(archive_);
        receiver_->link_to(archive_);
        send(tracker_, atom("link"), receiver_name, archive_name);
      }

      if (config_.check("index"))
      {
        unlink_from(index_);
        receiver_->link_to(index_);
        send(tracker_, atom("link"), receiver_name, index_name);
      }
    }

    auto search_name = *config_.get("search.name");
    if (config_.check("search"))
    {
      search_ = spawn<search, linked>();
      send(tracker_, atom("put"), "search", search_, search_name);

      if (config_.check("archive"))
        send(tracker_, atom("link"), search_name, archive_name);

      if (config_.check("index"))
        send(tracker_, atom("link"), search_name, index_name);
    }

    if (auto format = config_.get("importer"))
    {
      auto sch = load_and_parse<schema>(path{*config_.get("import.schema")});
      if (! sch)
      {
        VAST_LOG_ACTOR_ERROR("failed to load schema: " << sch.error());
        quit(exit::error);
        return;
      }

      auto sniff = config_.check("import.sniff-schema");
      auto r = config_.get("import.read");
      actor src;
      if (*format == "pcap")
      {
        if (sniff)
        {
          schema packet_schema;
          packet_schema.add(detail::make_packet_type());
          std::cout << packet_schema << std::flush;
          quit(exit::done);
          return;
        }

#ifdef VAST_HAVE_PCAP
        auto i = config_.get("import.interface");
        auto c = config_.as<size_t>("import.pcap-cutoff");
        auto m = *config_.as<size_t>("import.pcap-maxflows");
        std::string n = i ? *i : *r;
        src = spawn<source::pcap, detached>(*sch, std::move(n), c ? *c : -1, m);
#else
        VAST_LOG_ACTOR_ERROR("not compiled with pcap support");
        quit(exit::error);
        return;
#endif
      }
      else if (*format == "bro")
      {
        src = spawn<source::bro, detached>(*sch, *r, sniff);
      }
      else if (*format == "test")
      {
        auto id = *config_.as<event_id>("import.test-id");
        auto events = *config_.as<uint64_t>("import.test-events");
        src = spawn<source::test>(*sch, id, events);
      }
      else
      {
        VAST_LOG_ACTOR_ERROR("invalid import format: " << *format);
        quit(exit::error);
        return;
      }

      io::compression compression;
      auto method = *config_.get("import.compression");
      if (method == "null")
      {
        compression = io::null;
      }
      else if (method == "lz4")
      {
        compression = io::lz4;
      }
      else if (method == "snappy")
      {
#ifdef VAST_HAVE_SNAPPY
        compression = io::snappy;
#else
        VAST_LOG_ACTOR_ERROR("not compiled with snappy support");
        quit(exit::error);
        return;
#endif
      }
      else
      {
        VAST_LOG_ACTOR_ERROR("unknown compression method");
        quit(exit::error);
        return;
      }

      auto batch_size = *config_.as<uint64_t>("import.batch-size");
      importer_ = spawn<importer, linked>(dir, batch_size, compression);
      send(importer_, atom("source"), src);

      auto importer_name = *config_.get("import.name");
      send(tracker_, atom("put"), "importer", importer_, importer_name);

      if (config_.check("receiver"))
      {
        // In case we're running in "one-shot" mode where both IMPORTER and
        // RECEIVER share the same program, we initiate the shutdown
        // via IMPORTER to ensure proper delivery of inflight segments from
        // IMPORTER to RECEIVER.
        unlink_from(receiver_);
        importer_->link_to(receiver_);
        send(tracker_, atom("link"), importer_name, receiver_name);
      }
    }
    else if (auto format = config_.get("exporter"))
    {
      auto sch = load_and_parse<schema>(path{*config_.get("export.schema")});
      if (! sch)
      {
        VAST_LOG_ACTOR_ERROR("failed to load schema: " << sch.error());
        quit(exit::error);
        return;
      }

      auto w = config_.get("export.write");
      assert(w);
      actor snk;
      if (*format == "pcap")
      {
#ifdef VAST_HAVE_PCAP
        auto flush = config_.as<uint64_t>("export.pcap-flush");
        assert(flush);
        snk = spawn<sink::pcap, detached>(*sch, *w, *flush);
#else
        VAST_LOG_ACTOR_ERROR("not compiled with pcap support");
        quit(exit::error);
        return;
#endif
      }
      else if (*format == "bro")
      {
        snk = spawn<sink::bro>(std::move(*w));
      }
      else if (*format == "json")
      {
        path p{std::move(*w)};
        if (p != "-")
        {
          p = p.complete();
          if (! exists(p.parent()))
          {
            auto t = mkdir(p.parent());
            if (! t)
            {
              VAST_LOG_ACTOR_ERROR("failed to create directory: " <<
                                   p.parent());
              quit(exit::error);
              return;
            }
          }
        }

        snk = spawn<sink::json>(std::move(p));
      }
      else
      {
        VAST_LOG_ACTOR_ERROR("invalid export format: " << *format);
        quit(exit::error);
        return;
      }

      exporter_ = spawn<exporter, linked>();
      send(exporter_, atom("add"), std::move(snk));

      auto exporter_name = *config_.get("export.name");
      send(tracker_, atom("put"), "exporter", exporter_, exporter_name);

      auto limit = *config_.as<uint64_t>("export.limit");
      if (limit > 0)
        send(exporter_, atom("limit"), limit);

      auto query = config_.get("export.query");
      assert(query);

      sync_send(tracker_, atom("get"), search_name).then(
          on_arg_match >> [=](error const& e)
          {
            VAST_LOG_ACTOR_ERROR("could not get SEARCH: " << e);
            quit(exit::error);
          },
          [=](actor const& srch)
          {
            sync_send(srch, atom("query"), exporter_, *query).then(
                on_arg_match >> [=](error const& e)
                {
                  VAST_LOG_ACTOR_ERROR("got invalid query: " << e);
                  quit(exit::error);
                },
                [=](expression const& ast, actor qry)
                {
                  VAST_LOG_ACTOR_DEBUG("instantiated query for: " << ast);
                  exporter_->link_to(qry);
                  send(qry, atom("extract"), limit);
                },
                others() >> [=]
                {
                  VAST_LOG_ACTOR_ERROR("got unexpected reply: " <<
                                       to_string(last_dequeued()));

                  quit(exit::error);
                });
          });
    }
    else if (config_.check("console"))
    {
#ifdef VAST_HAVE_EDITLINE
      sync_send(tracker_, atom("get"), search_name).then(
          [=](actor const& search)
          {
            auto c = spawn<console, linked>(search, dir / "console");
            delayed_send(c, std::chrono::milliseconds(200), atom("prompt"));
          },
          [=](error const& e)
          {
            VAST_LOG_ACTOR_ERROR(e);
            quit(exit::error);
          });
#else
      VAST_LOG_ACTOR_ERROR("not compiled with editline support");
      quit(exit::error);
#endif
    }
  }
  catch (network_error const& e)
  {
    VAST_LOG_ACTOR_ERROR("encountered network error: " << e.what());
    quit(exit::error);
  }
}

} // namespace vast

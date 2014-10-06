#include "vast/program.h"

#include <caf/io/all.hpp>
#include <cstdlib>
#include <csignal>
#include <iostream>
#include "vast/archive.h"
#include "vast/exporter.h"
#include "vast/file_system.h"
#include "vast/identifier.h"
#include "vast/index.h"
#include "vast/importer.h"
#include "vast/logger.h"
#include "vast/profiler.h"
#include "vast/receiver.h"
#include "vast/search.h"
#include "vast/serialization.h"
#include "vast/signal_monitor.h"
#include "vast/detail/type_manager.h"
#include "vast/sink/bro.h"
#include "vast/sink/json.h"
#include "vast/source/bro.h"
#include "vast/source/bgpdump.h"
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

message_handler program::act()
{
  attach_functor(
      [=](uint32_t)
      {
        receiver_ = invalid_actor;
        identifier_ = invalid_actor;
        archive_ = invalid_actor;
        index_ = invalid_actor;
        search_ = invalid_actor;
      });

  return
  {
    on(atom("run")) >> [=]
    {
      run();
    },
    on(atom("receiver")) >> [=]
    {
      return receiver_;
    },
    on(atom("identifier")) >> [=]
    {
      return identifier_;
    },
    on(atom("archive")) >> [=]
    {
      return archive_;
    },
    on(atom("index")) >> [=]
    {
      return index_;
    },
    on(atom("search")) >> [=]
    {
      return search_;
    },
    on(atom("signal"), arg_match) >> [=](int signal)
    {
      VAST_LOG_ACTOR_VERBOSE("received signal " << signal);
      if (signal == SIGINT || signal == SIGTERM)
        quit(exit::stop);
    }
  };
}

std::string program::describe() const
{
  return "program";
}

void program::run()
{
  auto dir = path{*config_.get("directory")}.complete();

  try
  {
    bool core = config_.check("core");
    *config_["receiver"] = core;
    *config_["identifier"] = core;
    *config_["archive"] = core;
    *config_["index"] = core;
    *config_["search"] = core;

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

    auto identifier_host = *config_.get("identifier.host");
    auto identifier_port = *config_.as<unsigned>("identifier.port");
    if (config_.check("identifier"))
    {
      identifier_ = spawn<identifier>(dir);
      caf::io::publish(identifier_, identifier_port, identifier_host.c_str());

      VAST_LOG_ACTOR_INFO("publishes identifier at " << identifier_host << ':'
                          << identifier_port);
    }
    else if (config_.check("receiver"))
    {
      VAST_LOG_ACTOR_VERBOSE("connects to identifier at " << identifier_host <<
                             ':' << identifier_port);

      identifier_ = caf::io::remote_actor(identifier_host, identifier_port);
    }

    auto archive_host = *config_.get("archive.host");
    auto archive_port = *config_.as<unsigned>("archive.port");
    if (config_.check("archive"))
    {
      archive_ = spawn<archive>(
          dir,
          *config_.as<size_t>("archive.max-segments"),
          *config_.as<size_t>("archive.max-segment-size") * 1000000);

      VAST_LOG_ACTOR_INFO(
          "publishes archive at " << archive_host << ':' << archive_port);

      caf::io::publish(archive_, archive_port, archive_host.c_str());
    }
    else if (config_.check("receiver")
             || config_.check("search")
             || config_.check("index.rebuild"))
    {
      VAST_LOG_ACTOR_VERBOSE(
          "connects to archive at " << archive_host << ':' << archive_port);

      archive_ = caf::io::remote_actor(archive_host, archive_port);
    }

    auto index_host = *config_.get("index.host");
    auto index_port = *config_.as<unsigned>("index.port");
    if (config_.check("index"))
    {
      auto batch_size = *config_.as<size_t>("index.batch-size");
      auto max_events = *config_.as<size_t>("index.max-events");
      auto max_parts = *config_.as<size_t>("index.max-parts");
      auto active_parts = *config_.as<size_t>("index.active-parts");
      index_ = spawn<index>(dir, batch_size, max_events, max_parts,
                            active_parts);

      VAST_LOG_ACTOR_INFO(
          "publishes index at " << index_host << ':' << index_port);

      caf::io::publish(index_, index_port, index_host.c_str());
    }
    else if (config_.check("receiver")
             || config_.check("search")
             || config_.check("index.rebuild"))
    {
      VAST_LOG_ACTOR_VERBOSE("connects to index at " <<
                           index_host << ":" << index_port);

      index_ = caf::io::remote_actor(index_host, index_port);
    }

    if (config_.check("index.rebuild"))
    {
      // TODO
    }

    auto search_host = *config_.get("search.host");
    auto search_port = *config_.as<unsigned>("search.port");
    if (config_.check("search"))
    {
      search_ = spawn<search_actor>(dir, archive_, index_);
      VAST_LOG_ACTOR_INFO(
          "publishes search at " << search_host << ':' << search_port);

      caf::io::publish(search_, search_port, search_host.c_str());
    }
    else if (config_.check("receiver")
             || config_.check("console")
             || config_.check("exporter"))
    {
      VAST_LOG_ACTOR_VERBOSE(
          "connects to search at " << search_host << ":" << search_port);

      search_ = caf::io::remote_actor(search_host, search_port);

      if (config_.check("console"))
      {
#ifdef VAST_HAVE_EDITLINE
        auto c = spawn<console, detached+linked>(search_, dir / "console");
        delayed_send(c, std::chrono::milliseconds(200), atom("prompt"));
#else
        VAST_LOG_ACTOR_ERROR("not compiled with editline support");
        quit(exit::error);
        return;
#endif
      }
    }

    auto receiver_host = *config_.get("receiver.host");
    auto receiver_port = *config_.as<unsigned>("receiver.port");
    if (config_.check("receiver"))
    {
      receiver_ = spawn<receiver>(identifier_, archive_, index_, search_);
      VAST_LOG_ACTOR_INFO(
          "publishes receiver at " << receiver_host << ':' << receiver_port);

      // We always initiate the shutdown via the receiver, regardless of
      // whether we have an importer in our process.
      link_to(receiver_);
      receiver_->link_to(identifier_);
      receiver_->link_to(archive_);
      receiver_->link_to(index_);
      receiver_->link_to(search_);

      caf::io::publish(receiver_, receiver_port, receiver_host.c_str());
    }
    else if (config_.check("importer"))
    {
      VAST_LOG_ACTOR_VERBOSE(
          "connects to receiver at " << receiver_host << ":" << receiver_port);

      receiver_ = caf::io::remote_actor(receiver_host, receiver_port);
    }

    actor imp0rter;
    if (auto format = config_.get("importer"))
    {
      auto s = config_.get("import.schema");
      schema sch;
      if (s)
      {
        auto contents = load(*s);
        if (! contents)
        {
          VAST_LOG_ACTOR_ERROR("failed to load schema: " << contents.error());
          quit(exit::error);
          return;
        }

        auto t = to<schema>(*contents);
        if (! t)
        {
          VAST_LOG_ACTOR_ERROR("invalid schema: " << t.error());
          quit(exit::error);
          return;
        }

        sch = *t;
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
        src = spawn<source::pcap, detached>(sch, std::move(n), c ? *c : -1, m);
#else
        VAST_LOG_ACTOR_ERROR("not compiled with pcap support");
        quit(exit::error);
        return;
#endif
      }
      else if (*format == "bro")
      {
        src = spawn<source::bro, detached>(sch, *r, sniff);
      }
      else if (*format == "bgpdump")
      {
        src = spawn<source::bgpdump, detached>(sch, *r, sniff);
      }
      else if (*format == "test")
      {
        auto id = *config_.as<event_id>("import.test-id");
        auto events = *config_.as<uint64_t>("import.test-events");
        src = spawn<source::test>(sch, id, events);
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
      imp0rter = spawn<importer>(dir, receiver_, batch_size, compression);
      send(imp0rter, atom("add"), src);

      if (config_.check("receiver"))
        // We're running in "one-shot" mode where both IMPORTER and RECEIVER
        // share the same program. In this case we initiate the teardown
        // via IMPORTER as this ensures proper delivery of inflight segments
        // from IMPORTER to RECEIVER.
        imp0rter->link_to(receiver_);
      else
        // If we're running in ingestion mode without RECEIVER, we're
        // independent and terminate as soon as IMPORTER has finished.
        link_to(imp0rter);
    }
    else if (auto format = config_.get("exporter"))
    {
      sync_send(search_, atom("schema")).then(
          on_arg_match >> [=](schema const& sch)
          {
            auto w = config_.get("export.write");
            assert(w);
            actor snk;
            if (*format == "pcap")
            {
#ifdef VAST_HAVE_PCAP
              auto flush = config_.as<uint64_t>("export.pcap-flush");
              assert(flush);
              snk = spawn<sink::pcap, detached>(sch, *w, *flush);
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

            auto exp0rter = spawn<exporter, linked>();
            send(exp0rter, atom("add"), std::move(snk));

            auto limit = *config_.as<uint64_t>("export.limit");
            if (limit > 0)
              send(exp0rter, atom("limit"), limit);

            auto query = config_.get("export.query");
            assert(query);
            sync_send(search_, atom("query"), exp0rter, *query).then(
                on_arg_match >> [=](error const& e)
                {
                  VAST_LOG_ACTOR_ERROR("got invalid query: " << e);
                  quit(exit::error);
                },
                [=](expression const& ast, actor qry)
                {
                  VAST_LOG_ACTOR_DEBUG("instantiated query for: " << ast);
                  exp0rter->link_to(qry);
                  send(qry, atom("extract"), limit);
                },
                others() >> [=]
                {
                  VAST_LOG_ACTOR_ERROR("got unexpected reply: " <<
                                       to_string(last_dequeued()));

                  quit(exit::error);
                });
          },
          others() >> [=]
          {
            VAST_LOG_ACTOR_ERROR("expected schema, got: " <<
                                 to_string(last_dequeued()));
            quit(exit::error);
          });

    }
  }
  catch (network_error const& e)
  {
    VAST_LOG_ACTOR_ERROR("encountered network error: " << e.what());
    quit(exit::error);
  }
}

} // namespace vast

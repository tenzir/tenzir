#include "vast/actor/program.h"

#include <caf/io/all.hpp>
#include <cstdlib>
#include <csignal>
#include <iostream>
#include "vast/filesystem.h"
#include "vast/logger.h"
#include "vast/serialization.h"
#include "vast/detail/type_manager.h"
#include "vast/actor/archive.h"
#include "vast/actor/exporter.h"
#include "vast/actor/index.h"
#include "vast/actor/importer.h"
#include "vast/actor/receiver.h"
#include "vast/actor/search.h"
#include "vast/actor/signal_monitor.h"
#include "vast/actor/tracker.h"
#include "vast/actor/profiler.h"
#include "vast/actor/sink/bro.h"
#include "vast/actor/sink/json.h"
#include "vast/actor/source/bro.h"
#include "vast/actor/source/bgpdump.h"
#include "vast/actor/source/test.h"

#ifdef VAST_HAVE_PCAP
#include "vast/actor/sink/pcap.h"
#include "vast/actor/source/pcap.h"
#endif

#ifdef VAST_HAVE_EDITLINE
#include "vast/actor/console.h"
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
      auto t = run();
      if (t)
        return make_message(atom("ok"));
      else
        return make_message(std::move(t.error()));
    },
    on(atom("tracker")) >> [=]
    {
      return tracker_;
    },
    on(atom("signal"), arg_match) >> [=](int signal)
    {
      VAST_VERBOSE(this, "received signal", signal);
      if (signal == SIGINT || signal == SIGTERM)
        quit(exit::stop);
    },
    [=](error const& e)
    {
      VAST_ERROR(this, "got error:", e);
      quit(exit::error);
    },
    on(atom("ok")) >> [] { /* nothing to do */ }
  };
}

std::string program::name() const
{
  return "program";
}

trial<void> program::run()
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

    send(spawn<signal_monitor, linked>(this), atom("act"));

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
        quit(exit::error);
        return error{"not compiled with perftools CPU support"};
#endif
      }

      if (config_.check("profiler.heap"))
      {
#ifdef VAST_USE_PERFTOOLS_HEAP_PROFILER
        send(prof, atom("start"), atom("perftools"), atom("heap"));
#else
        quit(exit::error);
        return error{"not compiled with perftools heap support"};
#endif
      }

      if (config_.check("profiler.rusage"))
        send(prof, atom("start"), atom("rusage"));
    }

    auto host = *config_.get("tracker.host");
    auto port = *config_.as<unsigned>("tracker.port");
    if (config_.check("tracker"))
    {
      VAST_INFO(this, "publishes tracker at", host << ':' << port);
      tracker_ = spawn<tracker, linked>(dir);
      caf::io::publish(tracker_, port, host.c_str());
    }
    else
    {
      VAST_INFO(this, "connects to tracker at", host << ':' << port);
      tracker_ = caf::io::remote_actor(host, port);
    }

    scoped_actor self;
    optional<error> abort;
    message_handler ok_or_quit = (
        on(atom("ok")) >> [] { /* do nothing */ },
        [&](error& e)
        {
          abort = std::move(e);
          quit(exit::error);
        },
        others() >> [&]
        {
          abort = error{"got unexpected message: ",
                        to_string(self->last_dequeued())};
          quit(exit::error);
        });

    auto link = *config_.as<std::vector<std::string>>("tracker.link");
    if (! link.empty())
    {
      assert(link.size() == 2);
      self->sync_send(tracker_, atom("link"), link[0], link[1]).await(
        on(atom("ok")) >> [&]
        {
          VAST_INFO(this, "successfully linked", link[0], "to", link[1]);
          quit(exit::done);
        },
        [&](error& e)
        {
          abort = std::move(e);
          quit(exit::error);
        });
      if (abort)
        return std::move(*abort);
      else
        return nothing;
    }

    auto archive_name = *config_.get("archive.name");
    if (config_.check("archive"))
    {
      archive_ = spawn<archive, linked>(
          dir,
          *config_.as<size_t>("archive.max-segments"),
          *config_.as<size_t>("archive.max-segment-size") * 1000000);

      self->sync_send(tracker_, atom("put"), "archive", archive_, archive_name)
        .await(ok_or_quit);
      if (abort)
        return std::move(*abort);
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

      self->sync_send(tracker_, atom("put"), "index", index_, index_name)
        .await(ok_or_quit);
      if (abort)
        return std::move(*abort);
    }

    auto receiver_name = *config_.get("receiver.name");
    if (config_.check("receiver"))
    {
      receiver_ = spawn<receiver, linked>();

      // TRACKER needs to remain alive at least as long as RECEIVER, because
      // RECEIVER asks IDENTIFIER (which resides inside TRACKER) for chunk IDs.
      unlink_from(tracker_);

      // If RECEIVER and TRACKER live in different processes, a failing
      // RECEIVER should not take down VAST's central component.
      if (config_.check("tracker"))
        tracker_->link_to(receiver_);

      self->sync_send(tracker_, atom("put"), "receiver", receiver_,
                      receiver_name).await(ok_or_quit);
      if (abort)
        return std::move(*abort);

      self->sync_send(tracker_, atom("identifier")).await(
          [&](actor const& identifier)
          {
            send(receiver_, atom("set"), atom("identifier"), identifier);
          });

      if (config_.check("archive"))
      {
        unlink_from(archive_);
        receiver_->link_to(archive_);
        self->sync_send(tracker_, atom("link"), receiver_name, archive_name)
          .await(ok_or_quit);
        if (abort)
          return std::move(*abort);
      }

      if (config_.check("index"))
      {
        unlink_from(index_);
        receiver_->link_to(index_);
        self->sync_send(tracker_, atom("link"), receiver_name, index_name)
          .await(ok_or_quit);
        if (abort)
          return std::move(*abort);
      }

    }

    auto search_name = *config_.get("search.name");
    if (config_.check("search"))
    {
      search_ = spawn<search>();
      self->sync_send(tracker_, atom("put"), "search", search_, search_name)
        .await(ok_or_quit);
      if (abort)
        return std::move(*abort);

      if (config_.check("archive"))
      {
        self->sync_send(tracker_, atom("link"), search_name, archive_name)
          .await(ok_or_quit);
        if (abort)
          return std::move(*abort);
      }

      if (config_.check("index"))
      {
        self->sync_send(tracker_, atom("link"), search_name, index_name)
          .await(ok_or_quit);
        if (abort)
          return std::move(*abort);
      }

      if (config_.check("receiver"))
        search_->link_to(receiver_);
      else
        link_to(search_);
    }

    schema sch;
    if (auto format = config_.get("importer"))
    {
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
        quit(exit::error);
        return error{"not compiled with snappy support"};
#endif
      }
      else
      {
        quit(exit::error);
        return error{"unknown compression method"};
      }

      auto batch_size = *config_.as<uint64_t>("import.batch-size");
      importer_ = spawn<importer, linked>(dir, batch_size, compression);

      auto importer_name = *config_.get("import.name");
      self->sync_send(tracker_, atom("put"), "importer", importer_,
                      importer_name).await(ok_or_quit);
      if (abort)
        return std::move(*abort);

      if (config_.check("receiver"))
      {
        // If this program accomodates both IMPORTER and RECEIVER, we must
        // initiate the shutdown via IMPORTER to ensure proper delivery of
        // inflight chunks from IMPORTER to RECEIVER.
        unlink_from(receiver_);
        importer_->link_to(receiver_);
      }

      self->sync_send(tracker_, atom("link"), importer_name, receiver_name)
        .await(ok_or_quit);
      if (abort)
        return std::move(*abort);

      if (auto schema_file = config_.get("import.schema"))
      {
        auto t = load_and_parse<schema>(path{*schema_file});
        if (! t)
        {
          quit(exit::error);
          return error{"failed to load schema: ", t.error()};
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
          return nothing;
        }

#ifdef VAST_HAVE_PCAP
        auto i = config_.get("import.interface");
        auto c = config_.as<size_t>("import.pcap-cutoff");
        auto m = *config_.as<size_t>("import.pcap-maxflows");
        std::string n = i ? *i : *r;
        src = spawn<source::pcap, detached>(sch, std::move(n), c ? *c : -1, m);
#else
        quit(exit::error);
        return error{"not compiled with pcap support"};
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
        quit(exit::error);
        return error{"invalid import format: ", *format};
      }

      send(importer_, atom("add"), atom("source"), src);
    }
    else if (auto format = config_.get("exporter"))
    {
      auto exporter_name = *config_.get("export.name");
      self->sync_send(tracker_, atom("put"), "exporter", exporter_,
                      exporter_name).await(ok_or_quit);
      if (abort)
        return std::move(*abort);

      auto limit = *config_.as<uint64_t>("export.limit");
      if (limit > 0)
        send(exporter_, atom("limit"), limit);

      exporter_ = spawn<exporter, linked>();
      if (auto schema_file = config_.get("export.schema"))
      {
        auto t = load_and_parse<schema>(path{*schema_file});
        if (! t)
        {
          quit(exit::error);
          return error{"failed to load schema: ", t.error()};
        }

        sch = *t;
      }

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
        quit(exit::error);
        return error{"not compiled with pcap support"};
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
              quit(exit::error);
              return error{"failed to create directory: ", p.parent()};
            }
          }
        }

        snk = spawn<sink::json>(std::move(p));
      }
      else
      {
        quit(exit::error);
        return error{"invalid export format: ", *format};
      }

      send(exporter_, atom("add"), std::move(snk));

      auto query = config_.get("export.query");
      assert(query);
      self->sync_send(tracker_, atom("get"), search_name).await(
          on_arg_match >> [&](error& e)
          {
            abort = std::move(e);
            quit(exit::error);
          },
          [&](actor const& srch)
          {
            self->sync_send(srch, atom("query"), exporter_, *query).await(
                on_arg_match >> [&](error& e)
                {
                  abort = std::move(e);
                  quit(exit::error);
                },
                [&](expression const& ast, actor qry)
                {
                  VAST_DEBUG(this, "instantiated query for:", ast);
                  exporter_->link_to(qry);
                  send(qry, atom("extract"), limit);
                },
                others() >> [&]()
                {
                  abort = error{"got unexpected reply:",
                                to_string(last_dequeued())};
                  quit(exit::error);
                });
          });
      if (abort)
        return std::move(*abort);
    }
    else if (config_.check("console"))
    {
#ifdef VAST_HAVE_EDITLINE
      self->sync_send(tracker_, atom("get"), search_name).await(
          [&](actor const& search)
          {
            auto c = spawn<console, linked>(search, dir / "console");
            delayed_send(c, std::chrono::milliseconds(200), atom("prompt"));
          },
          [&](error& e)
          {
            abort = std::move(e);
            quit(exit::error);
          });
      if (abort)
        return std::move(*abort);
#else
      return error{"not compiled with editline support"};
#endif
    }

    return nothing;
  }
  catch (network_error const& e)
  {
    quit(exit::error);
    return error{"encountered network error:", e.what()};
  }
}

} // namespace vast

#include "vast/actor/program.h"

#include <caf/io/all.hpp>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include "vast/filesystem.h"
#include "vast/logger.h"
#include "vast/query_options.h"
#include "vast/actor/accountant.h"
#include "vast/actor/archive.h"
#include "vast/actor/exporter.h"
#include "vast/actor/index.h"
#include "vast/actor/importer.h"
#include "vast/actor/receiver.h"
#include "vast/actor/search.h"
#include "vast/actor/tracker.h"
#include "vast/actor/profiler.h"
#include "vast/actor/sink/bro.h"
#include "vast/actor/sink/json.h"
#include "vast/actor/source/bro.h"
#include "vast/actor/source/bgpdump.h"
#include "vast/actor/source/test.h"
#include "vast/util/system.h"

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
  : default_actor{"program"},
    config_{std::move(config)}
{
}

void program::on_exit()
{
  receiver_ = invalid_actor;
  tracker_ = invalid_actor;
  archive_ = invalid_actor;
  index_ = invalid_actor;
  search_ = invalid_actor;
  importer_ = invalid_actor;
  exporter_ = invalid_actor;
}

behavior program::make_behavior()
{
  return
  {
    [=](run_atom)
    {
      auto t = run();
      if (t)
        return make_message(ok_atom::value);
      else
        return make_message(std::move(t.error()));
    },
    [=](tracker_atom)
    {
      return tracker_;
    },
    [=](signal_atom, int signal)
    {
      VAST_VERBOSE(this, "received signal", signal);
      if (signal == SIGINT || signal == SIGTERM)
      {
        // We cut the flow of events at the source and let them trickle through
        // the pipeline so that we end up in a consistent state for a given
        // number of events.
        if (config_.check("importer"))
          send_exit(importer_, exit::done);
        else if (config_.check("receiver"))
          send_exit(receiver_, exit::done);
        else
          quit(exit::stop);
      }
    },
    [](ok_atom) { /* nothing to do */ },
    [=](error const& e)
    {
      VAST_ERROR(this, "got error:", e);
      quit(exit::error);
    },
    catch_unexpected()
  };
}

trial<void> program::run()
{
  auto dir = path{*config_.get("directory")}.complete();
  auto log_dir = dir / path{*config_.get("log.directory")};
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

    if (config_.check("profiler.cpu") || config_.check("profiler.heap"))
    {
      auto secs = *config_.as<unsigned>("profiler.interval");
      auto prof = spawn<profiler, detached+linked>(
          log_dir, std::chrono::seconds(secs));
      if (config_.check("profiler.cpu"))
      {
#ifdef VAST_USE_PERFTOOLS_CPU_PROFILER
        send(prof, start_atom::value, perftools_atom::value, cpu_atom::value);
#else
        quit(exit::error);
        return error{"not compiled with perftools CPU support"};
#endif
      }
      if (config_.check("profiler.heap"))
      {
#ifdef VAST_USE_PERFTOOLS_HEAP_PROFILER
        send(prof, start_atom::value, perftools_atom::value, heap_atom::value);
#else
        quit(exit::error);
        return error{"not compiled with perftools heap support"};
#endif
      }
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
      VAST_VERBOSE(this, "connects to tracker at", host << ':' << port);
      tracker_ = caf::io::remote_actor(host, port);
    }

    scoped_actor self;
    optional<error> abort;
    message_handler ok_or_quit{
        [](ok_atom) { /* do nothing */ },
        [&](error& e)
        {
          abort = std::move(e);
          quit(exit::error);
        },
        others() >> [&]
        {
          abort = error{"got unexpected message from ",
                        to_string(self->current_sender()), ": ",
                        to_string(self->current_message())};
          quit(exit::error);
        }};

    auto link = *config_.as<std::vector<std::string>>("tracker.link");
    if (! link.empty())
    {
      assert(link.size() == 2);
      self->sync_send(tracker_, link_atom::value, link[0], link[1]).await(
        [&](ok_atom)
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

    actor acct;
    if (config_.check("archive")
        || config_.check("index")
        || config_.check("importer"))
      acct = spawn<accountant<uint64_t>, detached+linked>(log_dir);

    auto archive_name = *config_.get("archive.name");
    if (config_.check("archive"))
    {
      archive_ = spawn<archive, priority_aware+linked>(
          dir,
          *config_.as<size_t>("archive.max-segments"),
          *config_.as<size_t>("archive.max-segment-size") * 1000000);
      send(archive_, accountant_atom::value, acct);
      self->sync_send(tracker_, put_atom::value, "archive", archive_,
                      archive_name).await(ok_or_quit);
      if (abort)
        return std::move(*abort);
    }

    auto index_name = *config_.get("index.name");
    if (config_.check("index"))
    {
      auto max_events = *config_.as<size_t>("index.part-size");
      auto passive_parts = *config_.as<size_t>("index.part-passive");
      auto active_parts = *config_.as<size_t>("index.part-active");
      index_ = spawn<index, priority_aware+linked>(
          dir, max_events, passive_parts, active_parts);
      send(index_, accountant_atom::value, acct);
      self->sync_send(tracker_, put_atom::value, "index", index_, index_name)
        .await(ok_or_quit);
      if (abort)
        return std::move(*abort);
    }

    auto receiver_name = *config_.get("receiver.name");
    if (config_.check("receiver"))
    {
      receiver_ = spawn<receiver, priority_aware+linked>();
      // Whenever we have a RECEIVER, it initiates the shutdown because it
      // depends on IDENTIFIER from inside TRACKER.
      unlink_from(tracker_);
      // If RECEIVER and TRACKER live in different processes, a failing
      // RECEIVER should not take down VAST's central component.
      if (config_.check("tracker"))
        tracker_->link_to(receiver_);
      self->sync_send(tracker_, put_atom::value, "receiver", receiver_,
                      receiver_name).await(ok_or_quit);
      if (abort)
        return std::move(*abort);
      self->sync_send(tracker_, get_atom::value, "identifier").await(
          [&](actor const& identifier)
          {
            send(receiver_, set_atom::value, identifier_atom::value,
                 identifier);
          });

      if (config_.check("archive"))
      {
        unlink_from(archive_);
        receiver_->link_to(archive_);
        if (config_.check("tracker"))
        {
          tracker_->unlink_from(receiver_);
          tracker_->link_to(archive_);
        }
        self->sync_send(tracker_, link_atom::value, receiver_name, archive_name)
          .await(ok_or_quit);
        if (abort)
          return std::move(*abort);
      }

      if (config_.check("index"))
      {
        unlink_from(index_);
        receiver_->link_to(index_);
        if (config_.check("tracker"))
        {
          tracker_->unlink_from(receiver_);
          tracker_->link_to(index_);
        }
        self->sync_send(tracker_, link_atom::value, receiver_name, index_name)
          .await(ok_or_quit);
        if (abort)
          return std::move(*abort);
      }
    }

    auto search_name = *config_.get("search.name");
    if (config_.check("search"))
    {
      search_ = spawn<search, linked>();
      self->sync_send(tracker_, put_atom::value, "search", search_, search_name)
        .await(ok_or_quit);
      if (abort)
        return std::move(*abort);
      if (config_.check("archive"))
      {
        self->sync_send(tracker_, link_atom::value, search_name, archive_name)
          .await(ok_or_quit);
        if (abort)
          return std::move(*abort);
      }
      if (config_.check("index"))
      {
        self->sync_send(tracker_, link_atom::value, search_name, index_name)
          .await(ok_or_quit);
        if (abort)
          return std::move(*abort);
      }
    }

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

      auto chunk_size = *config_.as<uint64_t>("import.chunk-size");
      importer_ = spawn<importer, priority_aware+linked>(dir, chunk_size,
                                                         compression);
      send(importer_, accountant_atom::value, acct);
      auto importer_name = *config_.get("import.name");
      self->sync_send(tracker_, put_atom::value, "importer", importer_,
                      importer_name).await(ok_or_quit);
      if (abort)
        return std::move(*abort);
      if (config_.check("receiver"))
      {
        // If this program accomodates both IMPORTER and RECEIVER, we must
        // initiate the shutdown via IMPORTER to ensure proper delivery of
        // inflight chunks from IMPORTER to RECEIVER.
        unlink_from(importer_);
        importer_->link_to(receiver_);
      }
      self->sync_send(tracker_, link_atom::value, importer_name, receiver_name)
        .await(ok_or_quit);
      if (abort)
        return std::move(*abort);
      actor src;
      if (*format == "pcap")
      {
#ifdef VAST_HAVE_PCAP
        auto r = config_.get("import.read");
        auto i = config_.get("import.interface");
        auto n = std::string{i ? *i : *r};
        auto c = config_.as<size_t>("import.pcap-cutoff");
        auto cutoff = c ? *c : -1;
        auto m = *config_.as<size_t>("import.pcap-flow-max");
        auto a = *config_.as<size_t>("import.pcap-flow-age");
        auto e = *config_.as<size_t>("import.pcap-flow-expiry");
        auto p = *config_.as<int64_t>("import.pcap-pseudo-realtime");
        src = spawn<source::pcap, priority_aware+detached>(
            std::move(n), cutoff, m, a, e, p);
#else
        quit(exit::error);
        return error{"not compiled with pcap support"};
#endif
      }
      else if (*format == "bro")
      {
        auto r = config_.get("import.read");
        src = spawn<source::bro, priority_aware+detached>(*r);
      }
      else if (*format == "bgpdump")
      {
        auto r = config_.get("import.read");
        src = spawn<source::bgpdump, priority_aware+detached>(*r);
      }
      else if (*format == "test")
      {
        auto id = *config_.as<event_id>("import.test-id");
        auto events = *config_.as<uint64_t>("import.test-events");
        src = spawn<source::test, priority_aware>(id, events);
      }
      else
      {
        quit(exit::error);
        return error{"invalid import format: ", *format};
      }
      if (auto schema_file = config_.get("import.schema"))
      {
        auto t = load_and_parse<schema>(path{*schema_file});
        if (! t)
        {
          quit(exit::error);
          return error{"failed to load schema: ", t.error()};
        }
        self->send(src, *t);
      }
      if (config_.check("import.sniff-schema"))
      {
        self->sync_send(src, schema_atom::value).await(
          [=](schema const& sch)
          {
            std::cout << sch << std::flush;
            send_exit(src, exit::done);
            quit(exit::done);
          });
        return nothing;
      };
      send(importer_, add_atom::value, source_atom::value, src);
    }
    else if (auto format = config_.get("exporter"))
    {
      auto exporter_name = *config_.get("export.name");
      self->sync_send(tracker_, put_atom::value, "exporter", exporter_,
                      exporter_name).await(ok_or_quit);
      if (abort)
        return std::move(*abort);
      auto limit = *config_.as<uint64_t>("export.limit");
      if (limit > 0)
        send(exporter_, limit_atom::value, limit);
      exporter_ = spawn<exporter, linked>();
      schema sch;
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
      send(exporter_, add_atom::value, std::move(snk));

      auto expr = config_.get("export.expression");
      assert(expr);
      auto opts = no_query_options;
      if (config_.check("export.continuous"))
        opts = continuous;
      else if (config_.check("export.historical"))
        opts = historical;
      else if (config_.check("export.unified"))
        opts = unified;
      self->sync_send(tracker_, get_atom::value, search_name).await(
        on_arg_match >> [&](error& e)
        {
          abort = std::move(e);
          quit(exit::error);
        },
        [&](actor const& srch)
        {
          self->sync_send(srch, *expr, opts, exporter_).await(
            on_arg_match >> [&](error& e)
            {
              abort = std::move(e);
              quit(exit::error);
            },
            [&](expression const& ast, actor const& qry)
            {
              VAST_DEBUG(this, "instantiated query", qry, "for:", ast);
              exporter_->link_to(qry);
              send(qry, extract_atom::value, limit);
            });
        });
      if (abort)
        return std::move(*abort);
    }
    else if (config_.check("console"))
    {
#ifdef VAST_HAVE_EDITLINE
      self->sync_send(tracker_, get_atom::value, search_name).await(
          [&](actor const& search)
          {
            auto c = spawn<console, linked>(search, dir / "console");
            delayed_send(c, std::chrono::milliseconds(200), prompt_atom::value);
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

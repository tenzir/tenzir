#include <csignal>
#include <cstdlib>

#include <algorithm>
#include <iostream>
#include <type_traits>

#include <caf/all.hpp>
#include <caf/io/all.hpp>

#include "vast/config.h"
#include "vast/logger.h"
#include "vast/time.h"
#include "vast/expression.h"
#include "vast/query_options.h"
#include "vast/actor/accountant.h"
#include "vast/actor/archive.h"
#include "vast/actor/exporter.h"
#include "vast/actor/identifier.h"
#include "vast/actor/importer.h"
#include "vast/actor/index.h"
#include "vast/actor/node.h"
#include "vast/actor/profiler.h"
#include "vast/actor/sink/ascii.h"
#include "vast/actor/sink/bro.h"
#include "vast/actor/sink/json.h"
#include "vast/actor/source/bro.h"
#include "vast/actor/source/bgpdump.h"
#include "vast/actor/source/test.h"
#include "vast/expr/normalize.h"
#include "vast/io/file_stream.h"
#include "vast/util/endpoint.h"
#include "vast/util/posix.h"
#include "vast/util/string.h"

#ifdef VAST_HAVE_PCAP
#include "vast/actor/sink/pcap.h"
#include "vast/actor/source/pcap.h"
#endif

using namespace caf;
using namespace std::string_literals;

namespace vast {

path const& node::log_path()
{
  static auto const ts = std::to_string(time::now().time_since_epoch().seconds());
  static auto const pid = std::to_string(util::process_id());
  static auto const dir = path{"log"} / (ts + '_' + pid);
  return dir;
}

node::node(std::string const& name, path const& dir)
  : default_actor{"node"},
    name_{name},
    dir_{dir}
{
}

void node::on_exit()
{
  store_ = invalid_actor;
  accountant_ = invalid_actor;
}

behavior node::make_behavior()
{
  auto exists_actor = [=](std::string const& str)
  {
    auto s = util::split_to_str(str, "@");
    auto& name = s.size() == 1 ? name_ : s[1];
    auto key = "actors/" + name + '/' + s[0];
    bool result = false;
    scoped_actor{}->sync_send(store_, exists_atom::value, key).await(
      [&](bool b) { result = b; }
    );
    return result;
  };
  auto get_actor = [=](std::string const& str)
  {
    auto s = util::split_to_str(str, "@");
    auto& name = s.size() == 1 ? name_ : s[1];
    std::tuple<std::string, actor, std::string> result;
    auto key = "actors/" + name + '/' + s[0];
    auto fqn = s.size() == 1 ? (s[0] + '@' + name_) : str;
    scoped_actor{}->sync_send(store_, get_atom::value, key).await(
      [&](actor const& a, std::string const& s) { result = {fqn, a, s}; },
      [](none) { /* nop */ }
    );
    return result;
  };
  auto put_actor = [=](actor const& a, std::string const& type,
                       std::string const& label)
  {
    auto key = "actors/" + name_ + "/" + label;
    auto result = make_message(ok_atom::value);
    scoped_actor{}->sync_send(store_, put_atom::value, key, a, type).await(
      [&](ok_atom)
      {
        a->attach_functor(
          [=](uint32_t) { anon_send(store_, delete_atom::value, key); }
        );
      },
      [&](error& e)
      {
        send_exit(a, exit::error);
        result = make_message(std::move(e));
      }
    );
    return result;
  };
  auto has_topology_entry = [=](std::string const& src, std::string const& snk)
  {
    bool result = false;
    scoped_actor self;
    self->sync_send(store_, list_atom::value, "topology/" + src).await(
      [&](std::map<std::string, message> const& vals)
      {
        auto pred = [&](auto& p)
        {
          return util::split_to_str(p.first, "/").back() == snk;
        };
        result = std::find_if(vals.begin(), vals.end(), pred) != vals.end();
      }
    );
    return result;
  };
  accountant_ = spawn<accountant<uint64_t>, linked>(dir_ / log_path());
  store_ = spawn<key_value_store, linked>();
  scoped_actor self;
  self->sync_send(store_, put_atom::value, "nodes/" + name_, this).await(
    [&](ok_atom) { /* nop */ },
    [&](error const& e)
    {
      VAST_ERROR(e);
      quit(exit::error);
    }
  );
  return
  {
    on("stop") >> [=]
    {
      VAST_VERBOSE(this, "stops");
      quit(exit::stop);
      return ok_atom::value;
    },
    on("peer", arg_match) >> [=](std::string const& endpoint)
    {
      VAST_VERBOSE(this, "peers with", endpoint);
      auto host = "127.0.0.1"s;
      auto port = uint16_t{42000};
      if (! util::parse_endpoint(endpoint, host, port))
        return make_message(error{"invalid endpoint: ", endpoint});
      try
      {
        VAST_DEBUG(this, "connects to", host << ':' << port);
        auto peer = caf::io::remote_actor(host.c_str(), port);
        auto result = make_message(ok_atom::value);
        scoped_actor self;
        self->sync_send(peer, sys_atom::value, this, store_, name_).await(
          [&](ok_atom, std::string const& peer_name)
          {
            VAST_INFO(this, "now peers with:", peer_name);
            peer->attach_functor(
              [=](uint32_t)
              {
                auto key1 = "actors/" + name_ + '/' + peer_name;
                auto key2 = "actors/" + peer_name + '/' + name_;
                anon_send(store_, delete_atom::value, key1);
                anon_send(store_, delete_atom::value, key2);
              }
            );
          },
          [&](error& e)
          {
            result = make_message(std::move(e));
          }
        );
        return result;
      }
      catch (caf::network_error const& e)
      {
        return make_message(error{"failed to connect to ", host, ':', port});
      }
    },
    on("spawn", any_vals) >> [=]
    {
      auto syntax = "spawn [arguments] <actor> [params]";
      auto msg = current_message().drop(1);
      if (msg.empty())
        return make_message(error{"missing actor: ", syntax});
      std::vector<std::string> actors = {
        "archive",
        "exporter",
        "identifier",
        "importer",
        "index",
        "profiler",
        "sink",
        "source"
      };
      // Convert arguments to string vector.
      std::vector<std::string> args(msg.size());
      for (auto i = 0u; i < msg.size(); ++i)
        args[i] = msg.get_as<std::string>(i);
      auto a = std::find_first_of(args.begin(), args.end(),
                                  actors.begin(), actors.end());
      if (a == args.end())
        return make_message(error{"invalid actor: ", syntax});
      // Extract spawn arguments.
      auto label = *a;
      auto r = message_builder{args.begin(), a}.extract_opts({
        {"label,l", "a unique label of the actor within this node", label},
      });
      if (! r.error.empty())
        return make_message(error{std::move(r.error)});
      VAST_VERBOSE(this, "attempts to spawn actor", *a, '(' << label << ')');
      if (exists_actor(label))
        return make_message(error{"actor already exists: ", label});
      auto component = msg.drop(a - args.begin());
      auto params = component.drop(1);
      auto result = component.apply({
        on("identifier") >> [&]
        {
          auto i = spawn<identifier>(dir_);
          attach_functor([=](uint32_t ec) { anon_send_exit(i, ec); });
          return put_actor(i, "identifier", "identifier");
        },
        on("archive", any_vals) >> [&]
        {
          uint64_t segments = 10;
          uint64_t size = 128;
          r = params.extract_opts({
            {"segments,n", "maximum number of cached segments", segments},
            {"size,s", "maximum size of segment before flushing (MB)", size}
          });
          if (! r.error.empty())
            return make_message(error{std::move(r.error)});
          size <<= 20; // MB'ify
          auto dir = dir_ / "archive";
          auto a = spawn<archive, priority_aware>(dir, segments, size);
          attach_functor([=](uint32_t ec) { anon_send_exit(a, ec); });
          send(a, put_atom::value, accountant_atom::value, accountant_);
          return put_actor(a, "archive", label);
        },
        on("index", any_vals) >> [&]
        {
          uint64_t events = 1 << 20;
          uint64_t passive = 10;
          uint64_t active = 5;
          r = params.extract_opts({
            {"events,e", "maximum events per partition", events},
            {"active,a", "maximum active partitions", active},
            {"passive,p", "maximum passive partitions", passive}
          });
          if (! r.error.empty())
            return make_message(error{std::move(r.error)});
          auto dir = dir_ / "index";
          auto i = spawn<index, priority_aware>(dir, events, passive, active);
          attach_functor([=](uint32_t ec) { anon_send_exit(i, ec); });
          send(i, put_atom::value, accountant_atom::value, accountant_);
          return put_actor(i, "index", label.empty() ? "index" : label);
        },
        on("importer") >> [&]
        {
          auto i = spawn<importer, priority_aware>();
          attach_functor([=](uint32_t ec) { anon_send_exit(i, ec); });
          //send(i, put_atom::value, accountant_atom::value, accountant_);
          return put_actor(i, "importer", label.empty() ? "importer" : label);
        },
        on("exporter", any_vals) >> [&]
        {
          auto limit = uint64_t{0};
          r = params.extract_opts({
            {"continuous,c", "marks a query as continuous"},
            {"historical,h", "marks a query as historical"},
            {"unified,u", "marks a query as unified"},
            {"limit,l", "seconds between measurements", limit}
          });
          if (! r.error.empty())
            return make_message(error{std::move(r.error)});
          // Join remainder into single string.
          std::string str;
          for (auto i = 0u; i < r.remainder.size(); ++i)
          {
            if (i != 0)
              str += ' ';
            str += r.remainder.get_as<std::string>(i);
          }
          VAST_VERBOSE(this, "got query:", str);
          auto query_opts = no_query_options;
          if (r.opts.count("continuous") > 0)
            query_opts = query_opts + continuous;
          if (r.opts.count("historical") > 0)
            query_opts = query_opts + historical;
          if (r.opts.count("unified") > 0)
            query_opts = unified;
          if (query_opts == no_query_options)
          {
            VAST_ERROR(this, "got query without options");
            return make_message(error{"no query options specified"});
          }
          VAST_DEBUG(this, "parses expression");
          auto expr = to<expression>(str);
          if (! expr)
          {
            VAST_VERBOSE(this, "ignores invalid query:", str);
            return make_message(expr.error());
          }
          *expr = expr::normalize(*expr);
          VAST_VERBOSE(this, "normalized query to", *expr);
          auto exp = spawn<exporter>(*expr, query_opts);
          attach_functor([=](uint32_t ec) { anon_send_exit(exp, ec); });
          if (r.opts.count("limit") > 0)
            send(exp, limit_atom::value, limit);
          return put_actor(exp, "exporter", label);
        },
        on("source", any_vals) >> [&]
        {
          auto batch_size = uint64_t{100000};
          auto comp = "lz4"s;
          auto schema_file = ""s;
          auto input = ""s;
          r = params.extract_opts({
            {"batch,b", "number of events to ingest at once", batch_size},
            {"compression,c", "compression method for event batches", comp},
            {"schema,s", "alternate schema file", schema_file},
            {"dump-schema,d", "print schema and exit"},
            {"read,r", "path to read events from", input},
            {"uds,u", "treat -r as UNIX domain socket to connect to"}
          });
          if (! r.error.empty())
            return make_message(error{std::move(r.error)});
          auto& format = params.get_as<std::string>(0);
          // The "pcap" and "test" sources manually verify the presence of
          // input. All other sources are file-based and we setup their input
          // stream here.
          std::unique_ptr<io::input_stream> in;
          if (! (format == "pcap" || format == "test"))
          {
            if (r.opts.count("read") == 0 || input.empty())
            {
              VAST_ERROR(this, "didn't specify valid input (-r)");
              return make_message(error{"no valid input specified (-r)"});
            }
            if (r.opts.count("uds") > 0)
            {
              auto uds = util::unix_domain_socket::connect(input);
              if (! uds)
              {
                auto err = "failed to connect to UNIX domain socket at ";
                VAST_ERROR(this, err << input);
                return make_message(error{err, input});
              }
              auto remote_fd = uds.recv_fd(); // Blocks!
              in = std::make_unique<io::file_input_stream>(remote_fd);
            }
            else
            {
              in = std::make_unique<io::file_input_stream>(input);
            }
          }
          auto dump_schema = r.opts.count("dump-schema") > 0;
          // Facilitate actor shutdown when returning with error.
          bool terminate = true;
          actor src;
          struct terminator
          {
            terminator(std::function<void()> f) : f(f) { }
            ~terminator() { f(); }
            std::function<void()> f;
          };
          terminator guard{[&] { if (terminate) send_exit(src, exit::error); }};
          // Spawn a source according to format.
          if (format == "pcap")
          {
#ifndef VAST_HAVE_PCAP
            return make_message(error{"not compiled with pcap support"});
#else
            auto flow_max = uint64_t{1} << 20;
            auto flow_age = 60u;
            auto flow_expiry = 10u;
            auto cutoff = std::numeric_limits<size_t>::max();
            auto pseudo_realtime = int64_t{0};
            r = r.remainder.extract_opts({ // -i overrides -r
              {"interface,i", "the interface to read packets from", input},
              {"cutoff,c", "skip flow packets after this many bytes", cutoff},
              {"flow-max,m", "number of concurrent flows to track", flow_max},
              {"flow-age,a", "max flow lifetime before eviction", flow_age},
              {"flow-expiry,e", "flow table expiration interval", flow_expiry},
              {"pseudo-realtime,p", "factor c delaying trace packets by 1/c",
                                    pseudo_realtime}
            });
            if (! r.error.empty())
              return make_message(error{std::move(r.error)});
            if (input.empty())
            {
              VAST_ERROR(this, "didn't specify input (-r or -i)");
              return make_message(error{"no input specified (-r or -i)"});
            }
            src = spawn<source::pcap, priority_aware + detached>(
              input, cutoff, flow_max, flow_age, flow_expiry, pseudo_realtime);
#endif
          }
          else if (format == "test")
          {
            auto id = event_id{0};
            auto events = uint64_t{100};
            r = r.remainder.extract_opts({
              {"id,i", "the base event ID", id},
              {"events,n", "number of events to generate", events}
            });
            if (! r.error.empty())
              return make_message(error{std::move(r.error)});
            src = spawn<source::test, priority_aware>(id, events);
          }
          else if (format == "bro")
          {
            src = spawn<source::bro, priority_aware + detached>(
              std::move(in));
          }
          else if (format == "bgpdump")
          {
            src = spawn<source::bgpdump, priority_aware + detached>(
              std::move(in));
          }
          else
          {
            return make_message(error{"invalid import format: ", format});
          }
          attach_functor([=](uint32_t ec) { anon_send_exit(src, ec); });
          // Set a new schema if provided.
          if (! schema_file.empty())
          {
            auto t = load_and_parse<schema>(path{schema_file});
            if (! t)
              return make_message(error{"failed to load schema: ", t.error()});
            send(src, put_atom::value, *t);
          }
          // Dump the schema.
          if (dump_schema)
          {
            auto res = make_message(ok_atom::value);
            scoped_actor self;
            self->sync_send(src, get_atom::value, schema_atom::value).await(
              [&](schema const& sch) { res = make_message(to_string(sch)); }
            );
            return res;
          };
          // Set parameters.
          send(src, batch_atom::value, batch_size);
          send(src, put_atom::value, accountant_atom::value, accountant_);
          if (comp == "null")
            send(src, io::null);
          else if (comp == "lz4")
            send(src, io::lz4);
          else if (comp == "snappy")
#ifdef VAST_HAVE_SNAPPY
            send(src, io::snappy);
#else
            return make_message(error{"not compiled with snappy support"});
#endif
          else
            return make_message(error{"unknown compression method: ", comp});
          // Save it.
          terminate = false;
          return put_actor(src, "source", label);
        },
        on("sink", any_vals) >> [&]
        {
          auto schema_file = ""s;
          auto output = ""s;
          r = params.extract_opts({
            {"schema,s", "alternate schema file", schema_file},
            {"write,w", "path to write events to", output},
            {"uds,u", "treat -w as UNIX domain socket to connect to"}
          });
          if (! r.error.empty())
            return make_message(error{std::move(r.error)});
          if (r.opts.count("write") == 0)
          {
            VAST_ERROR(this, "didn't specify output (-w)");
            return make_message(error{"no output specified (-w)"});
          }
          // Setup a custom schema.
          schema sch;
          if (! schema_file.empty())
          {
            auto t = load_and_parse<schema>(path{schema_file});
            if (! t)
            {
              VAST_ERROR(this, "failed to load schema", schema_file);
              return make_message(error{"failed to load schema: ", t.error()});
            }
            sch = std::move(*t);
          }
          // Facilitate actor shutdown when returning with error.
          actor snk;
          bool terminate = true;
          struct terminator
          {
            terminator(std::function<void()> f) : f(f) { }
            ~terminator() { f(); }
            std::function<void()> f;
          };
          terminator guard{[&] { if (terminate) send_exit(snk, exit::error); }};
          auto& format = params.get_as<std::string>(0);
          // The "pcap" and "bro" sink manually handle file output. All other
          // sources are file-based and we setup their input stream here.
          std::unique_ptr<io::output_stream> out;
          if (! (format == "pcap" || format == "bro"))
          {
            if (r.opts.count("uds") > 0)
            {
              auto uds = util::unix_domain_socket::connect(output);
              if (! uds)
              {
                auto err = "failed to connect to UNIX domain socket at ";
                VAST_ERROR(this, err << output);
                return make_message(error{err, output});
              }
              auto remote_fd = uds.recv_fd(); // Blocks!
              out = std::make_unique<io::file_output_stream>(remote_fd);
            }
          }
          else
          {
            out = std::make_unique<io::file_output_stream>(output);
          }
          if (format == "pcap")
          {
#ifndef VAST_HAVE_PCAP
            return make_message(error{"not compiled with pcap support"});
#else
            auto flush = 10000u;
            r = r.remainder.extract_opts({
              {"flush,f", "flush to disk after this many packets", flush}
            });
            if (! r.error.empty())
              return make_message(error{std::move(r.error)});
            snk = spawn<sink::pcap, priority_aware>(sch, output, flush);
#endif
          }
          else if (format == "bro")
          {
            snk = spawn<sink::bro>(output);
          }
          else if (format == "ascii")
          {
            snk = spawn<sink::ascii>(std::move(out));
          }
          else if (format == "json")
          {
            snk = spawn<sink::json>(std::move(out));
          }
          else
          {
            return make_message(error{"invalid export format: ", format});
          }
          attach_functor([=](uint32_t ec) { anon_send_exit(snk, ec); });
          terminate = false;
          return put_actor(snk, "sink", label);
        },
        on("profiler", any_vals) >> [&]
        {
          auto resolution = 0u;
          r = params.extract_opts({
            {"cpu,c", "start the CPU profiler"},
            {"heap,h", "start the heap profiler"},
            {"resolution,r", "seconds between measurements", resolution}
          });
          if (! r.error.empty())
            return make_message(error{std::move(r.error)});
          auto secs = std::chrono::seconds(resolution);
          auto prof = spawn<profiler, detached>(dir_ / log_path(), secs);
          attach_functor([=](uint32_t ec) { anon_send_exit(prof, ec); });
          if (r.opts.count("cpu") > 0)
          {
#ifdef VAST_USE_PERFTOOLS_CPU_PROFILER
            send(prof, start_atom::value, cpu_atom::value);
#else
            send_exit(prof, exit::error);
            return make_message(error{"not compiled with perftools CPU"});
#endif
          }
          if (r.opts.count("heap") > 0)
          {
#ifdef VAST_USE_PERFTOOLS_HEAP_PROFILER
            send(prof, start_atom::value, heap_atom::value);
#else
            send_exit(prof, exit::error);
            return make_message(error{"not compiled with perftools heap"});
#endif
          }
          return put_actor(prof, "profiler", "profiler");
        },
        others() >> []
        {
          return error{"not yet implemented"};
        }
      });
      assert(result);
      return std::move(*result);
    },
    on("send", val<std::string>, "run")
      >> [=](std::string const& arg, std::string const& /* run */)
    {
      VAST_VERBOSE(this, "sends RUN to", arg);
      auto state = get_actor(arg);
      auto& a = std::get<1>(state);
      auto& type = std::get<2>(state);
      if (! a)
        return make_message(error{"no such actor: ", arg});
      send(a, run_atom::value);
      if (type == "exporter")
        // FIXME: Because we've previously configured a limit, the extraction
        // will finish when hitting it. But this is not a good design, as it
        // prevents pull-based extraction of results. Once the API becomes
        // clearer, we need a better way for incremental extraction.
        send(a, extract_atom::value, uint64_t{0});
      return make_message(ok_atom::value);
    },
    on("send", val<std::string>, "flush")
      >> [=](std::string const& arg, std::string const& /* flush */)
    {
      VAST_VERBOSE(this, "sends FLUSH to", arg);
      auto state = get_actor(arg);
      auto& a = std::get<1>(state);
      if (! a)
        return make_message(error{"no such actor: ", arg});
      send(a, flush_atom::value);
      return make_message(ok_atom::value);
    },
    on("quit", arg_match) >> [=](std::string const& arg)
    {
      VAST_VERBOSE(this, "terminates actor", arg);
      auto state = get_actor(arg);
      auto& a = std::get<1>(state);
      if (! a)
        return make_message(error{"no such actor: ", arg});
      send_exit(a, exit::stop);
      return make_message(ok_atom::value);
    },
    on("connect", arg_match)
      >> [=](std::string const& source, std::string const& sink)
    {
      VAST_VERBOSE(this, "connects actors:", source, "->", sink);
      // Retrieve source and sink information.
      auto src = get_actor(source);
      auto snk = get_actor(sink);
      auto& src_label = std::get<0>(src);
      auto& snk_label = std::get<0>(snk);
      auto& src_actor = std::get<1>(src);
      auto& snk_actor = std::get<1>(snk);
      auto& src_type = std::get<2>(src);
      auto& snk_type = std::get<2>(snk);
      if (src_label.empty())
        return make_message(error{"no such source: ", source});
      if (snk_label.empty())
        return make_message(error{"no such sink: ", sink});
      if (has_topology_entry(src_label, snk_label))
        return make_message(
            error{"connection already exists: ", source, " -> ", sink});
      // Wire actors based on their type.
      message msg;
      if (src_type == "source")
      {
        if (snk_type == "importer")
          msg = make_message(put_atom::value, sink_atom::value, snk_actor);
        else
          return make_message(error{"sink not an importer: ", sink});
      }
      else if (src_type == "importer")
      {
        if (snk_type == "identifier")
          msg = make_message(put_atom::value, identifier_atom::value, snk_actor);
        else if (snk_type == "archive")
          msg = make_message(put_atom::value, archive_atom::value, snk_actor);
        else if (snk_type == "index")
          msg = make_message(put_atom::value, index_atom::value, snk_actor);
        else
          return make_message(error{"invalid importer sink: ", sink});
      }
      else if (src_type == "exporter")
      {
        if (snk_type == "archive")
          msg = make_message(put_atom::value, archive_atom::value, snk_actor);
        else if (snk_type == "index")
          msg = make_message(put_atom::value, index_atom::value, snk_actor);
        else if (snk_type == "sink")
          msg = make_message(put_atom::value, sink_atom::value, snk_actor);
        else
          return make_message(error{"invalid exporter sink: ", sink});
      }
      else
      {
        return make_message(error{"invalid source: ", source});
      }
      send(src_actor, msg);
      // Create new topology entry in the store.
      auto key = "topology/" + src_label + '/' + snk_label;
      scoped_actor self;
      self->sync_send(store_, put_atom::value, key).await([](ok_atom) {});
      auto del = [=](uint32_t) { anon_send(store_, delete_atom::value, key); };
      src_actor->attach_functor(del);
      snk_actor->attach_functor(del);
      return make_message(ok_atom::value);
    },
    on("disconnect", arg_match)
      >> [=](std::string const& source, std::string const& sink)
    {
      VAST_VERBOSE(this, "disconnects actors:", source, "->", sink);
      auto src = get_actor(source);
      auto snk = get_actor(sink);
      auto& src_label = std::get<0>(src);
      auto& snk_label = std::get<0>(snk);
      if (has_topology_entry(src_label, snk_label))
        return make_message(
            error{"connection already exists: ", source, " -> ", sink});
      // TODO: send message that performs actual diconnection.
      scoped_actor self;
      auto key = "topology/" + src_label + '/' + snk_label;
      self->sync_send(store_, delete_atom::value, key).await(
        [](uint64_t n) { assert(n == 1); }
      );
      return make_message(ok_atom::value);
    },
    on("show", arg_match) >> [=](std::string const& arg)
    {
      VAST_VERBOSE(this, "got request to show", arg);
      std::string result;
      scoped_actor self;
      std::string key;
      if (arg == "nodes")
        key = "nodes/";
      else if (arg == "peers")
        key = "peers/" + name_;
      else if (arg == "actors")
        key = "actors/" + name_;
      else if (arg == "topology")
        key = "topology/";
      else
        return make_message(error{"show: invalid argument"});
      self->sync_send(store_, list_atom::value, key).await(
        [&](std::map<std::string, message> const& values)
        {
          auto pred = [](auto& p) -> std::string const& { return p.first; };
          result = util::join(values.begin(), values.end(), "\n", pred);
        }
      );
      return make_message(std::move(result));
    },
    //
    // PRIVATE
    //
    [=](sys_atom, actor const& peer, actor const& peer_store,
        std::string const& peer_name)
    {
      auto task = spawn(
        [=](event_based_actor* self, actor parent) -> behavior
        {
          return
          {
            others() >> [=]
            {
              auto rp = self->make_response_promise();
              auto abort_on_error = [=](error e)
              {
                rp.deliver(make_message(std::move(e)));
                self->quit(exit::error);
              };
              if (peer_name == name_)
              {
                VAST_WARN(this, "ignores new peer with duplicate name");
                abort_on_error(error{"duplicate peer name"});
                return;
              }
              VAST_INFO(this, "got new peer:", peer_name);
              auto key1 = "peers/" + name_ + '/' + peer_name;
              auto key2 = "peers/" + peer_name + '/' + name_;
              self->send(store_, put_atom::value, key1, peer);
              self->send(store_, put_atom::value, key2, parent);
              self->become(
                [=](ok_atom)
                {
                  self->become(
                    [=](ok_atom)
                    {
                      self->send(store_, peer_atom::value, peer_store);
                      self->become(
                        [=](ok_atom)
                        {
                          peer->attach_functor(
                            [=](uint32_t)
                            {
                              anon_send(store_, delete_atom::value, key1);
                              anon_send(store_, delete_atom::value, key2);
                            }
                          );
                          rp.deliver(make_message(ok_atom::value, name_));
                          self->quit();
                        },
                        abort_on_error
                      );
                    },
                    abort_on_error
                  );
                },
                abort_on_error
              );
            }
          };
        },
        this
      );
      forward_to(task);
    },
    others() >> [=]
    {
      std::string cmd;
      current_message().extract([&](std::string const& s) { cmd += ' ' + s; });
      VAST_ERROR("invalid command syntax:" << cmd);
      return error{"invalid command syntax:", cmd};
    }
  };
}

} // namespace vast

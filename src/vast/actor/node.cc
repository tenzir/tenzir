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
#include "vast/actor/identifier.h"
#include "vast/actor/importer.h"
#include "vast/actor/index.h"
#include "vast/actor/exporter.h"
#include "vast/actor/http_broker.h"
#include "vast/actor/node.h"
#include "vast/expr/normalize.h"
#include "vast/io/compression.h"
#include "vast/io/file_stream.h"
#include "vast/util/assert.h"
#include "vast/util/endpoint.h"
#include "vast/util/string.h"

#ifdef VAST_HAVE_GPERFTOOLS
#include "vast/actor/profiler.h"
#endif

using namespace caf;
using namespace caf::io;
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
  accountant_ = spawn<accountant<uint64_t>, linked>(dir_ / log_path());
  store_ = spawn<key_value_store, linked>();
  // We always begin with registering ourselves in the key value store. We
  // don't have to check for conflicting names until we peer with another node.
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
    //
    // PUBLIC
    //
    on("stop") >> [=]
    {
      return stop();
    },
    on("peer", arg_match) >> [=](std::string const& e)
    {
      return request_peering(e);
    },
    on("spawn", any_vals) >> [=]
    {
      return spawn_actor(current_message().drop(1));
    },
    on("send", val<std::string>, "run")
      >> [=](std::string const& arg, std::string const& /* run */)
    {
      return send_run(arg);
    },
    on("send", val<std::string>, "flush")
      >> [=](std::string const& arg, std::string const& /* flush */)
    {
      send_flush(arg);
    },
    on("quit", arg_match) >> [=](std::string const& arg)
    {
      return quit_actor(arg);
    },
    on("connect", arg_match)
      >> [=](std::string const& source, std::string const& sink)
    {
      return connect(source, sink);
    },
    on("disconnect", arg_match)
      >> [=](std::string const& source, std::string const& sink)
    {
      return disconnect(source, sink);
    },
    on("show", arg_match) >> [=](std::string const& arg)
    {
      return show(arg);
    },
    [=](get_atom, std::string const& label)
    {
      auto s = get(label);
      return make_message(std::move(s.actor), std::move(s.fqn), std::move(s.type));
    },
    //
    // PRIVATE
    //
    [=](sys_atom, actor const& peer, actor const& peer_store,
        std::string const& peer_name)
    {
      // Respond to peering request.
      auto job = spawn(
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
      forward_to(job);
    },
    others() >> [=]
    {
      std::string cmd;
      current_message().extract([&](std::string const& s) { cmd += ' ' + s; });
      if (cmd.empty())
        cmd = to_string(current_message());
      VAST_ERROR("invalid command syntax:" << cmd);
      return error{"invalid command syntax:", cmd};
    }
  };
}

message node::stop()
{
  VAST_VERBOSE(this, "stops");
  quit(exit::stop);
  return make_message(ok_atom::value);
}

message node::request_peering(std::string const& endpoint)
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
}

message node::spawn_actor(message const& msg)
{
  auto syntax = "spawn [arguments] <actor> [params]";
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
    "source",
    "http_broker",
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
  // Check if actor exists already.
  auto s = util::split_to_str(label, "@");
  auto& name = s.size() == 1 ? name_ : s[1];
  auto key = "actors/" + name + '/' + s[0];
  bool actor_exists = false;
  scoped_actor{}->sync_send(store_, exists_atom::value, key).await(
    [&](bool b) { actor_exists = b; }
  );
  if (actor_exists)
  {
    VAST_ERROR(this, "aborts spawn: actor", label, "exists already");
    return make_message(error{"actor already exists: ", label});
  }
  VAST_VERBOSE(this, "attempts to spawn actor", *a, '(' << label << ')');
  auto component = msg.drop(a - args.begin());
  auto params = component.drop(1);
  auto result = component.apply({
    on("identifier") >> [&]
    {
      auto i = spawn<identifier>(dir_);
      attach_functor([=](uint32_t ec) { anon_send_exit(i, ec); });
      return put({i, "identifier", "identifier"});
    },
    on("archive", any_vals) >> [&]
    {
      io::compression method;
      auto comp = "lz4"s;
      uint64_t segments = 10;
      uint64_t size = 128;
      r = params.extract_opts({
        {"compression,c", "compression method for event batches", comp},
        {"segments,s", "maximum number of cached segments", segments},
        {"size,m", "maximum size of segment before flushing (MB)", size}
      });
      if (! r.error.empty())
        return make_message(error{std::move(r.error)});
      if (comp == "null")
        method = io::null;
      else if (comp == "lz4")
        method = io::lz4;
      else if (comp == "snappy")
#ifdef VAST_HAVE_SNAPPY
        method = io::snappy;
#else
        return make_message(error{"not compiled with snappy support"});
#endif
      else
        return make_message(error{"unknown compression method: ", comp});
      size <<= 20; // MB'ify
      auto dir = dir_ / "archive";
      auto a = spawn<archive, priority_aware>(dir, segments, size, method);
      attach_functor([=](uint32_t ec) { anon_send_exit(a, ec); });
      send(a, put_atom::value, accountant_atom::value, accountant_);
      return put({a, "archive", label});
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
      return put({i, "index", label.empty() ? "index" : label});
    },
    on("importer") >> [&]
    {
      auto i = spawn<importer, priority_aware>();
      attach_functor([=](uint32_t ec) { anon_send_exit(i, ec); });
      //send(i, put_atom::value, accountant_atom::value, accountant_);
      return put({i, "importer", label.empty() ? "importer" : label});
    },
    on("exporter", any_vals) >> [&]
    {
      auto limit = uint64_t{100};
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
      auto expr = detail::to_expression(str);
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
      return put({exp, "exporter", label});
    },
    on("source", any_vals) >> [&]
    {
      // Outsourced to reduce compiler memory footprint.
      return spawn_source(label, params);
    },
    on("sink", any_vals) >> [&]
    {
      // Outsourced to reduce compiler memory footprint.
      return spawn_sink(label, params);
    },
    on("profiler", any_vals) >> [&]
    {
#ifdef VAST_HAVE_GPERFTOOLS
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
        send(prof, start_atom::value, "cpu");
      if (r.opts.count("heap") > 0)
        send(prof, start_atom::value, "heap");
      return put({prof, "profiler", "profiler"});
#else
      return error{"not compiled with gperftools"};
#endif
    },
    on("http_broker", any_vals) >> [&]
    {
      auto port = uint16_t{8888};
      r = params.extract_opts({
        {"port,p", "the port to listen on", port}
      });
      if (! r.error.empty())
        return make_message(error{std::move(r.error)});
      // FIXME: fails :-(
      auto broker = spawn_io_server(http_broker_function, port, this);
      VAST_DEBUG(this, "spawned HTTP broker");
      attach_functor([=](uint32_t ec) { anon_send_exit(broker, ec); });
      return put({broker, "http_broker", "http_broker"});
    },
    others() >> []
    {
      return error{"not yet implemented"};
    }
  });
  VAST_ASSERT(result);
  return std::move(*result);
}

message node::send_run(std::string const& arg)
{
  VAST_VERBOSE(this, "sends RUN to", arg);
  auto state = get(arg);
  if (! state.actor)
    return make_message(error{"no such actor: ", arg});
  send(state.actor, run_atom::value);
  if (state.type == "exporter")
    // FIXME: Because we've previously configured a limit, the extraction
    // will finish when hitting it. But this is not a good design, as it
    // prevents pull-based extraction of results. Once the API becomes
    // clearer, we need a better way for incremental extraction.
    send(state.actor, extract_atom::value, uint64_t{0});
  return make_message(ok_atom::value);
}

void node::send_flush(std::string const& arg)
{
  VAST_VERBOSE(this, "sends FLUSH to", arg);
  auto rp = make_response_promise();
  auto state = get(arg);
  if (! state.actor)
  {
    rp.deliver(make_message(error{"no such actor: ", arg}));
    return;
  }
  if (! (state.type == "index" || state.type == "archive"))
  {
    rp.deliver(make_message(error{state.type, " does not support flushing"}));
    return;
  };
  auto job = spawn(
    [=](event_based_actor* self, actor target) -> behavior
    {
      return
      {
        others() >> [=]
        {
          self->send(target, flush_atom::value);
          self->become(
            [=](actor const& task)
            {
              self->monitor(task);
              self->become(
                [=](down_msg const& msg)
                {
                  VAST_ASSERT(msg.source == task);
                  rp.deliver(make_message(ok_atom::value));
                  self->quit(exit::done);
                }
              );
            },
            [=](ok_atom)
            {
              rp.deliver(self->current_message());
              self->quit(exit::done);
            },
            [=](error const&)
            {
              rp.deliver(self->current_message());
              self->quit(exit::error);
            },
            others() >> [=]
            {
              rp.deliver(make_message(error{"unexpected response to FLUSH"}));
              self->quit(exit::error);
            },
            after(time::seconds(10)) >> [=]
            {
              rp.deliver(make_message(error{"timed out"}));
              self->quit(exit::error);
            }
          );
        }
      };
    },
    state.actor
  );
  forward_to(job);
}

message node::quit_actor(std::string const& arg)
{
  VAST_VERBOSE(this, "terminates actor", arg);
  auto state = get(arg);
  if (! state.actor)
    return make_message(error{"no such actor: ", arg});
  send_exit(state.actor, exit::stop);
  return make_message(ok_atom::value);
}

message node::connect(std::string const& sources, std::string const& sinks)
{
  for (auto& source : util::split_to_str(sources, ","))
    for (auto& sink : util::split_to_str(sinks, ","))
    {
      VAST_VERBOSE(this, "connects actors:", source, "->", sink);
      // Retrieve source and sink information.
      auto src = get(source);
      auto snk = get(sink);
      if (src.actor == invalid_actor)
        return make_message(error{"no such source: ", source});
      if (snk.actor == invalid_actor)
        return make_message(error{"no such sink: ", sink});
      if (has_topology_entry(src.fqn, snk.fqn))
        return make_message(
            error{"connection already exists: ", source, " -> ", sink});
      // Wire actors based on their type.
      message msg;
      if (src.type == "source")
      {
        if (snk.type == "importer")
          msg = make_message(put_atom::value, sink_atom::value, snk.actor);
        else
          return make_message(error{"sink not an importer: ", sink});
      }
      else if (src.type == "importer")
      {
        if (snk.type == "identifier")
          msg = make_message(put_atom::value, identifier_atom::value,
                             snk.actor);
        else if (snk.type == "archive")
          msg = make_message(put_atom::value, archive_atom::value, snk.actor);
        else if (snk.type == "index")
          msg = make_message(put_atom::value, index_atom::value, snk.actor);
        else
          return make_message(error{"invalid importer sink: ", sink});
      }
      else if (src.type == "exporter")
      {
        if (snk.type == "archive")
          msg = make_message(put_atom::value, archive_atom::value, snk.actor);
        else if (snk.type == "index")
          msg = make_message(put_atom::value, index_atom::value, snk.actor);
        else if (snk.type == "sink")
          msg = make_message(put_atom::value, sink_atom::value, snk.actor);
        else
          return make_message(error{"invalid exporter sink: ", sink});
      }
      else
      {
        return make_message(error{"invalid source: ", source});
      }
      send(src.actor, msg);
      // Create new topology entry in the store.
      auto key = "topology/" + src.fqn + '/' + snk.fqn;
      scoped_actor self;
      self->sync_send(store_, put_atom::value, key).await([](ok_atom) {});
      auto del = [=](uint32_t) { anon_send(store_, delete_atom::value, key); };
      src.actor->attach_functor(del);
      snk.actor->attach_functor(del);
    }
  return make_message(ok_atom::value);
}

message node::disconnect(std::string const& sources, std::string const& sinks)
{
  for (auto& source : util::split_to_str(sources, ","))
    for (auto& sink : util::split_to_str(sinks, ","))
    {
      VAST_VERBOSE(this, "disconnects actors:", source, "->", sink);
      auto src = get(source);
      auto snk = get(sink);
      if (has_topology_entry(src.fqn, snk.fqn))
        return make_message(
            error{"connection already exists: ", source, " -> ", sink});
      // TODO: send message that performs actual diconnection.
      scoped_actor self;
      auto key = "topology/" + src.fqn + '/' + snk.fqn;
      self->sync_send(store_, delete_atom::value, key).await(
        [](uint64_t n) { VAST_ASSERT(n == 1); }
      );
    }
  return make_message(ok_atom::value);
}

message node::show(std::string const& arg)
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
}

node::actor_state node::get(std::string const& label)
{
  auto s = util::split_to_str(label, "@");
  auto& name = s.size() == 1 ? name_ : s[1];
  actor_state result;
  auto key = "actors/" + name + '/' + s[0];
  auto fqn = s.size() == 1 ? (s[0] + '@' + name_) : label;
  scoped_actor{}->sync_send(store_, get_atom::value, key).await(
    [&](actor const& a, std::string const& s) { result = {a, fqn, s}; },
    [](none) { /* nop */ }
  );
  return result;
};

message node::put(actor_state const& state)
{
  auto key = "actors/" + name_ + "/" + state.fqn;
  auto result = make_message(ok_atom::value);
  scoped_actor{}->sync_send(store_, put_atom::value, key, state.actor,
                            state.type).await(
    [&](ok_atom)
    {
      state.actor->attach_functor(
        [=](uint32_t) { anon_send(store_, delete_atom::value, key); }
      );
    },
    [&](error& e)
    {
      send_exit(state.actor, exit::error);
      result = make_message(std::move(e));
    }
  );
  return result;
};

bool node::has_topology_entry(std::string const& src, std::string const& snk)
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

} // namespace vast

/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include "vast/system/node.hpp"

#include <csignal>

#include <chrono>
#include <fstream>
#include <sstream>

#include <caf/all.hpp>
#include <caf/io/all.hpp>

#include "vast/config.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/endpoint.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/json.hpp"
#include "vast/detail/assert.hpp"
#include "vast/json.hpp"
#include "vast/logger.hpp"
#include "vast/system/accountant.hpp"
#include "vast/system/consensus.hpp"
#include "vast/system/spawn_archive.hpp"
#include "vast/system/spawn_arguments.hpp"
#include "vast/system/spawn_exporter.hpp"
#include "vast/system/spawn_importer.hpp"
#include "vast/system/spawn_index.hpp"
#include "vast/system/spawn_metastore.hpp"
#include "vast/system/spawn_node.hpp"
#include "vast/system/spawn_or_connect_to_node.hpp"
#include "vast/system/spawn_profiler.hpp"
#include "vast/system/spawn_sink.hpp"
#include "vast/system/spawn_source.hpp"

using std::string;

using namespace caf;

namespace vast::system {

namespace {

// Local commands need access to the node actor.
thread_local node_actor* this_node;

// Convenience function for wrapping an error into a CAF message.
auto err(ec code, string msg) {
  return caf::make_message(make_error(code, std::move(msg)));
}

// Tries to establish peering to another node.
caf::message peer_command(const command&, caf::actor_system& sys,
                          caf::config_value_map& options,
                          command::argument_iterator first,
                          command::argument_iterator last) {
  VAST_ASSERT(this_node != nullptr);
  if (std::distance(first, last) != 1)
    return err(ec::syntax_error, "expected exactly one endpoint argument");
  auto ep = to<endpoint>(*first);
  if (!ep)
    return err(ec::parse_error, "invalid endpoint format");
  // Use localhost:42000 by default.
  if (ep->host.empty())
    ep->host = "127.0.0.1";
  if (ep->port == 0)
    ep->port = 42000;
  VAST_DEBUG(this_node, "connects to", ep->host << ':' << ep->port);
  auto& mm = sys.middleman();
  // TODO: this blocks the node, consider talking to the MM actor instead.
  auto peer = mm.remote_actor(ep->host.c_str(), ep->port);
  if (!peer) {
    VAST_ERROR(this_node, "failed to connect to peer:",
               sys.render(peer.error()));
    return caf::make_message(std::move(peer.error()));
  }
  VAST_DEBUG(this_node, "sends peering request");
  auto node_name = get_or(options, "node.name", "node");
  auto& st = this_node->state;
  this_node->delegate(*peer, peer_atom::value, st.tracker, st.name);
  return caf::none;
}

// Queries registered components for various status information.
caf::message status_command(const command&, caf::actor_system&,
                            caf::config_value_map&, command::argument_iterator,
                            command::argument_iterator) {
  auto rp = this_node->make_response_promise();
  this_node->request(this_node->state.tracker, infinite, get_atom::value).then(
    [=, self = this_node](const registry& reg) mutable {
      json::object obj;
      for (auto& peer : reg.components) {
        json::array xs;
        for (auto& pair : peer.second)
          xs.push_back(json{pair.second.label});
        obj.emplace(peer.first, std::move(xs));
      }
      auto& sys = self->system();
      json::object sys_stats;
      sys_stats.emplace("running-actors", sys.registry().running());
      sys_stats.emplace("detached-actors", sys.detached_actors());
      sys_stats.emplace("worker-threads", sys.scheduler().num_workers());
      obj.emplace("system", std::move(sys_stats));
      rp.deliver(to_string(json{std::move(obj)}));
    },
    [=](caf::error& err) mutable {
      rp.deliver(std::move(err));
    }
  );
  return caf::none;
}

// Tries to spawn a new VAST component.
caf::expected<caf::actor> spawn_component(const command& cmd,
                                          spawn_arguments& args) {
  VAST_ASSERT(cmd.parent != nullptr);
  using caf::atom_uint;
  auto self = this_node;
  auto name_atm = caf::atom_from_string(cmd.name);
  auto parent_name_atm = caf::atom_from_string(cmd.parent->name);
  switch (atom_uint(name_atm)) {
    case atom_uint("archive"):
      return spawn_archive(self, args);
    case atom_uint("exporter"):
      return spawn_exporter(self, args);
    case atom_uint("importer"):
      return spawn_importer(self, args);
    case atom_uint("index"):
      return spawn_index(self, args);
    case atom_uint("metastore"):
      return spawn_metastore(self, args);
    case atom_uint("profiler"):
      return spawn_profiler(self, args);
    default:
      // Source and sink commands require dispatching on the parent name first.
      switch (atom_uint(parent_name_atm)) {
        case atom_uint("source"):
          switch (atom_uint(name_atm)) {
            case atom_uint("pcap"):
              return spawn_pcap_source(self, args);
            case atom_uint("bro"):
              return spawn_bro_source(self, args);
            case atom_uint("mrt"):
              return spawn_mrt_source(self, args);
            case atom_uint("bgpdump"):
              return spawn_bgpdump_source(self, args);
            default:
              break;
          }
          break;
        case atom_uint("sink"):
          switch (atom_uint(name_atm)) {
            case atom_uint("pcap"):
              return spawn_pcap_sink(self, args);
            case atom_uint("bro"):
              return spawn_bro_sink(self, args);
            case atom_uint("csv"):
              return spawn_csv_sink(self, args);
            case atom_uint("ascii"):
              return spawn_ascii_sink(self, args);
            case atom_uint("json"):
              return spawn_json_sink(self, args);
            default:
              break;
          }
          break;
      }
      return make_error(ec::unspecified, "invalid spawn component");
  }
}

caf::message spawn_command(const command& cmd, caf::actor_system&,
                           caf::config_value_map& options,
                           command::argument_iterator first,
                           command::argument_iterator last) {
  using std::end;
  using std::begin;
  // Save some typing.
  auto& st = this_node->state;
  // We configured the command to have the name of the component.
  // Note: caf::string_view is not convertible to string.
  string comp_name{cmd.name.begin(), cmd.name.end()};
  // Auto-generate label if none given.
  string label;
  if (auto label_ptr = caf::get_if<string>(&options, "global.label")) {
    label = *label_ptr;
  } else {
    label = comp_name;
    const char* multi_instance[] = {"importer", "exporter", "source", "sink"};
    if (std::count(begin(multi_instance), end(multi_instance), label) != 0) {
      // Create a new label and update our counter in the map.
      auto n = ++this_node->state.labels[label];
      label += '-';
      label += std::to_string(n);
      VAST_DEBUG(this_node, "auto-generated new label:", label);
    }
  }
  // Spawn our new VAST component.
  spawn_arguments args{cmd, st.dir, label, options, first, last};
  caf::actor new_component;
  if (auto spawn_res = spawn_component(cmd, args))
    new_component = std::move(*spawn_res);
  else
    return caf::make_message(std::move(spawn_res.error()));
  // Register component at tracker.
  auto rp = this_node->make_response_promise();
  this_node->request(st.tracker, infinite, try_put_atom::value,
                     std::move(comp_name), new_component,
                     std::move(label)).then(
    [=]() mutable {
      rp.deliver(std::move(new_component));
    },
    [=](error& e) mutable {
      rp.deliver(std::move(e));
    }
  );
  return caf::none;
}

caf::message kill_command(const command&, caf::actor_system&,
                          caf::config_value_map&,
                          command::argument_iterator first,
                          command::argument_iterator last) {
  if (std::distance(first, last) != 1)
    return err(ec::syntax_error, "expected exactly one component argument");
  auto rp = this_node->make_response_promise();
  this_node->request(this_node->state.tracker, infinite, get_atom::value).then(
    [rp, self = this_node, label = *first](registry& reg) mutable {
      auto& local = reg.components[self->state.name];
      auto i = std::find_if(local.begin(), local.end(),
                            [&](auto& p) { return p.second.label == label; });
      if (i == local.end()) {
        rp.deliver(make_error(ec::unspecified, "no such component: " + label));
        return;
      }
      self->send_exit(i->second.actor, exit_reason::user_shutdown);
      rp.deliver(ok_atom::value);
    },
    [rp](error& e) mutable {
      rp.deliver(std::move(e));
    }
  );
  return caf::none;
}

} // namespace <anonymous>

node_state::node_state(caf::event_based_actor* selfptr) : self(selfptr) {
  // nop
}

node_state::~node_state() {
  auto err = self->fail_state();
  if (!err)
    err = exit_reason::user_shutdown;
  self->send_exit(tracker, err);
  self->send_exit(accountant, std::move(err));
}

void node_state::init(string init_name, path init_dir) {
  // Set member variables.
  name = std::move(init_name);
  dir = std::move(init_dir);
  // Bring up the accountant.
  auto accountant_log = dir / "log" / "current" / "accounting.log";
  accountant = self->spawn<monitored>(system::accountant,
                                      std::move(accountant_log));
  self->system().registry().put(accountant_atom::value, accountant);
  // Bring up the tracker.
  tracker = self->spawn<monitored>(system::tracker, name);
  self->set_down_handler(
    [=](const down_msg& msg) {
      VAST_IGNORE_UNUSED(msg);
      VAST_DEBUG(self, "got DOWN from", msg.source);
      self->quit(msg.reason);
    }
  );
  self->system().registry().put(tracker_atom::value, tracker);
  // Default options for commands.
  auto opts = [] { return command::opts(); };
  // Add top-level commands.
  cmd.add(status_command, "status", "shows various properties of a topology",
          opts());
  cmd.add(kill_command, "kill", "terminates a component", opts());
  cmd.add(peer_command, "peer", "peers with another node", opts());
  // Add spawn commands.
  auto sp = cmd.add(nullptr, "spawn", "creates a new component", opts());
  sp->add(spawn_command, "archive", "creates a new archive",
          opts()
            .add<size_t>("segments,s", "number of cached segments")
            .add<size_t>("max-segment-size,m", "maximum segment size in MB"));
  sp->add(spawn_command, "exporter", "creates a new exporter",
          opts()
            .add<bool>("segments,s", "number of cached segments")
            .add<bool>("continuous,c", "marks a query as continuous")
            .add<bool>("historical,h", "marks a query as historical")
            .add<bool>("unified,u", "marks a query as unified")
            .add<uint64_t>("events,e", "maximum number of results"));
  sp->add(spawn_command, "importer", "creates a new importer",
          opts()
            .add<size_t>("ids,n",
                         "number of initial IDs to request (deprecated)"));
  sp->add(spawn_command, "index", "creates a new index",
          opts()
            .add<size_t>("max-events,e", "maximum events per partition")
            .add<size_t>("max-parts,p",
                         "maximum number of in-memory partitions")
            .add<size_t>("taste-parts,t",
                         "number of immediately scheduled partitions")
            .add<size_t>("max-queries,q",
                         "maximum number of concurrent queries"));
  sp->add(spawn_command, "metastore", "creates a new metastore",
          opts().add<raft::server_id>("id,i",
                                      "the server ID of the consensus module"));
  sp->add(spawn_command, "profiler", "creates a new profiler",
          opts()
            .add<bool>("cpu,c", "start the CPU profiler")
            .add<bool>("heap,h", "start the heap profiler")
            .add<size_t>("resolution,r", "seconds between measurements"));
  // Add spawn sink commands.
  auto src = sp->add(nullptr, "source", "creates a new source",
                     opts()
                       .add<string>("read,r", "path to input")
                       .add<bool>("uds,d", "treat -w as UNIX domain socket")
                       .add<string>("schema,s", "path to alternate schema"));
  src->add(spawn_command, "pcap", "creates a new PCAP source",
           opts()
             .add<size_t>("cutoff,c", "skip flow packets after this many bytes")
             .add<size_t>("flow-max,m", "number of concurrent flows to track")
             .add<size_t>("flow-age,a", "max flow lifetime before eviction")
             .add<size_t>("flow-expiry,e", "flow table expiration interval")
             .add<int64_t>("pseudo-realtime,p",
                           "factor c delaying trace packets by 1/c"));
  src->add(spawn_command, "test", "creates a new test source",
           opts()
             .add<size_t>("seed,s", "the PRNG seed")
             .add<size_t>("events,n", "number of events to generate"));
  src->add(spawn_command, "bro", "creates a new Bro source", opts());
  src->add(spawn_command, "bgpdump", "creates a new BGPdump source", opts());
  src->add(spawn_command, "mrt", "creates a new MRT source", opts());
  // Add spawn sink commands.
  auto snk = sp->add(nullptr, "sink", "creates a new sink",
                     opts()
                       .add<string>("write,w", "path to write events to")
                       .add<bool>("uds,d", "treat -w as UNIX domain socket"));
  snk->add(spawn_command, "pcap", "creates a new PCAP sink",
           opts().add<size_t>("flush,f",
                              "flush to disk after this many packets"));
  snk->add(spawn_command, "bro", "creates a new Bro sink", opts());
  snk->add(spawn_command, "ascii", "creates a new ASCII sink", opts());
  snk->add(spawn_command, "csv", "creates a new CSV sink", opts());
  snk->add(spawn_command, "json", "creates a new JSON sink", opts());
}

caf::behavior node(node_actor* self, string id, path dir) {
  self->state.init(std::move(id), std::move(dir));
  return {
    [=](const std::vector<std::string>& cli, caf::config_value_map& options) {
      VAST_DEBUG(self, "got command", cli, "with options", options);
      // Run the command.
      this_node = self;
      // Note: several commands make a response promise. In this case, they
      // return an empty message that has no effect when returning it.
      return run(self->state.cmd, self->system(), options, cli);
    },
    [=](const std::vector<std::string>& cli) {
      VAST_DEBUG(self, "got command", cli);
      // Run the command.
      this_node = self;
      return run(self->state.cmd, self->system(), cli);
    },
    [=](peer_atom, actor& tracker, string& peer_name) {
      self->delegate(self->state.tracker, peer_atom::value,
                     std::move(tracker), std::move(peer_name));
    },
    [=](get_atom) {
      auto rp = self->make_response_promise();
      self->request(self->state.tracker, infinite, get_atom::value).then(
        [=](registry& reg) mutable {
          rp.deliver(self->state.name, std::move(reg));
        },
        [=](error& e) mutable {
          rp.deliver(std::move(e));
        }
      );
    },
    [=](signal_atom, int signal) {
      VAST_IGNORE_UNUSED(signal);
      VAST_WARNING(self, "got signal", ::strsignal(signal));
    }
  };
}

} // namespace vast::system

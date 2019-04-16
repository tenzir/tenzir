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
#include "vast/defaults.hpp"
#include "vast/detail/assert.hpp"
#include "vast/json.hpp"
#include "vast/logger.hpp"
#include "vast/system/accountant.hpp"
#include "vast/system/node.hpp"
#include "vast/system/raft.hpp"
#include "vast/system/spawn_archive.hpp"
#include "vast/system/spawn_arguments.hpp"
#include "vast/system/spawn_consensus.hpp"
#include "vast/system/spawn_exporter.hpp"
#include "vast/system/spawn_importer.hpp"
#include "vast/system/spawn_index.hpp"
#include "vast/system/spawn_node.hpp"
#include "vast/system/spawn_or_connect_to_node.hpp"
#include "vast/system/spawn_profiler.hpp"
#include "vast/system/spawn_sink.hpp"
#include "vast/system/spawn_source.hpp"

using namespace caf;

namespace vast::system {

namespace {

// Local commands need access to the node actor.
thread_local node_actor* this_node;

// Convenience function for wrapping an error into a CAF message.
auto make_error_msg(ec code, std::string msg) {
  return caf::make_message(make_error(code, std::move(msg)));
}

// Stop the node and exit the process.
caf::message stop_command(const command&, caf::actor_system&, caf::settings&,
                          command::argument_iterator,
                          command::argument_iterator) {
  // We cannot use this_node->send() here because it triggers
  // an illegal instruction interrupt.
  caf::anon_send_exit(this_node, exit_reason::user_shutdown);
  return caf::none;
}

// Sends an atom to a registered actor. Blocks until the actor responds.
caf::message send_command(const command&, caf::actor_system& sys,
                          caf::settings&, command::argument_iterator first,
                          command::argument_iterator last) {
  // Expect exactly two arguments.
  if (std::distance(first, last) != 2)
    return make_error_msg(ec::syntax_error,
                          "expected two arguments: receiver and message atom");
  // Get destination actor from the registry.
  auto dst = sys.registry().get(caf::atom_from_string(*first));
  if (dst == nullptr)
    return make_error_msg(ec::syntax_error,
                          "registry contains no actor named " + *first);
  // Dispatch to destination.
  auto f = caf::make_function_view(caf::actor_cast<caf::actor>(dst));
  if (auto res = f(caf::atom_from_string(*(first + 1))))
    return std::move(*res);
  else
    return caf::make_message(std::move(res.error()));
}

// Tries to establish peering to another node.
caf::message peer_command(const command&, caf::actor_system& sys,
                          caf::settings&, command::argument_iterator first,
                          command::argument_iterator last) {
  VAST_ASSERT(this_node != nullptr);
  if (std::distance(first, last) != 1)
    return make_error_msg(ec::syntax_error,
                          "expected exactly one endpoint argument");
  auto ep = to<endpoint>(*first);
  if (!ep)
    return make_error_msg(ec::parse_error, "invalid endpoint format");
  // Use localhost:42000 by default.
  if (ep->host.empty())
    ep->host = "127.0.0.1";
  if (ep->port.number() == 0)
    return make_error_msg(ec::parse_error, "cannot connect to port 0");
  VAST_DEBUG(this_node, "connects to", ep->host << ':' << ep->port);
  auto& mm = sys.middleman();
  // TODO: this blocks the node, consider talking to the MM actor instead.
  auto peer = mm.remote_actor(ep->host.c_str(), ep->port.number());
  if (!peer) {
    VAST_ERROR(this_node, "failed to connect to peer:",
               sys.render(peer.error()));
    return caf::make_message(std::move(peer.error()));
  }
  VAST_DEBUG(this_node, "sends peering request");
  auto& st = this_node->state;
  this_node->delegate(*peer, peer_atom::value, st.tracker, st.name);
  return caf::none;
}

void collect_component_status(node_actor* self,
                              caf::response_promise status_promise,
                              registry& reg) {
  // Shared state between our response handlers.
  struct req_state_t {
    // Keeps track of how many requests are pending.
    size_t pending = 0;
    // Promise to the original client request.
    caf::response_promise rp;
    // Maps nodes to a map associating components with status information.
    caf::settings content;
  };
  auto req_state = std::make_shared<req_state_t>();
  req_state->rp = std::move(status_promise);
  // Pre-fill our result with system stats.
  auto& sys_stats = put_dictionary(req_state->content, "system");
  auto& sys = self->system();
  put(sys_stats, "running-actors", sys.registry().running());
  put(sys_stats, "detached-actors", sys.detached_actors());
  put(sys_stats, "worker-threads", sys.scheduler().num_workers());
  // Send out requests and collects answers.
  for (auto& [node_name, state_map] : reg.components) {
    req_state->pending += state_map.size();
    for (auto& kvp : state_map) {
      auto& comp_state = kvp.second;
      // Skip the tracker. It has no interesting state.
      if (comp_state.label == "tracker") {
        req_state->pending -= 1;
        continue;
      }
      self->request(comp_state.actor, infinite, status_atom::value).then(
        [=, lbl = comp_state.label, nn = node_name]
        (caf::config_value::dictionary& xs) mutable {
          auto& st = *req_state;
          st.content[nn].as_dictionary().emplace(std::move(lbl), std::move(xs));
          if (--st.pending == 0)
            st.rp.deliver(to_string(to_json(st.content)));
        },
        [=, lbl = comp_state.label, nn = node_name](caf::error& err) mutable {
          auto& st = *req_state;
          auto& dict = st.content[nn].as_dictionary();
          dict.emplace(std::move(lbl), self->system().render(err));
          if (--st.pending == 0)
            st.rp.deliver(to_string(to_json(st.content)));
        }
      );
    }
  }
}

caf::message status_command(const command&, caf::actor_system&,
                            caf::settings&, command::argument_iterator,
                            command::argument_iterator) {
  auto self = this_node;
  auto rp = self->make_response_promise();
  self->request(self->state.tracker, infinite, get_atom::value).then(
    [=](registry& reg) mutable {
      collect_component_status(self, std::move(rp), reg);
    }
  );
  return caf::none;
}

maybe_actor spawn_accountant(node_actor* self, spawn_arguments& args) {
  auto accountant_log = args.dir / "log" / "current" / "accounting.log";
  auto accountant = self->spawn<monitored>(system::accountant, accountant_log);
  self->system().registry().put(accountant_atom::value, accountant);
  return caf::actor_cast<caf::actor>(accountant);
}

// Tries to spawn a new VAST component.
caf::expected<caf::actor> spawn_component(const command& cmd,
                                          spawn_arguments& args) {
  VAST_TRACE(VAST_ARG(args));
  VAST_ASSERT(cmd.parent != nullptr);
  using caf::atom_uint;
  auto self = this_node;
  auto i = node_state::factories.find(full_name(cmd));
  if (i == node_state::factories.end())
    return make_error(ec::unspecified, "invalid spawn component");
  return i->second(self, args);
}

caf::message spawn_command(const command& cmd, caf::actor_system& sys,
                           caf::settings& options,
                           command::argument_iterator first,
                           command::argument_iterator last) {
  VAST_TRACE(VAST_ARG(options), VAST_ARG("args", first, last));
  VAST_UNUSED(sys);
  using std::end;
  using std::begin;
  // Save some typing.
  auto& st = this_node->state;
  // We configured the command to have the name of the component.
  // Note: caf::string_view is not convertible to string.
  std::string comp_name{cmd.name.begin(), cmd.name.end()};
  // Auto-generate label if none given.
  std::string label;
  if (auto label_ptr = caf::get_if<std::string>(&options, "label")) {
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
  else {
    VAST_DEBUG(__func__, "got an error from spawn_component:",
               sys.render(spawn_res.error()));
    return caf::make_message(std::move(spawn_res.error()));
  }
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

caf::message kill_command(const command&, caf::actor_system&, caf::settings&,
                          command::argument_iterator first,
                          command::argument_iterator last) {
  if (std::distance(first, last) != 1)
    return make_error_msg(ec::syntax_error,
                          "expected exactly one component argument");
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

/// Lifts a factory function that accepts `local_actor*` as first argument
/// to a function accpeting `node_actor*` instead.
template <maybe_actor (*Fun)(local_actor*, spawn_arguments&)>
node_state::component_factory lift_component_factory() {
  return [](node_actor* self, spawn_arguments& args) {
    // Delegate to lifted function.
    return Fun(self, args);
  };
}

template <maybe_actor (*Fun)(node_actor*, spawn_arguments&)>
node_state::component_factory lift_component_factory() {
  return Fun;
}

#define ADD(cmd_full_name, fun)                                                \
  result.emplace(cmd_full_name, lift_component_factory<fun>())

auto make_factories() {
  node_state::named_component_factories result;
  ADD("spawn accountant", spawn_accountant);
  ADD("spawn archive", spawn_archive);
  ADD("spawn exporter", spawn_exporter);
  ADD("spawn importer", spawn_importer);
  ADD("spawn index", spawn_index);
  ADD("spawn consensus", spawn_consensus);
  ADD("spawn profiler", spawn_profiler);
  ADD("spawn source pcap", spawn_pcap_source);
  ADD("spawn source zeek", spawn_zeek_source);
  ADD("spawn source mrt", spawn_mrt_source);
  ADD("spawn source bgpdump", spawn_bgpdump_source);
  ADD("spawn sink pcap", spawn_pcap_sink);
  ADD("spawn sink zeek", spawn_zeek_sink);
  ADD("spawn sink csv", spawn_csv_sink);
  ADD("spawn sink ascii", spawn_ascii_sink);
  ADD("spawn sink json", spawn_json_sink);
  return result;
}

#undef ADD

} // namespace <anonymous>

node_state::named_component_factories node_state::factories = make_factories();

node_state::node_state(caf::event_based_actor* selfptr) : self(selfptr) {
  // nop
}

node_state::~node_state() {
  auto err = self->fail_state();
  if (!err)
    err = exit_reason::user_shutdown;
  self->send_exit(tracker, err);
}

void node_state::init(std::string init_name, path init_dir) {
  // Set member variables.
  name = std::move(init_name);
  dir = std::move(init_dir);
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
  cmd.add(stop_command, "stop", "stops the node", opts());
  cmd.add(kill_command, "kill", "terminates a component", opts());
  cmd.add(send_command, "send", "sends atom to a registered actor", opts());
  cmd.add(peer_command, "peer", "peers with another node", opts());
  // Add spawn commands.
  auto sp = cmd.add(nullptr, "spawn", "creates a new component", opts());
  sp->add(spawn_command, "accountant", "spawns the accountant", opts());
  sp->add(spawn_command, "archive", "creates a new archive",
          opts()
            .add<size_t>("segments,s", "number of cached segments")
            .add<size_t>("max-segment-size,m", "maximum segment size in MB"));
  sp->add(spawn_command, "exporter", "creates a new exporter",
          opts()
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
  sp->add(spawn_command, "consensus", "creates a new consensus",
          opts().add<raft::server_id>("id,i",
                                      "the server ID of the consensus module"));
  sp->add(spawn_command, "profiler", "creates a new profiler",
          opts()
            .add<bool>("cpu,c", "start the CPU profiler")
            .add<bool>("heap,h", "start the heap profiler")
            .add<size_t>("resolution,r", "seconds between measurements"));
  // Add spawn source commands.
  auto src
    = sp->add(nullptr, "source", "creates a new source",
              opts()
                .add<std::string>("read,r", "path to input")
                .add<std::string>("schema,s", "path to alternate schema")
                .add<caf::atom_value>("table-slice,t", "table slice type")
                .add<bool>("uds,d", "treat -w as UNIX domain socket"));
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
  src->add(spawn_command, "zeek", "creates a new Zeek source", opts());
  src->add(spawn_command, "bgpdump", "creates a new BGPdump source", opts());
  src->add(spawn_command, "mrt", "creates a new MRT source", opts());
  // Add spawn sink commands.
  auto snk = sp->add(nullptr, "sink", "creates a new sink",
                     opts()
                       .add<std::string>("write,w", "path to write events to")
                       .add<bool>("uds,d", "treat -w as UNIX domain socket"));
  snk->add(spawn_command, "pcap", "creates a new PCAP sink",
           opts().add<size_t>("flush,f",
                              "flush to disk after this many packets"));
  snk->add(spawn_command, "zeek", "creates a new Zeek sink", opts());
  snk->add(spawn_command, "ascii", "creates a new ASCII sink", opts());
  snk->add(spawn_command, "csv", "creates a new CSV sink", opts());
  snk->add(spawn_command, "json", "creates a new JSON sink", opts());
}

caf::behavior node(node_actor* self, std::string id, path dir) {
  self->state.init(std::move(id), std::move(dir));
  return {
    [=](const std::vector<std::string>& cli, caf::settings& options) {
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
    [=](peer_atom, actor& tracker, std::string& peer_name) {
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

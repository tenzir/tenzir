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

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/endpoint.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/json.hpp"
#include "vast/config.hpp"
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
#include "vast/system/spawn_counter.hpp"
#include "vast/system/spawn_exporter.hpp"
#include "vast/system/spawn_importer.hpp"
#include "vast/system/spawn_index.hpp"
#include "vast/system/spawn_node.hpp"
#include "vast/system/spawn_or_connect_to_node.hpp"
#include "vast/system/spawn_pivoter.hpp"
#include "vast/system/spawn_profiler.hpp"
#include "vast/system/spawn_sink.hpp"
#include "vast/system/spawn_source.hpp"
#include "vast/system/spawn_type_registry.hpp"

#include <caf/all.hpp>
#include <caf/io/all.hpp>

#include <chrono>
#include <csignal>
#include <fstream>
#include <sstream>

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
caf::message stop_command(const command::invocation&, caf::actor_system&) {
  // We cannot use this_node->send() here because it triggers
  // an illegal instruction interrupt.
  caf::anon_send_exit(this_node, exit_reason::user_shutdown);
  return caf::none;
}

// Sends an atom to a registered actor. Blocks until the actor responds.
caf::message
send_command(const command::invocation& invocation, caf::actor_system& sys) {
  auto first = invocation.arguments.begin();
  auto last = invocation.arguments.end();
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
caf::message
peer_command(const command::invocation& invocation, caf::actor_system& sys) {
  auto first = invocation.arguments.begin();
  auto last = invocation.arguments.end();
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

caf::message status_command(const command::invocation&, caf::actor_system&) {
  auto self = this_node;
  auto rp = self->make_response_promise();
  self->request(self->state.tracker, infinite, get_atom::value).then(
    [=](registry& reg) mutable {
      collect_component_status(self, std::move(rp), reg);
    }
  );
  return caf::none;
}

maybe_actor spawn_accountant(node_actor* self, spawn_arguments&) {
  auto accountant = self->spawn<monitored>(system::accountant);
  self->system().registry().put(accountant_atom::value, accountant);
  return caf::actor_cast<caf::actor>(accountant);
}

// Tries to spawn a new VAST component.
caf::expected<caf::actor>
spawn_component(const command::invocation& invocation, spawn_arguments& args) {
  VAST_TRACE(VAST_ARG(args));
  using caf::atom_uint;
  auto self = this_node;
  auto i = node_state::component_factory.find(invocation.full_name);
  if (i == node_state::component_factory.end())
    return make_error(ec::unspecified, "invalid spawn component");
  return i->second(self, args);
}

caf::message
kill_command(const command::invocation& invocation, caf::actor_system&) {
  auto first = invocation.arguments.begin();
  auto last = invocation.arguments.end();
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
node_state::component_factory_fun lift_component_factory() {
  return [](node_actor* self, spawn_arguments& args) {
    // Delegate to lifted function.
    return Fun(self, args);
  };
}

template <maybe_actor (*Fun)(node_actor*, spawn_arguments&)>
node_state::component_factory_fun lift_component_factory() {
  return Fun;
}

auto make_component_factory() {
  return node_state::named_component_factory{
    {"spawn accountant", lift_component_factory<spawn_accountant>()},
    {"spawn archive", lift_component_factory<spawn_archive>()},
    {"spawn counter", lift_component_factory<spawn_counter>()},
    {"spawn exporter", lift_component_factory<spawn_exporter>()},
    {"spawn importer", lift_component_factory<spawn_importer>()},
    {"spawn type-registry", lift_component_factory<spawn_type_registry>()},
    {"spawn index", lift_component_factory<spawn_index>()},
    {"spawn consensus", lift_component_factory<spawn_consensus>()},
    {"spawn pivoter", lift_component_factory<spawn_pivoter>()},
    {"spawn profiler", lift_component_factory<spawn_profiler>()},
    {"spawn source pcap", lift_component_factory<spawn_pcap_source>()},
    {"spawn source syslog", lift_component_factory<spawn_syslog_source>()},
    {"spawn source zeek", lift_component_factory<spawn_zeek_source>()},
    {"spawn source mrt", lift_component_factory<spawn_mrt_source>()},
    {"spawn source bgpdump", lift_component_factory<spawn_bgpdump_source>()},
    {"spawn sink pcap", lift_component_factory<spawn_pcap_sink>()},
    {"spawn sink zeek", lift_component_factory<spawn_zeek_sink>()},
    {"spawn sink csv", lift_component_factory<spawn_csv_sink>()},
    {"spawn sink ascii", lift_component_factory<spawn_ascii_sink>()},
    {"spawn sink json", lift_component_factory<spawn_json_sink>()},
  };
}

auto make_command_factory() {
  // When updating this list, remember to update its counterpart in
  // application.cpp as well iff necessary
  return command::factory{
    {"kill", kill_command},
    {"peer", peer_command},
    {"send", send_command},
    {"spawn accountant", node_state::spawn_command},
    {"spawn archive", node_state::spawn_command},
    {"spawn consensus", node_state::spawn_command},
    {"spawn counter", node_state::spawn_command},
    {"spawn exporter", node_state::spawn_command},
    {"spawn importer", node_state::spawn_command},
    {"spawn type-registry", node_state::spawn_command},
    {"spawn index", node_state::spawn_command},
    {"spawn pivoter", node_state::spawn_command},
    {"spawn sink ascii", node_state::spawn_command},
    {"spawn sink csv", node_state::spawn_command},
    {"spawn sink json", node_state::spawn_command},
    {"spawn sink pcap", node_state::spawn_command},
    {"spawn sink zeek", node_state::spawn_command},
    {"spawn source bgpdump", node_state::spawn_command},
    {"spawn source mrt", node_state::spawn_command},
    {"spawn source pcap", node_state::spawn_command},
    {"spawn source syslog", node_state::spawn_command},
    {"spawn source test", node_state::spawn_command},
    {"spawn source zeek", node_state::spawn_command},
    {"status", status_command},
    {"stop", stop_command},
  };
}

} // namespace

caf::message
node_state::spawn_command(const command::invocation& invocation,
                          [[maybe_unused]] caf::actor_system& sys) {
  VAST_TRACE(invocation);
  using std::begin;
  using std::end;
  // Save some typing.
  auto& st = this_node->state;
  // We configured the command to have the name of the component.
  std::string comp_name{invocation.name()};
  // Auto-generate label if none given.
  std::string label;
  if (auto label_ptr = caf::get_if<std::string>(&invocation.options, "label")) {
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
  spawn_arguments args{invocation, st.dir, label};
  caf::actor new_component;
  if (auto spawn_res = spawn_component(invocation, args))
    new_component = std::move(*spawn_res);
  else {
    VAST_DEBUG(__func__, "got an error from spawn_component:",
               sys.render(spawn_res.error()));
    return caf::make_message(std::move(spawn_res.error()));
  }
  // Register component at tracker.
  auto rp = this_node->make_response_promise();
  this_node
    ->request(st.tracker, infinite, try_put_atom::value, std::move(comp_name),
              new_component, std::move(label))
    .then([=]() mutable { rp.deliver(std::move(new_component)); },
          [=](error& e) mutable { rp.deliver(std::move(e)); });
  return caf::none;
}

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
  node_state::component_factory = make_component_factory();
  if (node_state::extra_component_factory != nullptr) {
    auto extra = node_state::extra_component_factory();
    // FIXME replace with std::map::merge once CI is updated to a newer libc++
    extra.insert(node_state::component_factory.begin(),
                 node_state::component_factory.end());
    node_state::component_factory = std::move(extra);
  }
  node_state::command_factory = make_command_factory();
  if (node_state::extra_command_factory != nullptr) {
    auto extra = node_state::extra_command_factory();
    // FIXME replace with std::map::merge once CI is updated to a newer libc++
    extra.insert(node_state::command_factory.begin(),
                 node_state::command_factory.end());
    node_state::command_factory = std::move(extra);
  }
  // Set member variables.
  name = std::move(init_name);
  dir = std::move(init_dir);
  // Bring up the tracker.
  tracker = self->spawn<monitored>(system::tracker, name);
  self->set_down_handler([=](const down_msg& msg) {
    VAST_IGNORE_UNUSED(msg);
    VAST_DEBUG(self, "got DOWN from", msg.source);
    self->quit(msg.reason);
  });
  self->system().registry().put(tracker_atom::value, tracker);
}

caf::behavior node(node_actor* self, std::string id, path dir) {
  self->state.init(std::move(id), std::move(dir));
  return {
    [=](const command::invocation& invocation) {
      VAST_DEBUG(self, "got command", invocation.full_name, "with options",
                 invocation.options, "and arguments", invocation.arguments);
      // Run the command.
      this_node = self;
      return run(invocation, self->system(), node_state::command_factory);
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
    }};
}

} // namespace vast::system

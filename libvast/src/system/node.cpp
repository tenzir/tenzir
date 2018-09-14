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
#include "vast/json.hpp"
#include "vast/logger.hpp"

#include "vast/system/accountant.hpp"
#include "vast/system/consensus.hpp"
#include "vast/system/node.hpp"
#include "vast/system/spawn.hpp"

using namespace std::string_literals;
using namespace caf;

namespace vast {
namespace system {

namespace {

using node_ptr = stateful_actor<node_state>*;

void stop(node_ptr self) {
  self->send_exit(self, exit_reason::user_shutdown);
}

void peer(node_ptr self, message args) {
  auto rp = self->make_response_promise();
  if (args.empty()) {
    rp.deliver(make_error(ec::syntax_error, "no endpoint given"));
    return;
  }
  auto ep = to<endpoint>(args.get_as<std::string>(0));
  if (!ep) {
    rp.deliver(make_error(ec::parse_error, "invalid endpoint format"));
    return;
  }
  // Use localhost:42000 by default.
  if (ep->host.empty())
    ep->host = "127.0.0.1";
  if (ep->port == 0)
    ep->port = 42000;
  VAST_DEBUG(self, "connects to", ep->host << ':' << ep->port);
  auto& mm = self->system().middleman();
  auto peer = mm.remote_actor(ep->host.c_str(), ep->port);
  if (!peer) {
    VAST_ERROR(self, "failed to connect to peer:",
               self->system().render(peer.error()));
    rp.deliver(peer.error());
    return;
  }
  VAST_DEBUG(self, "sends peering request");
  auto t = actor_cast<actor>(self->state.tracker);
  rp.delegate(*peer, peer_atom::value, t, self->state.name);
}

void show(node_ptr self, message /* args */) {
  auto rp = self->make_response_promise();
  self->request(self->state.tracker, infinite, get_atom::value).then(
    [=](const registry& reg) mutable {
      json::object result;
      for (auto& peer : reg.components) {
        json::array xs;
        for (auto& pair : peer.second)
          xs.push_back(json{pair.second.label});
        result.emplace(peer.first, std::move(xs));
      }
      rp.deliver(to_string(json{std::move(result)}));
    }
  );
}

void spawn(node_ptr self, message args) {
  auto rp = self->make_response_promise();
  if (args.empty()) {
    rp.deliver(make_error(ec::syntax_error, "missing arguments"));
    return;
  }
  using factory_function = std::function<expected<actor>(options&)>;
  auto bind = [=](auto f) {
    return [=](options& opts) {
      return f(self, opts);
    };
  };
  static auto factory = std::unordered_map<std::string, factory_function>{
    {"metastore", bind(spawn_metastore)},
    {"archive", bind(spawn_archive)},
    {"index", bind(spawn_index)},
    {"importer", bind(spawn_importer)},
    {"exporter", bind(spawn_exporter)},
    {"source", bind(spawn_source)},
    {"sink", bind(spawn_sink)},
    {"profiler", bind(spawn_profiler)}
  };
  // Split arguments into two halves at the command: the first half pertains to
  // "spawn" and the second half the command.
  factory_function fun;
  std::string component;
  size_t i;
  for (i = 0; i < args.size(); ++i) {
    auto j = factory.find(args.get_as<std::string>(i));
    if (j != factory.end()) {
      component = j->first;
      fun = j->second;
      break;
    }
  }
  if (!fun) {
    rp.deliver(make_error(ec::unspecified, "invalid spawn component"));
    return;
  }
  // Parse arguments.
  auto component_args = args.take_right(args.size() - i - 1);
  auto spawn_args = args.take(i);
  std::string label;
  auto r = spawn_args.extract_opts({
    {"label,l", "a unique label for the component", label}
  });
  if (!r.remainder.empty()) {
    auto invalid = r.remainder.get_as<std::string>(0);
    rp.deliver(make_error(ec::syntax_error, "invalid syntax", invalid));
  }
  if (!r.error.empty())
    rp.deliver(make_error(ec::syntax_error, std::move(r.error)));
  // Register the component with the track.
  self->request(self->state.tracker, infinite, get_atom::value).then(
    [=](registry& reg) mutable {
      VAST_ASSERT(reg.components.count(self->state.name) > 0);
      auto& local = reg.components[self->state.name];
      // Check if we can spawn more than one instance of the given component.
      for (auto c : {"metastore", "archive", "index", "profiler"})
        if (c == component && local.count(component) > 0) {
          rp.deliver(make_error(ec::unspecified, "component already exists"));
          return;
        }
      // Auto-generate a label if none given.
      if (label.empty()) {
        label = component;
        auto multi_instance = component == "importer"
                           || component == "exporter"
                           || component == "source"
                           || component == "sink";
        if (multi_instance) {
          label += '-';
          label += std::to_string(++self->state.labels[component]);
          VAST_DEBUG(self, "auto-generated new label:", label);
        }
      }
      // Dispatch spawn command.
      auto opts = options{component_args, self->state.dir, label};
      auto a = fun(opts);
      if (!a) {
        rp.deliver(a.error());
        return;
      }
      self->request(self->state.tracker, infinite, put_atom::value, component,
                    *a, label).then(
        [=](ok_atom) mutable {
          rp.deliver(*a);
        },
        [=](error& e) mutable {
          rp.deliver(std::move(e));
        }
      );
    },
    [=](error& e) mutable {
      rp.deliver(std::move(e));
    }
  );
}

void kill(node_ptr self, message args) {
  auto rp = self->make_response_promise();
  if (args.empty()) {
    rp.deliver(make_error(ec::syntax_error, "missing component"));
    return;
  }
  if (args.size() > 1) {
    rp.deliver(make_error(ec::syntax_error, "too many arguments"));
    return;
  }
  self->request(self->state.tracker, infinite, get_atom::value).then(
    [=](registry& reg) mutable {
      auto& label = args.get_as<std::string>(0);
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
    [=](error& e) mutable {
      rp.deliver(std::move(e));
    }
  );
}

void send(node_ptr self, message args) {
  auto rp = self->make_response_promise();
  if (args.empty()) {
    rp.deliver(make_error(ec::syntax_error, "missing component"));
    return;
  } else if (args.size() == 1) {
    rp.deliver(make_error(ec::syntax_error, "missing command"));
    return;
  }
  self->request(self->state.tracker, infinite, get_atom::value).then(
    [=](registry& reg) mutable {
      auto& label = args.get_as<std::string>(0);
      auto& local = reg.components[self->state.name];
      auto i = std::find_if(local.begin(), local.end(),
                            [&](auto& p) { return p.second.label == label; });
      if (i == local.end()) {
        rp.deliver(make_error(ec::unspecified, "no such component: " + label));
        return;
      }
      auto cmd = atom_from_string(args.get_as<std::string>(1));
      self->send(i->second.actor, cmd);
      rp.deliver(ok_atom::value);
    },
    [=](error& e) mutable {
      rp.deliver(std::move(e));
    }
  );
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

void node_state::init(std::string init_name, path init_dir) {
  // Set member variables.
  name = std::move(init_name);
  dir = std::move(init_dir);
  // Bring up the accountant.
  auto accountant_log = dir / "log" / "current" / "accounting.log";
  accountant = self->spawn<monitored>(system::accountant,
                                      std::move(accountant_log));
  self->system().registry().put(accountant_atom::value,
                                actor_cast<strong_actor_ptr>(accountant));
  // Bring up the tracker.
  tracker = self->spawn<monitored>(system::tracker, name);
  self->set_down_handler(
    [=](const down_msg& msg) {
      VAST_IGNORE_UNUSED(msg);
      VAST_DEBUG(self, "got DOWN from", msg.source);
      self->quit(msg.reason);
    }
  );
}

caf::behavior node(node_ptr self, std::string id, path dir) {
  self->state.init(std::move(id), std::move(dir));
  return {
    [=](const std::string& cmd, message& args) {
      VAST_DEBUG(self, "got command", cmd, deep_to_string(args));
      if (cmd == "stop") {
        stop(self);
      } else if (cmd == "peer") {
        peer(self, args);
      } else if (cmd == "show") {
        show(self, args);
      } else if (cmd == "spawn") {
        spawn(self, args);
      } else if (cmd == "kill") {
        kill(self, args);
      } else if (cmd == "send") {
        send(self, args);
      } else {
        auto e = make_error(ec::unspecified, "invalid command", cmd);
        VAST_WARNING(self, self->system().render(e));
        self->make_response_promise().deliver(std::move(e));
      }
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

} // namespace system
} // namespace vast

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

#include <caf/all.hpp>

#include "vast/detail/assert.hpp"
#include "vast/detail/string.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/system/archive.hpp"
#include "vast/system/consensus.hpp"
#include "vast/system/tracker.hpp"

using namespace caf;

namespace vast::system {

namespace {

struct terminator_state {
  terminator_state(event_based_actor* selfptr) : self(selfptr) {
    // nop
  }

  caf::error reason;
  actor parent;
  component_state_map components;
  std::vector<std::string> victim_stack;
  size_t pending_down_messages = 0;
  event_based_actor* self;

  // Keeps this actor alive until we manually stop it.
  strong_actor_ptr anchor;

  static inline const char* name = "terminator";

  void init(caf::error reason, caf::actor parent,
            component_state_map components,
            std::vector<std::string> victim_stack) {
    VAST_TRACE(VAST_ARG(components), VAST_ARG(victim_stack));
    this->anchor = actor_cast<strong_actor_ptr>(self);
    this->reason = std::move(reason);
    this->parent = std::move(parent);
    this->components = std::move(components);
    this->victim_stack = std::move(victim_stack);
  }

  template <class Label>
  void kill(const caf::actor& actor, const Label& label) {
    VAST_IGNORE_UNUSED(label);
    VAST_DEBUG(self, "sends exit_msg to", label);
    self->monitor(actor);
    self->send_exit(actor, reason);
    ++pending_down_messages;
  }

  void kill_next() {
    VAST_TRACE("");
    do {
      if (victim_stack.empty()) {
        if (parent == nullptr) {
          VAST_DEBUG(self, "stops after stopping all components");
          anchor = nullptr;
          self->quit();
          return;
        }
        VAST_DEBUG(self, "kills parent and remaining components");
        for (auto& component : components)
          kill(component.second.actor, component.second.label);
        components.clear();
        kill(parent, "tracker");
        parent = nullptr;
        return;
      }
      auto er = components.equal_range(victim_stack.back());
      for (auto i = er.first; i != er.second; ++i)
        kill(i->second.actor, i->second.label);
      components.erase(er.first, er.second);
      victim_stack.pop_back();
    } while (pending_down_messages == 0);
  }

  void got_down_msg() {
    VAST_TRACE("");
    if (--pending_down_messages == 0)
      kill_next();
  }
};

behavior terminator(stateful_actor<terminator_state>* self, caf::error reason,
                    caf::actor parent, component_state_map components,
                    std::vector<std::string> victim_stack) {
  VAST_DEBUG(self, "starts terminator with", victim_stack.size(),
             "victims on the stack and", components.size(), "in total");
  self->state.init(std::move(reason), std::move(parent), std::move(components),
                   std::move(victim_stack));
  self->state.kill_next();
  self->set_down_handler([=](const down_msg&) {
    self->state.got_down_msg();
  });
  return {
    [] {
      // Dummy message handler to keep the actor alive and kicking.
    }
  };
}

void register_component(scheduled_actor* self, tracker_state& st,
                        const std::string& type, const actor& component,
                        std::string& label) {
  using caf::anon_send;
  // Save new component.
  self->monitor(component);
  auto& local = st.registry.components[st.node];
  local.emplace(type, component_state{component, label});
  // Wire it to existing components.
  auto actors = [&](auto key) {
    std::vector<actor> result;
    auto er = local.equal_range(key);
    for (auto i = er.first; i != er.second; ++i)
      result.push_back(i->second.actor);
    return result;
  };
  if (type == "exporter") {
    for (auto& a : actors("archive"))
      anon_send(component, actor_cast<archive_type>(a));
    for (auto& a : actors("index"))
      anon_send(component, index_atom::value, a);
    for (auto& a : actors("sink"))
      anon_send(component, sink_atom::value, a);
  } else if (type == "importer") {
    for (auto& a : actors("consensus"))
      anon_send(component, actor_cast<consensus_type>(a));
    for (auto& a : actors("archive"))
      anon_send(component, actor_cast<archive_type>(a));
    for (auto& a : actors("index"))
      anon_send(component, index_atom::value, a);
    for (auto& a : actors("source"))
      anon_send(a, sink_atom::value, component);
  } else if (type == "source") {
    for (auto& a : actors("importer"))
      anon_send(component, sink_atom::value, a);
  } else if (type == "sink") {
    for (auto& a : actors("exporter"))
      anon_send(a, sink_atom::value, component);
  }
  // Propagate new component to peer.
  auto msg = make_message(put_atom::value, st.node, type, component, label);
  for (auto& peer : st.registry.components) {
    auto& t = peer.second.find("tracker")->second.actor;
    if (t != self)
      anon_send(t, msg);
  }
}

// Checks whether a component can be spawned at most once.
bool is_singleton(const std::string& component) {
  const char* singletons[] = {"archive", "index", "consensus"};
  auto pred = [&](const char* lhs) { return lhs == component; };
  return std::any_of(std::begin(singletons), std::end(singletons), pred);
}

} // namespace <anonymous>

tracker_type::behavior_type
tracker(tracker_type::stateful_pointer<tracker_state> self, std::string node) {
  self->state.node = node;
  // Insert ourself into the registry.
  self->state.registry.components[node].emplace(
    "tracker", component_state{actor_cast<actor>(self), "tracker"});
  self->set_down_handler(
    [=](const down_msg& msg) {
      auto pred = [&](auto& p) { return p.second.actor == msg.source; };
      for (auto& [node, comp_state] : self->state.registry.components) {
        auto i = std::find_if(comp_state.begin(), comp_state.end(), pred);
        if (i != comp_state.end()) {
          if (i->first == "tracker")
            self->state.registry.components.erase(node);
          else
            comp_state.erase(i);
          return;
        }
      }
    }
  );
  self->set_exit_handler(
    [=](const exit_msg& msg) {
      // Only trap the first exit, regularly shutdown on the next one.
      self->set_exit_handler({});
      // We shut down the components in the order in which data flows so that
      // downstream components can still process in-flight data. Because the
      // terminator operates with a stack of components, we specify them in
      // reverse order.
      self->spawn(terminator, msg.reason, actor_cast<actor>(self),
                  self->state.registry.components[node],
                  std::vector<std::string>{"exporter", "index", "archive",
                                           "importer", "source"});
    }
  );
  return {
    [=](put_atom, const std::string& type, const actor& component,
        std::string& label) -> result<ok_atom> {
      VAST_DEBUG(self, "got new", type, '(' << label << ')');
      register_component(self, self->state, type, component, label);
      return ok_atom::value;
    },
    [=](try_put_atom, const std::string& type, const actor& component,
        std::string& label) -> result<void> {
      VAST_DEBUG(self, "got new", type, '(' << label << ')');
      auto& st = self->state;
      auto& local = st.registry.components[node];
      if (is_singleton(type) && local.count(type) > 0)
        return make_error(ec::unspecified, "component already exists");
      register_component(self, st, type, component, label);
      return caf::unit;
    },
    [=](put_atom, const std::string& name, const std::string& type,
        const actor& component, const std::string& label) {
      VAST_DEBUG(self, "got PUT from peer", name, "for", type);
      auto& components = self->state.registry.components[name];
      components.emplace(type, component_state{component, label});
      self->monitor(component);
    },
    [=](get_atom) -> result<registry> {
      return self->state.registry;
    },
    [=](peer_atom, const actor& peer, const std::string& peer_name)
    -> typed_response_promise<state_atom, registry> {
      auto rp = self->make_response_promise<state_atom, registry>();
      if (self->state.registry.components.count(peer_name) > 0) {
        VAST_ERROR(self, "peer name already exists", peer_name);
        return rp.deliver(make_error(ec::unspecified, "duplicate node name"));
      }
      VAST_DEBUG(self, "shipping state to new peer", peer_name);
      rp.delegate(peer, state_atom::value, self->state.registry);
      return rp;
    },
    [=](state_atom, registry& reg) -> result<ok_atom> {
      VAST_DEBUG(self, "got state for", reg.components.size(), "peers");
      // Monitor all remote components.
      for (auto& peer : reg.components)
        for (auto& pair : peer.second)
          self->monitor(pair.second.actor);
      // Incorporate new state from peer.
      std::move(reg.components.begin(), reg.components.end(),
                std::inserter(self->state.registry.components,
                              self->state.registry.components.end()));
      // Broadcast our own state to all peers, without expecting a reply.
      auto i = self->state.registry.components.find(node);
      VAST_ASSERT(i != self->state.registry.components.end());
      for (auto& peer : self->state.registry.components) {
        auto& t = peer.second.find("tracker")->second.actor;
        if (t != self)
          self->send(actor_cast<tracker_type>(t), state_atom::value,
                     component_map_entry{*i});
      }
      return ok_atom::value;
    },
    [=](state_atom, component_map_entry& entry) {
      VAST_DEBUG(self, "got components from new peer");
      for (auto& pair : entry.second)
        self->monitor(pair.second.actor);
      auto result = self->state.registry.components.insert(std::move(entry));
      VAST_ASSERT(result.second);
    }
  };
}

} // namespace vast::system

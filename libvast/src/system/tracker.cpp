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

#include "vast/system/tracker.hpp"

#include "vast/detail/assert.hpp"
#include "vast/detail/string.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/system/archive.hpp"
#include "vast/system/shutdown.hpp"

using namespace caf;

namespace vast::system {

namespace {

void register_component(scheduled_actor* self, tracker_state& st,
                        const std::string& type, const actor& component,
                        std::string& label) {
  using caf::anon_send;
  // Save new component.
  self->monitor(component);
  auto& local = st.registry.components.value[st.node].value;
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
      anon_send(component, atom::index_v, a);
    for (auto& a : actors("sink"))
      anon_send(component, atom::sink_v, a);
  } else if (type == "importer") {
    for (auto& a : actors("source"))
      anon_send(a, atom::sink_v, component);
  } else if (type == "source") {
    for (auto& a : actors("importer"))
      anon_send(component, atom::sink_v, a);
  } else if (type == "sink") {
    for (auto& a : actors("exporter"))
      anon_send(a, atom::sink_v, component);
  }
  // Propagate new component to peer.
  auto msg = make_message(atom::put_v, st.node, type, component, label);
  for (auto& peer : st.registry.components.value) {
    auto& t = peer.second.value.find("tracker")->second.actor;
    if (t != self)
      anon_send(t, msg);
  }
}

// Checks whether a component can be spawned at most once.
bool is_singleton(const std::string& component) {
  const char* singletons[] = {"archive", "importer", "index", "type-registry"};
  auto pred = [&](const char* lhs) { return lhs == component; };
  return std::any_of(std::begin(singletons), std::end(singletons), pred);
}

} // namespace

tracker_type::behavior_type
tracker(tracker_type::stateful_pointer<tracker_state> self, std::string node) {
  self->state.node = node;
  // Insert ourself into the registry.
  self->state.registry.components.value[node].value.emplace(
    "tracker", component_state{actor_cast<actor>(self), "tracker"});
  self->set_down_handler([=](const down_msg& msg) {
    auto pred = [&](auto& p) { return p.second.actor == msg.source; };
    for (auto& [node, comp_state] : self->state.registry.components.value) {
      auto i
        = std::find_if(comp_state.value.begin(), comp_state.value.end(), pred);
      if (i != comp_state.value.end()) {
        if (i->first == "tracker")
          self->state.registry.components.value.erase(node);
        else
          comp_state.value.erase(i);
        return;
      }
    }
  });
  self->set_exit_handler([=](const exit_msg&) {
    // We shut down the components in the order in which data flows so that
    // downstream components can still process in-flight data. Because the
    // terminator operates with a stack of components, we specify them in
    // reverse order.
    auto actors = std::vector<caf::actor>{};
    auto components = std::vector<std::string>{"source", "importer", "archive",
                                               "index", "exporter"};
    auto& local = self->state.registry.components.value[node].value;
    for (auto& component : components) {
      auto er = local.equal_range(component);
      for (auto i = er.first; i != er.second; ++i) {
        self->demonitor(i->second.actor);
        actors.push_back(i->second.actor);
      }
      local.erase(er.first, er.second);
    }
    // Add remaining components.
    for ([[maybe_unused]] auto& [_, comp] : local) {
      if (comp.actor != self) {
        self->demonitor(comp.actor);
        actors.push_back(comp.actor);
      }
    }
    // Avoid no longer needed subscription to DOWN messages.
    self->set_down_handler({});
    local.clear();
    // Perform asynchronous shutdown.
    auto t = self->spawn(terminator<policy::sequential>);
    shutdown(caf::actor_cast<caf::event_based_actor*>(self), t,
             std::move(actors));
  });
  return {
    [=](atom::put, const std::string& type, const actor& component,
        std::string& label) -> result<atom::ok> {
      VAST_DEBUG(self, "got new", type, '(' << label << ')');
      register_component(self, self->state, type, component, label);
      return atom::ok_v;
    },
    [=](atom::try_put, const std::string& type, const actor& component,
        std::string& label) -> result<void> {
      VAST_DEBUG(self, "got new", type, '(' << label << ')');
      auto& st = self->state;
      auto& local = st.registry.components.value[node].value;
      if (is_singleton(type) && local.count(type) > 0)
        return make_error(ec::unspecified, "component already exists");
      register_component(self, st, type, component, label);
      return caf::unit;
    },
    [=](atom::put, const std::string& name, const std::string& type,
        const actor& component, const std::string& label) {
      VAST_DEBUG(self, "got PUT from peer", name, "for", type);
      auto& components = self->state.registry.components.value[name].value;
      components.emplace(type, component_state{component, label});
      self->monitor(component);
    },
    [=](atom::get) -> result<registry> { return self->state.registry; },
    [=](atom::peer, const actor& peer, const std::string& peer_name)
      -> typed_response_promise<atom::state, registry> {
      auto rp = self->make_response_promise<atom::state, registry>();
      if (self->state.registry.components.value.count(peer_name) > 0) {
        VAST_ERROR(self, "peer name already exists", peer_name);
        return rp.deliver(make_error(ec::unspecified, "duplicate node name"));
      }
      VAST_DEBUG(self, "shipping state to new peer", peer_name);
      rp.delegate(peer, atom::state_v, self->state.registry);
      return rp;
    },
    [=](atom::state, registry& reg) -> result<atom::ok> {
      VAST_DEBUG(self, "got state for", reg.components.value.size(), "peers");
      // Monitor all remote components.
      for (auto& peer : reg.components.value)
        for (auto& pair : peer.second.value)
          self->monitor(pair.second.actor);
      // Incorporate new state from peer.
      std::move(reg.components.value.begin(), reg.components.value.end(),
                std::inserter(self->state.registry.components.value,
                              self->state.registry.components.value.end()));
      // Broadcast our own state to all peers, without expecting a reply.
      auto i = self->state.registry.components.value.find(node);
      VAST_ASSERT(i != self->state.registry.components.value.end());
      for (auto& peer : self->state.registry.components.value) {
        auto& t = peer.second.value.find("tracker")->second.actor;
        if (t != self)
          self->send(actor_cast<tracker_type>(t), atom::state_v,
                     component_map_entry{*i});
      }
      return atom::ok_v;
    },
    [=](atom::state, component_map_entry& entry) {
      VAST_DEBUG(self, "got components from new peer");
      for (auto& pair : entry.value.second.value)
        self->monitor(pair.second.actor);
      auto result
        = self->state.registry.components.value.insert(std::move(entry.value));
      VAST_ASSERT(result.second);
    }};
}

} // namespace vast::system

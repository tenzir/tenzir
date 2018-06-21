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

#include "vast/error.hpp"
#include "vast/logger.hpp"

#include "vast/detail/assert.hpp"
#include "vast/detail/string.hpp"

#include "vast/system/archive.hpp"
#include "vast/system/meta_store.hpp"
#include "vast/system/tracker.hpp"

using namespace caf;

namespace vast {
namespace system {

tracker_type::behavior_type
tracker(tracker_type::stateful_pointer<tracker_state> self, std::string node) {
  self->state.registry.components[node].emplace(
    "tracker", component_state{actor_cast<actor>(self), "tracker"});
  self->set_down_handler(
    [=](const down_msg& msg) {
      auto pred = [&](auto& p) { return p.second.actor == msg.source; };
      for (auto& peer : self->state.registry.components) {
        auto i = std::find_if(peer.second.begin(), peer.second.end(), pred);
        if (i != peer.second.end()) {
          if (i->first == "tracker")
            self->state.registry.components.erase(peer.first);
          else
            peer.second.erase(i);
          return;
        }
      }
    }
  );
  self->set_exit_handler(
    [=](const exit_msg& msg) {
      // We shut down the components in the order in which data flows so that
      // downstream components can still process in-flight data. This is much
      // easier to express with a blocking actor than asynchronously with
      // nesting.
      self->set_exit_handler({});
      self->spawn(
        [=, parent=actor_cast<actor>(self),
         local=self->state.registry.components[node]]
        (blocking_actor* terminator) mutable {
          auto shutdown = [&](auto key) {
            auto er = local.equal_range(key);
            for (auto i = er.first; i != er.second; ++i) {
              VAST_DEBUG("sending EXIT to", i->second.label);
              terminator->send_exit(i->second.actor, msg.reason);
              terminator->wait_for(i->second.actor);
            }
            local.erase(er.first, er.second);
          };
          shutdown("source");
          shutdown("importer");
          shutdown("archive");
          shutdown("index");
          shutdown("exporter");
          // For the remaining components, the shutdown order doesn't matter.
          for (auto& pair : local)
            if (pair.first != "tracker") { // happens at the very end
              VAST_DEBUG("sending EXIT to", pair.second.label);
              terminator->send_exit(pair.second.actor, msg.reason);
              terminator->wait_for(pair.second.actor);
            }
          VAST_DEBUG("sending EXIT to tracker");
          terminator->send_exit(parent, msg.reason);
          terminator->wait_for(parent);
          VAST_DEBUG("completed shutdown of all components");
        }
      );
    }
  );
  return {
    [=](put_atom, const std::string& type, const actor& component,
        std::string& label) -> result<ok_atom> {
      VAST_DEBUG(self, "got new", type, '(' << label << ')');
      // Save new component.
      self->monitor(component);
      auto& local = self->state.registry.components[node];
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
          self->anon_send(component, actor_cast<archive_type>(a));
        for (auto& a : actors("index"))
          self->anon_send(component, index_atom::value, a);
        for (auto& a : actors("sink"))
          self->anon_send(component, sink_atom::value, a);
      } else if (type == "importer") {
        for (auto& a : actors("metastore"))
          self->anon_send(component, actor_cast<meta_store_type>(a));
        for (auto& a : actors("archive"))
          self->anon_send(component, actor_cast<archive_type>(a));
        for (auto& a : actors("index"))
          self->anon_send(component, index_atom::value, a);
        for (auto& a : actors("source"))
          self->anon_send(a, sink_atom::value, component);
      } else if (type == "source") {
        for (auto& a : actors("importer"))
          self->anon_send(component, sink_atom::value, a);
      } else if (type == "sink") {
        for (auto& a : actors("exporter"))
          self->anon_send(a, sink_atom::value, component);
      }
      // Propagate new component to peer.
      auto msg = make_message(put_atom::value, node, type, component, label);
      for (auto& peer : self->state.registry.components) {
        auto& t = peer.second.find("tracker")->second.actor;
        if (t != self)
          self->anon_send(t, msg);
      }
      return ok_atom::value;
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

} // namespace system
} // namespace vast

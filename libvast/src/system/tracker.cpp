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
  self->state.components[node].emplace("tracker",
                                       component_state{actor_cast<actor>(self),
                                                       "tracker"});
  //auto qualify = [=](auto str) {
  //  auto split = detail::split_to_str(str, "@");
  //  return split[0] + '@' + (split.size() == 1 ? node : split[1]);
  //};
  self->set_down_handler(
    [=](const down_msg& msg) {
      auto pred = [&](auto& p) { return p.second.actor == msg.source; };
      for (auto& peer : self->state.components) {
        auto i = std::find_if(peer.second.begin(), peer.second.end(), pred);
        if (i != peer.second.end()) {
          if (i->first == "tracker")
            self->state.components.erase(peer.first);
          else
            peer.second.erase(i);
          return;
        }
      }
    }
  );
  self->set_exit_handler(
    [=](const exit_msg& msg) {
      for (auto& pair : self->state.components[node])
        if (pair.first != "tracker")
          self->send_exit(pair.second.actor, msg.reason);
      self->quit(msg.reason);
    }
  );
  return {
    [=](put_atom, const std::string& type, const actor& component,
        std::string& label) -> result<ok_atom> {
      VAST_DEBUG(self, "tracking new", type, '(' << label << ')');
      // Save new component.
      self->monitor(component);
      auto& local = self->state.components[node];
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
      auto msg = make_message(state_atom::value, node, type, component, label);
      for (auto& peer : self->state.components) {
        auto& t = peer.second.find("tracker")->second.actor;
        if (t != self)
          self->anon_send(t, msg);
      }
      return ok_atom::value;
    },
    [=](get_atom) -> result<registry> {
      return self->state.components;
    },
    [=](peer_atom, const actor& peer, const std::string& peer_name)
    -> typed_response_promise<state_atom, registry> {
      auto rp = self->make_response_promise<state_atom, registry>();
      if (self->state.components.count(peer_name) > 0) {
        VAST_ERROR(self, "peer name already exists", peer_name);
        return rp.deliver(make_error(ec::unspecified, "duplicate node name"));
      }
      VAST_DEBUG(self, "shipping state to new peer", peer_name);
      rp.delegate(peer, state_atom::value, self->state.components);
      return rp;
    },
    [=](state_atom, registry& components)
    -> result<ok_atom> {
      VAST_DEBUG(self, "got state for", components.size(), "peers");
      for (auto& peer : components)
        for (auto& pair : peer.second)
          self->monitor(pair.second.actor);
      // Incorporate new state from peer.
      std::move(components.begin(), components.end(),
                std::inserter(self->state.components,
                              self->state.components.end()));
      // Broadcast our own state to all peers, without expecting a reply.
      auto i = self->state.components.find(node);
      VAST_ASSERT(i != self->state.components.end());
      for (auto& peer : self->state.components) {
        auto& t = peer.second.find("tracker")->second.actor;
        if (t != self)
          self->send(actor_cast<tracker_type>(t), state_atom::value, *i);
      }
      return ok_atom::value;
    },
    [=](state_atom, registry_entry& entry) {
      VAST_DEBUG(self, "got components from new peer");
      for (auto& pair : entry.second)
        self->monitor(pair.second.actor);
      auto result = self->state.components.insert(std::move(entry));
      VAST_ASSERT(result.second);
    },
    [=](state_atom, const std::string& name, const std::string& type,
        const actor& component, const std::string& label) {
      VAST_DEBUG(self, "got PUT from peer", name, "for", type);
      auto& reg = self->state.components[name];
      reg.emplace(type, component_state{component, label});
      self->monitor(component);
    },
  };
}

} // namespace system
} // namespace vast

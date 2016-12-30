#include <caf/all.hpp>

#include "vast/error.hpp"
#include "vast/logger.hpp"

#include "vast/detail/assert.hpp"
#include "vast/detail/string.hpp"

#include "vast/system/tracker.hpp"

using namespace caf;

namespace vast {
namespace system {

tracker_type::behavior_type
tracker(tracker_type::stateful_pointer<tracker_state> self, std::string node) {
  self->state.components[node].emplace("tracker", actor_cast<actor>(self));
  //auto qualify = [=](auto str) {
  //  auto split = detail::split_to_str(str, "@");
  //  return split[0] + '@' + (split.size() == 1 ? node : split[1]);
  //};
  self->set_down_handler(
    [=](const down_msg& msg) {
      for (auto& peer : self->state.components)
        for (auto& pair : peer.second)
          if (pair.second == msg.source) {
            if (pair.first == "tracker")
              self->state.components.erase(peer.first);
            else
              peer.second.erase(pair.first);
            return;
          }
    }
  );
  self->set_exit_handler(
    [=](const exit_msg& msg) {
      for (auto& pair : self->state.components[node])
        if (pair.first != "tracker")
          self->send_exit(pair.second, msg.reason);
      self->quit(msg.reason);
    }
  );
  return {
    [=](put_atom, const std::string& type, const actor& component)
    -> result<ok_atom> {
      self->monitor(component);
      self->state.components[node].emplace(type, component);
      // Wire components.
      if (type == "exporter" || type == "importer") {
        // Wire to archive and indexes.
        // TODO
      }
      if (type == "exporter") {
        // Wire to sinks.
        // TODO
      } else if (type == "importer") {
        // Wire to sources.
        // TODO
      } else if (type == "source") {
        // Wire to importers
        // TODO
      } else if (type == "sink") {
        // Wire to exporters.
        // TODO
      }
      auto msg = make_message(state_atom::value, node, type, component);
      for (auto& peer : self->state.components) {
        auto& t = peer.second.find("tracker")->second;
        if (t != self)
          self->anon_send(t, msg);
      }
      return ok_atom::value;
    },
    [=](get_atom) -> result<component_map> {
      return self->state.components;
    },
    [=](peer_atom, const actor& peer, const std::string& peer_name)
    -> typed_response_promise<state_atom, component_map> {
      auto rp = self->make_response_promise<state_atom, component_map>();
      if (self->state.components.count(peer_name) > 0) {
        VAST_ERROR(self, "peer name already exists", peer_name);
        return rp.deliver(make_error(ec::unspecified, "duplicate node name"));
      }
      VAST_DEBUG(self, "shipping state to new peer", peer_name);
      rp.delegate(peer, state_atom::value, self->state.components);
      return rp;
    },
    [=](state_atom, component_map& components)
    -> result<ok_atom> {
      VAST_DEBUG(self, "got state for", components.size(), "peers");
      for (auto& peer : components)
        for (auto& pair : peer.second)
          self->monitor(pair.second);
      // Incorporate new state from peer.
      std::move(components.begin(), components.end(),
                std::inserter(self->state.components,
                              self->state.components.end()));
      // Broadcast our own state to all peers, without expecting a reply.
      auto i = self->state.components.find(node);
      VAST_ASSERT(i != self->state.components.end());
      for (auto& peer : self->state.components) {
        auto& t = peer.second.find("tracker")->second;
        if (t != self)
          self->send(actor_cast<tracker_type>(t), state_atom::value, *i);
      }
      return ok_atom::value;
    },
    [=](state_atom, component_map_entry& entry) {
      VAST_DEBUG(self, "got components from new peer");
      for (auto& pair : entry.second)
        self->monitor(pair.second);
      auto result = self->state.components.insert(std::move(entry));
      VAST_ASSERT(result.second);
    },
    [=](state_atom, const std::string& name, const std::string& type,
        const actor& component) {
      VAST_DEBUG(self, "got PUT from peer", name, "for", type);
      self->state.components[name].emplace(type, component);
      self->monitor(component);
    },
  };
}

} // namespace system
} // namespace vast

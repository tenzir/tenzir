#include <iterator>
#include <fstream>

#include "vast/error.h"
#include "vast/logger.h"
#include "vast/none.h"
#include "vast/actor/atoms.h"
#include "vast/actor/key_value_store.h"
#include "vast/concept/printable/vast/error.h"
#include "vast/concept/printable/vast/filesystem.h"
#include "vast/concept/serializable/io.h"
#include "vast/concept/serializable/caf/message.h"

namespace vast {

key_value_store::state::state(local_actor* self)
  : basic_state{self, "key-value-store"} {
}

behavior key_value_store::make(stateful_actor<state>* self, path dir) {
  auto update = [=](std::string const& key, message const& value) {
    self->state.data[key] = value;
    if (self->state.persistent.find(key) == self->state.persistent.end())
      return true;
    if (! exists(dir)) {
      auto t = mkdir(dir);
      if (! t) {
        VAST_ERROR_AT(self, "failed to create state directory:", t.error());
        return false;
      }
    }
    auto filename = dir / key;
    std::ofstream f{filename.str()};
    // TODO: String serialization would make for a more readable file system
    // representation, but to_string/from_string is currently broken in CAF.
    // f << to_string(value);
    caf::binary_serializer bs{std::ostreambuf_iterator<char>(f)};
    bs << value;
    if (f)
      return true;
    VAST_ERROR_AT(self, "failed to save entry:", key, "->", to_string(value));
    if (exists(filename))
      rm(filename);
    return false;
  };
  // Assigns a value to a given key.
  auto assign = [=](std::string const& key, message const& value) {
    if (! update(key, value))
      return make_message(error{"failed to update entry: ", key});
    return make_message(ok_atom::value);
  };
  // Adds a value to an existing value.
  auto add = [=](std::string const& key, message const& value) {
    auto existing = self->state.data[key];
    if (existing.empty()) {
      auto copy = value;  // FIXME: message::apply() non-const?
      auto unit = copy.apply({
        [](int8_t) -> int8_t { return 0; },
        [](uint8_t) -> uint8_t { return 0; },
        [](int16_t) -> int16_t { return 0; },
        [](uint16_t) -> uint16_t { return 0; },
        [](int32_t) -> int32_t { return 0; },
        [](uint32_t) -> uint32_t { return 0; },
        [](int64_t) -> int64_t  { return 0; },
        [](uint64_t) -> uint64_t { return 0; },
        [](double) -> double { return 0; },
        [](float) -> float { return 0; },
        [](std::string const&) -> std::string { return ""; }
      });
      if (!unit)
        return make_message(error{"unsupported value"});
      self->state.data[key] = value;
      if (! update(key, value))
        return make_message(error{"failed to write entry to filesystem"});
      return *unit + value;
    }
    auto result = (existing + value).apply({
      [](int8_t x, int8_t y) { return x + y; },
      [](uint8_t x, uint8_t y) { return x + y; },
      [](int16_t x, int16_t y) { return x + y; },
      [](uint16_t x, uint16_t y) { return x + y; },
      [](int32_t x, int32_t y) { return x + y; },
      [](uint32_t x, uint32_t y) { return x + y; },
      [](int64_t x, int64_t y) { return x + y; },
      [](uint64_t x, uint64_t y) { return x + y; },
      [](double x, double y) { return x + y; },
      [](float x, float y) { return x + y; },
      [](std::string const& x, std::string const& y) { return x + y; }
    });
    if (!result)
      return make_message(error{"different operand types"});
    if (! update(key, *result))
      return make_message(error{"failed to write entry to filesystem"});
    return existing + *result;
  };
  // Deletes all values prefixed by a given key (and removes associated
  // persistent state).
  auto erase = [=](std::string const& prefix) {
    auto total = uint64_t{0};
    for (auto& pair : self->state.data.prefixed_by(prefix)) {
      auto key = pair->first;
      total += self->state.data.erase(key);
      self->state.persistent.erase(key);
      if (exists(key))
        rm(key);
    }
    return make_message(total);
  };
  // Poor-man's log replication. The current implementation merely pushes the
  // "log" (which is the current message) to the remote stores.
  // TODO: Refactor the replication and peering aspects into a seperate raft
  // consensus module and orthogonalize them to the key-value store
  // implementation.
  auto replicate = [=](response_promise& rp, auto f, auto&&... xs) {
    if (self->state.followers.empty()) {
      VAST_DEBUG_AT(self, "replicates entry locally");
      rp.deliver(f(xs...));
    } else {
      VAST_DEBUG_AT(self, "replicates entry to", self->state.followers.size(),
                    "follower(s)");
      for (auto& follower : self->state.followers)
        self->send(follower, self->current_message());
      auto num_acks = std::make_shared<size_t>(self->state.followers.size());
      self->become(
        keep_behavior,
        [=](down_msg const& msg) {
          VAST_DEBUG_AT(self, "got DOWN from follower", msg.source);
          auto n = self->state.followers.erase(actor_cast<actor>(msg.source));
          VAST_ASSERT(n > 0);
          if (--*num_acks == 0) {
            VAST_DEBUG_AT(self, "completed follower replication");
            rp.deliver(f(xs...));
            self->unbecome();
          }
        },
        [=](replicate_atom, ok_atom) {
          // Technically, we only require the majority to get back to us.
          // Because don't have terms and batched AppendEntries yet, we use
          // slightly stronger requirements.
          if (--*num_acks == 0) {
            VAST_DEBUG_AT(self, "completed follower replication");
            rp.deliver(f(xs...));
            self->unbecome();
          }
        }
      );
    }
  };
  // Load existing persistent values.
  for (auto entry : directory{dir}) {
    auto key = entry.basename().str();
    std::ifstream f{entry.str()};
    std::string contents{std::istreambuf_iterator<char>{f},
                         std::istreambuf_iterator<char>{}};
    if (! f) {
      VAST_ERROR_AT(self, "failed to read contents of file:", entry);
      self->quit(exit::error);
      return {};
    }
    // TODO: String serialization would make for a more readable file system
    // representation, but to_string/from_string is currently broken in CAF.
    // auto value = from_string<message>(contents);
    caf::binary_deserializer bs{contents.data(), contents.size()};
    message value;
    bs >> value;
    VAST_DEBUG_AT(self, "loaded persistent key:", key, "->", to_string(value));
    self->state.persistent[key] = {};
    self->state.data[key] = value;
  }
  behavior candidating = {
    others >> [=] {
      VAST_ERROR("leader election not yet implemented");
      self->quit(exit::error);
    }
  };
  auto leading = std::make_shared<behavior>();
  behavior following = {
    [=](leader_atom) {
      // Because we don't have implemented leader election yet, we use an
      // external vote to unconditionally promote followers to leaders.
      VAST_DEBUG_AT(self, "changes state: follower -> leader");
      self->become(*leading);
    },
    [=](down_msg const& msg) {
      VAST_DEBUG_AT(self, "got DOWN from leader");
      VAST_DEBUG_AT(self, "changes state: follower -> candidate");
      VAST_ASSERT(msg.source == self->state.leader->address());
      self->state.leader = invalid_actor;
      self->become(candidating);
    },
    [=](exists_atom, std::string const& key) {
      VAST_DEBUG_AT(self, "forwards EXISTS to leader:", key);
      self->forward_to(self->state.leader);
    },
    [=](get_atom, std::string const& key) {
      VAST_DEBUG_AT(self, "forwards GET to leader:", key);
      self->forward_to(self->state.leader);
    },
    [=](list_atom, std::string const& key) {
      VAST_DEBUG_AT(self, "forwards LIST to leader:", key);
      self->forward_to(self->state.leader);
    },
    on(put_atom::value, val<std::string>, any_vals) >> [=](std::string const&
                                                           key) {
      auto value = self->current_message().drop(2);
      if (self->current_sender() != self->state.leader->address()) {
        VAST_DEBUG_AT(self, "forwards PUT:", key, "->", to_string(value));
        self->forward_to(self->state.leader);
      } else {
        VAST_DEBUG_AT(self, "replicates PUT:", key, "->", to_string(value));
        assign(key, value);
        self->send(self->state.leader, replicate_atom::value, ok_atom::value);
      }
    },
    on(add_atom::value, val<std::string>, any_vals) >> [=](std::string const&
                                                           key) {
      auto value = self->current_message().drop(2);
      if (self->current_sender() != self->state.leader->address()) {
        VAST_DEBUG_AT(self, "forwards ADD:", key, "+=", to_string(value));
        self->forward_to(self->state.leader);
      } else {
        VAST_DEBUG_AT(self, "replicates ADD:", key, "+=", to_string(value));
        add(key, value);
        self->send(self->state.leader, replicate_atom::value, ok_atom::value);
      }
    },
    [=](delete_atom, std::string const& key) {
      if (self->current_sender() != self->state.leader->address()) {
        VAST_DEBUG_AT(self, "forwards DELETE:", key);
        self->forward_to(self->state.leader);
      } else {
        VAST_DEBUG_AT(self, "replicates DELETE:", key);
        erase(key);
        self->send(self->state.leader, replicate_atom::value, ok_atom::value);
      }
    },
    [=](announce_atom, actor const& leader, storage& data) {
      VAST_DEBUG_AT(self, "got state from leader");
      self->state.leader = leader;
      self->monitor(self->state.leader);
      // Send back the state difference A - B, with local follower A
      // and leader B.
      storage delta;
      for (auto& pair : self->state.data)
        if (data.find(pair.first) == data.end())
          delta[pair.first] = pair.second;
      self->state.data = std::move(data);
      return make_message(announce_atom::value, ok_atom::value,
                          std::move(delta));
    },
    log_others(self)
  };
  *leading = {
    [=](down_msg const& msg) {
      VAST_DEBUG_AT(self, "got DOWN from follower", msg.source);
      self->state.followers.erase(actor_cast<actor>(msg.source));
    },
    [=](follower_atom) {
      VAST_DEBUG_AT(self, "changes state: leader -> follower");
      self->become(following);
    },
    [=](follower_atom, add_atom, actor const& follower) {
      auto rp = self->make_response_promise();
      VAST_DEBUG_AT(self, "got request to add new follower", follower);
      // If we know self follower already, we have nothing to do.
      if (self->state.followers.find(follower) != self->state.followers.end()) {
        VAST_WARN_AT(self, "ignores existing follower");
        rp.deliver(make_message(ok_atom::value));
        return;
      }
      VAST_DEBUG_AT(self, "relays", self->state.data.size(),
                 "entries to follower");
      self->send(follower, announce_atom::value, self, self->state.data);
      self->become(
        keep_behavior,
        [=](announce_atom, ok_atom, storage const& delta) {
          VAST_DEBUG_AT(self, "got", delta.size(), "new entries from follower");
          self->monitor(follower);
          self->state.followers.insert(follower);
          rp.deliver(self->current_message().drop(1));
          self->unbecome();
        }
      );
    },
    [=](exists_atom, std::string const& key) {
      VAST_DEBUG_AT(self, "got EXISTS:", key);
      return !self->state.data.prefixed_by(key).empty();
    },
    [=](get_atom, std::string const& key) {
      VAST_DEBUG_AT(self, "got GET for key:", key);
      auto v = self->state.data.find(key);
      if (v == self->state.data.end())
        return make_message(nil);
      return v->second.empty() ? make_message(unit) : v->second;
    },
    [=](list_atom, std::string const& key) {
      VAST_DEBUG_AT(self, "got LIST:", key);
      std::map<std::string, message> result;
      if (!key.empty()) {
        auto values = self->state.data.prefixed_by(key);
        for (auto& v : values)
          result.emplace(v->first, v->second);
      }
      return make_message(std::move(result));
    },
    on(put_atom::value, val<std::string>, any_vals) >> [=](std::string const&
                                                           key) {
      auto rp = self->make_response_promise();
      auto value = self->current_message().drop(2);
      VAST_DEBUG_AT(self, "got PUT:", key, "->", to_string(value));
      replicate(rp, assign, key, value);
    },
    on(add_atom::value, val<std::string>, any_vals) >> [=](std::string const&
                                                           key) {
      auto rp = self->make_response_promise();
      auto value = self->current_message().drop(2);
      VAST_DEBUG_AT(self, "got ADD:", key, "+=", to_string(value));
      replicate(rp, add, key, value);
    },
    [=](delete_atom, std::string const& key) {
      auto rp = self->make_response_promise();
      VAST_DEBUG_AT(self, "got DELETE:", key);
      replicate(rp, erase, key);
    },
    [=](persist_atom, std::string const& key) {
      VAST_DEBUG_AT(self, "marks key as persistent:", key);
      self->state.persistent[key] = {};
    },
    log_others(self)
  };
  return following;
}

} // namespace vast

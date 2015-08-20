#include <fstream>

#include <caf/all.hpp>

#include "vast/error.h"
#include "vast/logger.h"
#include "vast/none.h"
#include "vast/actor/key_value_store.h"
#include "vast/concept/printable/vast/error.h"
#include "vast/concept/printable/vast/filesystem.h"
#include "vast/concept/serializable/io.h"
#include "vast/concept/serializable/caf/message.h"

namespace vast {

key_value_store::key_value_store(path dir)
  : default_actor{"key-value-store"},
    dir_{std::move(dir)} {
}

void key_value_store::on_exit() {
  leader_ = invalid_actor;
  followers_.clear();
}

behavior key_value_store::make_behavior() {
  // Assigns a value to a given key.
  auto assign = [=](std::string const& key, message const& value) {
    if (! update(key, value))
      return make_message(error{"failed to update entry: ", key});
    return make_message(ok_atom::value);
  };
  // Adds a value to an existing value.
  auto add = [=](std::string const& key, message const& value) {
    auto existing = data_[key];
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
      data_[key] = value;
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
    for (auto& pair : data_.prefixed_by(prefix)) {
      auto key = pair->first;
      total += data_.erase(key);
      persistent_.erase(key);
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
    if (followers_.empty()) {
      VAST_DEBUG(this, "replicates entry locally");
      rp.deliver(f(xs...));
    } else {
      VAST_DEBUG(this, "replicates entry to", followers_.size(), "follower(s)");
      for (auto& follower : followers_)
        send(follower, current_message());
      auto num_acks = std::make_shared<size_t>(followers_.size());
      become(
        keep_behavior,
        [=](down_msg const& msg) {
          VAST_DEBUG(this, "got DOWN from follower", msg.source);
          auto n = followers_.erase(actor_cast<actor>(msg.source));
          VAST_ASSERT(n > 0);
          if (--*num_acks == 0) {
            VAST_DEBUG(this, "completed follower replication");
            rp.deliver(f(xs...));
            unbecome();
          }
        },
        [=](replicate_atom, ok_atom) {
          // Technically, we only require the majority to get back to us.
          // Because don't have terms and batched AppendEntries yet, we use
          // slightly stronger requirements.
          if (--*num_acks == 0) {
            VAST_DEBUG(this, "completed follower replication");
            rp.deliver(f(xs...));
            unbecome();
          }
        }
      );
    }
  };
  // Load existing persistent values.
  for (auto entry : directory{dir_}) {
    auto key = entry.basename().str();
    std::ifstream f{entry.str()};
    f.unsetf(std::ios_base::skipws);
    std::string str;
    f >> str;
    if (! f) {
      VAST_ERROR(this, "failed to read contents of file:", entry);
      quit(exit::error);
      return {};
    }
    // TODO: String serialization would make for a more readable file system
    // representation, but to_string/from_string is currently broken in CAF.
    // auto value = from_string<message>(str);
    caf::binary_deserializer bs{str.data(), str.size()};
    message value;
    bs >> value;
    VAST_DEBUG(this, "loaded persistent key:", key, "->", to_string(value));
    persistent_[key] = {};
    data_[key] = value;
  }
  following_ = {
    [=](leader_atom) {
      // Because we don't have implemented leader election yet, we use an
      // external vote to unconditionally promote followers to leaders.
      VAST_DEBUG(this, "changes state: follower -> leader");
      become(leading_);
    },
    [=](down_msg const& msg) {
      VAST_DEBUG(this, "got DOWN from leader");
      VAST_DEBUG(this, "changes state: follower -> candidate");
      VAST_ASSERT(msg.source == leader_->address());
      leader_ = invalid_actor;
      become(candidating_);
    },
    [=](exists_atom, std::string const& key) {
      VAST_DEBUG(this, "forwards EXISTS to leader:", key);
      forward_to(leader_);
    },
    [=](get_atom, std::string const& key) {
      VAST_DEBUG(this, "forwards GET to leader:", key);
      forward_to(leader_);
    },
    [=](list_atom, std::string const& key) {
      VAST_DEBUG(this, "forwards LIST to leader:", key);
      forward_to(leader_);
    },
    on(put_atom::value, val<std::string>, any_vals) >> [=](std::string const&
                                                           key) {
      auto value = current_message().drop(2);
      if (current_sender() != leader_->address()) {
        VAST_DEBUG(this, "forwards PUT:", key, "->", to_string(value));
        forward_to(leader_);
      } else {
        VAST_DEBUG(this, "replicates PUT:", key, "->", to_string(value));
        assign(key, value);
        send(leader_, replicate_atom::value, ok_atom::value);
      }
    },
    on(add_atom::value, val<std::string>, any_vals) >> [=](std::string const&
                                                           key) {
      auto value = current_message().drop(2);
      if (current_sender() != leader_->address()) {
        VAST_DEBUG(this, "forwards ADD:", key, "+=", to_string(value));
        forward_to(leader_);
      } else {
        VAST_DEBUG(this, "replicates ADD:", key, "+=", to_string(value));
        add(key, value);
        send(leader_, replicate_atom::value, ok_atom::value);
      }
    },
    [=](delete_atom, std::string const& key) {
      if (current_sender() != leader_->address()) {
        VAST_DEBUG(this, "forwards DELETE:", key);
        forward_to(leader_);
      } else {
        VAST_DEBUG(this, "replicates DELETE:", key);
        erase(key);
        send(leader_, replicate_atom::value, ok_atom::value);
      }
    },
    [=](announce_atom, actor const& leader, storage& data) {
      VAST_DEBUG(this, "got state from leader");
      leader_ = leader;
      monitor(leader_);
      // Send back the state difference A - B, with local follower A
      // and leader B.
      storage delta;
      for (auto& pair : data_)
        if (data.find(pair.first) == data.end())
          delta[pair.first] = pair.second;
      data_ = std::move(data);
      return make_message(announce_atom::value, ok_atom::value,
                          std::move(delta));
    },
    catch_unexpected()
  };
  candidating_ = {
    others >> [=] {
      VAST_ERROR("leader election not yet implemented");
      quit(exit::error);
    }
  };
  leading_ = {
    [=](down_msg const& msg) {
      VAST_DEBUG(this, "got DOWN from follower", msg.source);
      followers_.erase(actor_cast<actor>(msg.source));
    },
    [=](follower_atom) {
      VAST_DEBUG(this, "changes state: leader -> follower");
      become(following_);
    },
    [=](follower_atom, add_atom, actor const& new_follower) {
      auto rp = make_response_promise();
      VAST_DEBUG(this, "got request to add new follower", new_follower);
      // If we know this follower already, we have nothing to do.
      if (followers_.find(new_follower) != followers_.end()) {
        VAST_WARN(this, "ignores existing follower");
        rp.deliver(make_message(ok_atom::value));
        return;
      }
      VAST_DEBUG(this, "relays", data_.size(), "entries to follower");
      send(new_follower, announce_atom::value, this, data_);
      become(
        keep_behavior,
        [=](announce_atom, ok_atom, storage const& delta) {
          VAST_DEBUG(this, "got", delta.size(), "new entries from follower");
          monitor(new_follower);
          followers_.insert(new_follower);
          rp.deliver(current_message().drop(1));
          unbecome();
        }
      );
    },
    [=](exists_atom, std::string const& key) {
      VAST_DEBUG(this, "got EXISTS:", key);
      return !data_.prefixed_by(key).empty();
    },
    [=](get_atom, std::string const& key) {
      VAST_DEBUG(this, "got GET for key:", key);
      auto v = data_.find(key);
      if (v == data_.end())
        return make_message(nil);
      return v->second.empty() ? make_message(unit) : v->second;
    },
    [=](list_atom, std::string const& key) {
      VAST_DEBUG(this, "got LIST:", key);
      std::map<std::string, message> result;
      if (!key.empty()) {
        auto values = data_.prefixed_by(key);
        for (auto& v : values)
          result.emplace(v->first, v->second);
      }
      return make_message(std::move(result));
    },
    on(put_atom::value, val<std::string>, any_vals) >> [=](std::string const&
                                                           key) {
      auto rp = make_response_promise();
      auto value = current_message().drop(2);
      VAST_DEBUG(this, "got PUT:", key, "->", to_string(value));
      replicate(rp, assign, key, value);
    },
    on(add_atom::value, val<std::string>, any_vals) >> [=](std::string const&
                                                           key) {
      auto rp = make_response_promise();
      auto value = current_message().drop(2);
      VAST_DEBUG(this, "got ADD:", key, "+=", to_string(value));
      replicate(rp, add, key, value);
    },
    [=](delete_atom, std::string const& key) {
      auto rp = make_response_promise();
      VAST_DEBUG(this, "got DELETE:", key);
      replicate(rp, erase, key);
    },
    [=](persist_atom, std::string const& key) {
      VAST_DEBUG(this, "marks key as persistent:", key);
      persistent_[key] = {};
    },
    catch_unexpected()
  };
  return following_;
}

bool key_value_store::update(std::string const& key, message const& value) {
  data_[key] = value;
  if (persistent_.find(key) == persistent_.end())
    return true;
  if (! exists(dir_)) {
    auto t = mkdir(dir_);
    if (! t) {
      VAST_ERROR(this, "failed to create state directory:", t.error());
      return false;
    }
  }
  auto filename = dir_ / key;
  std::ofstream f{filename.str()};
  // TODO: String serialization would make for a more readable file system
  // representation, but to_string/from_string is currently broken in CAF.
  // f << to_string(value);
  caf::binary_serializer bs{std::ostreambuf_iterator<char>(f)};
  bs << value;
  if (f)
    return true;
  VAST_ERROR(this, "failed to save entry:", key, "->", to_string(value));
  if (exists(filename))
    rm(filename);
  return false;
}

} // namespace vast

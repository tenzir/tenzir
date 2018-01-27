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

#ifndef VAST_SYSTEM_REPLICATED_STORE_HPP
#define VAST_SYSTEM_REPLICATED_STORE_HPP

#include <cstdint>
#include <vector>

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>

#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/none.hpp"

#include "vast/system/consensus.hpp"
#include "vast/system/key_value_store.hpp"
#include "vast/system/timeouts.hpp"

#include "vast/detail/assert.hpp"
#include "vast/detail/operators.hpp"

namespace vast::system {

template <class Key, class Value>
struct replicated_store_state {
  // -- persistent state ----------------------
  std::unordered_map<Key, Value> store;
  raft::index_type last_applied = 0;
  uint64_t last_snapshot_size = 0;
  // -- volatile state ------------------------
  uint64_t request_id = 0;
  std::unordered_map<uint64_t, caf::response_promise> requests;
  std::chrono::steady_clock::time_point last_stats_update;
  static inline const char* name = "replicated-store";
};

template <class Inspector, class Key, class Value>
auto inspect(Inspector& f, replicated_store_state<Key, Value>& state) {
  return f(state.store, state.last_applied, state.last_snapshot_size);
}

template <class Key, class Value>
using replicated_store_type =
  class key_value_store_type<Key, Value>::template extend<
    caf::replies_to<snapshot_atom>::template with<ok_atom>,
    caf::reacts_to<raft::index_type, caf::message>
  >;

// FIXME: Make it possible to deserialize caf::actor_addr. This semantically
// equivalent structure is a workaround for the lack of persistence of
// caf::actor_addr, which currently cannot be deserialized. But since we embed
// the actor identity in every (persistent) operation, we need this auxiliary
// type.
class actor_identity
  : detail::equality_comparable<actor_identity>,
    detail::equality_comparable<actor_identity, caf::actor_addr>,
    detail::equality_comparable<caf::actor_addr, actor_identity> {
public:
  actor_identity() = default;

  explicit actor_identity(const caf::actor_addr& addr)
    : node_{addr.node()},
      id_{addr.id()} {
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, actor_identity& ai) {
    return f(ai.node_, ai.id_);
  }

  friend bool operator==(const actor_identity& x, const actor_identity& y) {
    return x.node_ == y.node_ && x.id_ == y.id_;
  }

  friend bool operator==(const actor_identity& x, const caf::actor_addr& y) {
    return x.node_ == y.node() && x.id_ == y.id();
  }

  friend bool operator==(const caf::actor_addr& x, const actor_identity& y) {
    return y == x;
  }

private:
  caf::node_id node_;
  caf::actor_id id_;
};

namespace detail {

template <class Actor>
auto apply(Actor* self, caf::message& operation) {
  using key_type = typename decltype(self->state.store)::key_type;
  using value_type = typename decltype(self->state.store)::mapped_type;
  return *operation.apply({
    [=](put_atom, const key_type& key, value_type& value) {
      VAST_DEBUG(self, "applies PUT");
      self->state.store[key] = std::move(value);
      return ok_atom::value;
    },
    [=](add_atom, const key_type& key, const value_type& value) {
      VAST_DEBUG(self, "applies ADD");
      auto old = self->state.store[key];
      self->state.store[key] += value;
      return old;
    },
    [=](delete_atom, const key_type& key) {
      VAST_DEBUG(self, "applies DELETE");
      self->state.store.erase(key);
      return ok_atom::value;
    },
  });
}

// Applies a mutable operation coming from the consensus module.
template <class Actor>
void update(Actor* self, caf::message& command) {
  command.apply({
    [=](const actor_identity& identity, uint64_t id, caf::message operation) {
      if (identity != self->address()) {
        VAST_IGNORE_UNUSED(id);
        VAST_DEBUG(self, "got remote operation", id);
        apply(self, operation);
      } else {
        VAST_DEBUG(self, "got local operation", id);
        auto id = command.get_as<uint64_t>(1);
        auto i = self->state.requests.find(id);
        if (i != self->state.requests.end()) {
          i->second.deliver(apply(self, operation));
          self->state.requests.erase(i);
        }
      }
    },
    [=](snapshot_atom, raft::index_type, const std::vector<char>& data) {
      VAST_DEBUG(self, "applies snapshot");
      caf::binary_deserializer bd{self->system(), data};
      bd >> self->state;
      self->state.last_snapshot_size = data.size();
      return ok_atom::value;
    }
  });
}

// Replicates the current message through the consensus module.
template <class Actor>
void replicate(Actor* self, const caf::actor& consensus,
               caf::response_promise rp) {
  auto operation = self->current_mailbox_element()->move_content_to_message();
  auto id = ++self->state.request_id;
  self->state.requests.emplace(id, rp);
  auto msg = make_message(actor_identity{self->address()}, id, operation);
  self->request(consensus, consensus_timeout, replicate_atom::value, msg).then(
    [=](ok_atom) {
      VAST_DEBUG(self, "submitted operation", id);
    },
    [=](error& e) mutable {
      rp.deliver(std::move(e));
      self->state.requests.erase(id);
    }
  );
}

} // namespace detail

/// A replicated key-value store that sits on top of a consensus module.
/// @param self The actor handle.
/// @param consensus The consensus module.
// FIXME: The implementation currently does *not* guarantee linearizability.
// Consider the case when the store crashes it has successfully submitted a log
// entry to the consensus module but before returning to the client. The client
// will then get an error and may try again, resulting in the same command
// being applied twice.
// The fix involves filtering out duplicate commands by associating unique
// sequence numbers with client commands, turning at-least-once into
// exactly-once semantics.
template <class Key, class Value>
typename replicated_store_type<Key, Value>::behavior_type
replicated_store(
  class replicated_store_type<Key, Value>::template stateful_pointer<
    replicated_store_state<Key, Value>
  > self,
  caf::actor consensus) {
  using namespace caf;
  self->monitor(consensus);
  self->anon_send(consensus, subscribe_atom::value, actor_cast<actor>(self));
  // Takes a snapshot at the currently applied index.
  auto make_snapshot = [=] {
    VAST_ASSERT(self->state.last_applied > 0);
    std::vector<char> state;
    binary_serializer bs{self->system(), state};
    bs << self->state;
    VAST_DEBUG(self, "serialized", state.size(), "bytes");
    return state;
  };
  self->set_down_handler(
    [=](const down_msg& msg) {
      VAST_ASSERT(msg.source == consensus);
      VAST_DEBUG(self, "got DOWN from consensus module");
      // Abort outstdanding requests.
      for (auto& rp : self->state.requests)
        rp.second.deliver(make_error(ec::unspecified, "consensus module down"));
      self->quit(msg.reason);
    }
  );
  self->set_exit_handler(
    [=](const exit_msg& msg) {
      // Abort outstdanding requests.
      for (auto& rp : self->state.requests)
        rp.second.deliver(msg.reason);
      self->quit(msg.reason);
    }
  );
  return {
    // Linearizability: all writes go through the consensus module.
    [=](put_atom, const Key&, const Value&) {
      VAST_DEBUG(self, "replicates PUT");
      auto rp = self->template make_response_promise<ok_atom>();
      detail::replicate(self, consensus, rp);
      return rp;
    },
    [=](add_atom, const Key&, const Value&) {
      VAST_DEBUG(self, "replicates ADD");
      auto rp = self->template make_response_promise<Value>();
      detail::replicate(self, consensus, rp);
      return rp;
    },
    [=](delete_atom, const Key&) {
      VAST_DEBUG(self, "replicates DELETE");
      auto rp = self->template make_response_promise<ok_atom>();
      detail::replicate(self, consensus, rp);
      return rp;
    },
    // Sequential consistency: all reads may be stale since we're not going
    // through the consensus module. (For linearizability, we would have to go
    // through the leader.)
    [=](get_atom, const Key& key) -> result<optional<Value>> {
      auto i = self->state.store.find(key);
      if (i == self->state.store.end())
        return nil;
      return i->second;
    },
    [=](raft::index_type index, message& operation) {
      using namespace std::chrono_literals;
      VAST_DEBUG(self, "applies entry", index, "(consensus update)");
      detail::update(self, operation);
      self->state.last_applied = index;
      auto now = std::chrono::steady_clock::now();
      if (now - self->state.last_stats_update < 10s)
        return;
      VAST_DEBUG(self, "gathers statistics");
      self->state.last_stats_update = now;
      self->request(consensus, consensus_timeout, statistics_atom::value).then(
        [=](const raft::statistics& stats) {
          auto low = uint64_t{64} << 20;
          auto high = self->state.last_snapshot_size * 4;
          if (stats.log_bytes > std::max(low, high))
            self->anon_send(self, snapshot_atom::value);
        }
      );
    },
    [=](snapshot_atom) {
      VAST_DEBUG(self, "takes snapshot at index", self->state.last_applied);
      auto rp = self->template make_response_promise<ok_atom>();
      auto snapshot = make_snapshot();
      auto snapshot_size = snapshot.size();
      self->request(consensus, consensus_timeout, snapshot_atom::value,
                    self->state.last_applied, std::move(snapshot)).then(
        [=](raft::index_type) mutable {
          VAST_DEBUG(self, "successfully snapshotted state");
          self->state.last_snapshot_size = snapshot_size;
          rp.deliver(ok_atom::value);
        },
        [=](error& e) mutable {
          VAST_ERROR(self, "failed to snapshot:", self->system().render(e));
          rp.deliver(std::move(e));
        }
      );
      return rp;
    }
  };
}

} // namespace vast::system

#endif

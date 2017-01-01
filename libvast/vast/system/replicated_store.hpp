#ifndef VAST_SYSTEM_REPLICATED_STORE_HPP
#define VAST_SYSTEM_REPLICATED_STORE_HPP

#include "vast/system/consensus.hpp"
#include "vast/system/key_value_store.hpp"

namespace vast {
namespace system {

template <class Key, class Value>
struct replicated_store_state {
  raft::index_type last_applied = 0;
  std::unordered_map<Key, Value> store;
  const char* name = "replicated-store";
};

/// A replicated key-value store that sits on top of a consensus module.
/// @param self The actor handle.
/// @param consensus The consensus module.
/// @param store The actor handle.
/// @param self The actor handle.
// FIXME: The implementation currently does *not* guarantee linearizability.
// Consider the case when the store crashes it has successfully submitted a log
// entry to the consensus module but before returning to the client. The client
// will then get an error and may try again, resulting in the same command
// being applied twice.
// The fix involves filtering out duplicate commands by associating unique
// sequence numbers with client commands, turning at-least-once into
// exactly-once semantics.
template <class Key, class Value>
typename key_value_store_type<Key, Value>::behavior_type
replicated_store(
  typename key_value_store_type<Key, Value>::template stateful_pointer<
    replicated_store_state<Key, Value>
  > self,
  caf::actor consensus,
  std::chrono::milliseconds timeout) {
  // Send the current command/message to the consensus module, and once it has
  // been replicated, apply the command to the local state.
  auto replicate = [=](auto rp, auto apply) {
    auto msg = self->current_mailbox_element()->move_content_to_message();
    self->request(consensus, timeout, replicate_atom::value, msg).then(
      [=](ok_atom, raft::index_type index) mutable {
        self->state.last_applied = index;
        rp.deliver(apply(index, msg));
      },
      [=](error& e) mutable {
        rp.deliver(std::move(e));
      }
    );
    return rp;
  };
  return {
    [=](put_atom, const Key&, Value&) {
      return replicate(
        self->template make_response_promise<ok_atom>(),
        [=](raft::index_type, caf::message& msg) {
          auto& key = msg.get_as<Key>(1);
          auto& value = msg.get_as<Value>(2);
          self->state.store[key] = value;
          return ok_atom::value;
        }
      );
    },
    [=](add_atom, const Key& key, const Value&) {
      auto& old = self->state.store[key];
      return replicate(
        self->template make_response_promise<Value>(),
        [=](raft::index_type, caf::message& msg) {
          auto& key = msg.get_as<Key>(1);
          auto& value = msg.get_as<Value>(2);
          self->state.store[key] += value;
          return old;
        }
      );
    },
    [=](delete_atom, const Key&) {
      return replicate(
        self->template make_response_promise<ok_atom>(),
        [=](raft::index_type, caf::message& msg) {
          auto& key = msg.get_as<Key>(1);
          self->state.store.erase(key);
          return ok_atom::value;
        }
      );
    },
    [=](get_atom, const Key& key) -> caf::result<optional<Value>> {
      auto i = self->state.store.find(key);
      if (i == self->state.store.end())
        return nil;
      return i->second;
    }
  };
}

} // namespace system
} // namespace vast

#endif

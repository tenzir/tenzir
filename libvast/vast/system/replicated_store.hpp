#ifndef VAST_SYSTEM_REPLICATED_STORE_HPP
#define VAST_SYSTEM_REPLICATED_STORE_HPP

#include "vast/system/consensus.hpp"
#include "vast/system/key_value_store.hpp"

namespace vast {
namespace system {

template <class Key, class Value>
struct replicated_store_state {
  std::unordered_map<Key, Value> store;
  const char* name = "replicated-store";
};

/// A replicated key-value store that sits on top of a consensus module.
/// @param self The actor handle.
/// @param consensus The consensus module.
/// @param store The actor handle.
/// @param self The actor handle.
template <class Key, class Value>
typename key_value_store_type<Key, Value>::behavior_type
replicated_store(
  typename key_value_store_type<Key, Value>::template stateful_pointer<
    replicated_store_state<Key, Value>
  > self,
  caf::actor consensus,
  std::chrono::milliseconds timeout) {
  // Wrapper function to interact with the consensus module.
  auto replicate = [=](auto rp, auto apply) {
    auto msg = self->current_mailbox_element()->move_content_to_message();
    self->request(consensus, timeout, replicate_atom::value, msg).then(
      [=](ok_atom, raft::index_type index) mutable {
        rp.deliver(apply(index, msg));
      },
      [=](error& e) mutable {
        rp.deliver(std::move(e));
      }
    );
    return rp;
  };
  using ok_promise = caf::typed_response_promise<ok_atom>;
  using add_promise = caf::typed_response_promise<Value>;
  return {
    [=](put_atom, const Key&, Value&) {
      return replicate(
        self->template make_response_promise<ok_promise>(),
        [=](raft::index_type, caf::message& msg) {
          auto& key = msg.get_as<Key>(1);
          auto& value = msg.get_as<Value>(2);
          self->state.store[key] = value;
          return ok_atom::value;
        }
      );
    },
    [=](add_atom, const Key& key, const Value&) {
      auto old = self->state.store[key];
      return replicate(
        self->template make_response_promise<add_promise>(),
        [self, x=std::move(old)](raft::index_type, caf::message& msg) {
          auto& key = msg.get_as<Key>(1);
          auto& value = msg.get_as<Value>(2);
          self->state.store[key] += value;
          return x;
        }
      );
    },
    [=](delete_atom, const Key&) {
      return replicate(
        self->template make_response_promise<ok_promise>(),
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

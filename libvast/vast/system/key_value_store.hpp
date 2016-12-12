#ifndef VAST_SYSTEM_KEY_VALUE_STORE_HPP
#define VAST_SYSTEM_KEY_VALUE_STORE_HPP

#include <unordered_map>

#include <caf/stateful_actor.hpp>
#include <caf/replies_to.hpp>
#include <caf/typed_actor.hpp>

#include "vast/data.hpp"
#include "vast/error.hpp"
#include "vast/optional.hpp"

#include "vast/system/atoms.hpp"

namespace vast {
namespace system {

template <class Key, class Value>
using key_value_store_type = caf::typed_actor<
  // Updates the value of a specific key.
  typename caf::replies_to<put_atom, Key, Value>::template with<ok_atom>,
  // Adds a value to a specific key and returns old key.
  typename caf::replies_to<add_atom, Key, Value>::template with<Value>,
  // Deletes a key-value pair.
  typename caf::replies_to<delete_atom, Key>::template with<ok_atom>,
  // Retrieves the value for a given key pair.
  typename caf::replies_to<get_atom, Key>::template with<optional<Value>>
>;

template <class Key, class Value>
struct data_store_state {
  std::unordered_map<Key, Value> store;
  const char* name = "data-store";
};

/// A key-value store that stores its data in a `std::unordered_map`.
/// @param self The actor handle.
template <class Key, class Value>
typename key_value_store_type<Key, Value>::behavior_type
data_store(
  typename key_value_store_type<Key, Value>::template stateful_pointer<
    data_store_state<Key, Value>
  > self) {
  return {
    [=](put_atom, const Key& key, Value& value) {
      self->state.store[key] = std::move(value);
      return ok_atom::value;
    },
    [=](add_atom, const Key& key, const Value& value) -> caf::result<Value> {
      auto& v = self->state.store[key];
      auto old = v;
      v += value;
      return old;
    },
    [=](delete_atom, const Key& key) {
      self->state.store.erase(key);
      return ok_atom::value;
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

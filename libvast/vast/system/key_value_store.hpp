#ifndef VAST_SYSTEM_KEY_VALUE_STORE_HPP
#define VAST_SYSTEM_KEY_VALUE_STORE_HPP

#include <caf/stateful_actor.hpp>
#include <caf/replies_to.hpp>
#include <caf/typed_actor.hpp>

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

} // namespace system
} // namespace vast

#endif

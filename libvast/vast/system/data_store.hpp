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

#ifndef VAST_SYSTEM_DATA_STORE_HPP
#define VAST_SYSTEM_DATA_STORE_HPP

#include <unordered_map>

#include "vast/data.hpp"

#include "vast/system/key_value_store.hpp"

namespace vast::system {

template <class Key, class Value>
struct data_store_state {
  std::unordered_map<Key, Value> store;
  static inline const char* name = "data-store";
};

/// A key-value store that stores its data in a `std::unordered_map`.
/// @param self The actor handle.
template <class Key, class Value>
typename key_value_store_type<Key, Value>::behavior_type
data_store(
  class key_value_store_type<Key, Value>::template stateful_pointer<
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

} // namespace vast::system

#endif


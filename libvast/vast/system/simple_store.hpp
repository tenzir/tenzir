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

#pragma once

#include <unordered_map>

#include <caf/none.hpp>

#include "vast/data.hpp"
#include "vast/filesystem.hpp"
#include "vast/load.hpp"
#include "vast/save.hpp"

#include "vast/system/key_value_store.hpp"

namespace vast::system {

template <class Key, class Value>
struct simple_store_state {
  using simple_store_actor = typename key_value_store_type<
    Key, Value>::template stateful_pointer<simple_store_state>;

  static inline const char* name = "simple-store";
  std::unordered_map<Key, Value> store;
  path file;

  caf::error init(simple_store_actor self, const path& dir) {
    file = dir / "store";
    if (exists(file)) {
      if (auto err = vast::load(self->system(), file, store)) {
        VAST_WARNING_ANON(name, "unable to load state file:", file);
        return err;
      }
    }
    return caf::none;
  }

  caf::error save(caf::actor_system& sys) {
    return vast::save(sys, file, store);
  }
};

/// A key-value store that stores its data in a `std::unordered_map`.
/// @param self The actor handle.
template <class Key, class Value>
typename key_value_store_type<Key, Value>::behavior_type
simple_store(
  typename key_value_store_type<Key, Value>::template stateful_pointer<
    simple_store_state<Key, Value>
  > self, path dir) {
  using behavior_type =
    typename key_value_store_type<Key, Value>::behavior_type;
  if (auto err = self->state.init(self, dir)) {
    self->quit(std::move(err));
    return behavior_type::make_empty_behavior();
  }
  return {
    [=](put_atom, const Key& key, Value& value) -> caf::result<ok_atom> {
      self->state.store[key] = std::move(value);
      if (auto err = self->state.save(self->system()))
        return err;
      return ok_atom::value;
    },
    [=](add_atom, const Key& key, const Value& value) -> caf::result<Value> {
      auto& v = self->state.store[key];
      auto old = v;
      v += value;
      if (auto err = self->state.save(self->system()))
        return err;
      return old;
    },
    [=](delete_atom, const Key& key) -> caf::result<ok_atom> {
      self->state.store.erase(key);
      if (auto err = self->state.save(self->system()))
        return err;
      return ok_atom::value;
    },
    [=](get_atom, const Key& key) -> caf::result<optional<Value>> {
      auto i = self->state.store.find(key);
      if (i == self->state.store.end())
        return caf::none;
      return i->second;
    }
  };
}

} // namespace vast::system

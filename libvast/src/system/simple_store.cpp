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

#include "vast/system/simple_store.hpp"

#include <caf/all.hpp>
#include <caf/none.hpp>

#include "vast/filesystem.hpp"
#include "vast/load.hpp"
#include "vast/save.hpp"


namespace vast::system {

simple_store_state::simple_store_state(actor_ptr self)
  : self{self} {
  // nop
}

caf::error simple_store_state::init(path dir) {
  file = std::move(dir) / "store";
  if (exists(file)) {
    if (auto err = vast::load(self->system(), file, store)) {
      VAST_WARNING_ANON(name, "unable to load state file:", file);
      return err;
    }
  }
  return caf::none;
}

caf::error simple_store_state::save() {
  return vast::save(self->system(), file, store);
}

/// A key-value store that stores its data in a `std::unordered_map`.
/// @param self The actor handle.
consensus_type::behavior_type
simple_store(simple_store_state::actor_ptr self, path dir) {
  using behavior_type = consensus_type::behavior_type;
  if (auto err = self->state.init(std::move(dir))) {
    self->quit(std::move(err));
    return behavior_type::make_empty_behavior();
  }
  return {
    [=](put_atom, const std::string& key, data& value) -> caf::result<ok_atom> {
      self->state.store[key] = std::move(value);
      if (auto err = self->state.save())
        return err;
      return ok_atom::value;
    },
    [=](add_atom, const std::string& key, const data& value) -> caf::result<data> {
      auto& v = self->state.store[key];
      auto old = v;
      v += value;
      if (auto err = self->state.save())
        return err;
      return old;
    },
    [=](delete_atom, const std::string& key) -> caf::result<ok_atom> {
      self->state.store.erase(key);
      if (auto err = self->state.save())
        return err;
      return ok_atom::value;
    },
    [=](get_atom, const std::string& key) -> caf::result<optional<data>> {
      auto i = self->state.store.find(key);
      if (i == self->state.store.end())
        return caf::none;
      return i->second;
    }
  };
}

} // namespace vast::system

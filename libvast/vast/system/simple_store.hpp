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

#include <caf/fwd.hpp>

#include "vast/data.hpp"
#include "vast/filesystem.hpp"

#include "vast/system/consensus.hpp"

namespace vast::system {

struct simple_store_state {
  using actor_ptr = consensus_type::stateful_pointer<simple_store_state>;

  static inline const char* name = "simple-store";

  simple_store_state(actor_ptr self);

  /// Initializes the state.
  /// @param dir The directory of the store.
  caf::error init(path dir);

  /// Saves the current state to `file`.
  /// @pre `init()` was run successfully.
  caf::error save();

  actor_ptr self;

  /// The data container.
  std::unordered_map<std::string, data> store;

  /// The location of the persistence file.
  path file;
};

/// A key-value store that stores its data in a `std::unordered_map`.
/// @param self The actor handle.
/// @param dir The directory of the store.
consensus_type::behavior_type
simple_store(simple_store_state::actor_ptr self, path dir);

} // namespace vast::system

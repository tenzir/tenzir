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

#include "vast/system/meta_store.hpp"

namespace vast::system {

class simple_store_state {
public:
  using actor_ptr = meta_store_type::stateful_pointer<simple_store_state>;

  static inline const char* name = "simple-store";
  std::unordered_map<std::string, data> store;
  path file;

  caf::error init(actor_ptr self, path dir);

  caf::error save();

private:
  actor_ptr self;
};

/// A key-value store that stores its data in a `std::unordered_map`.
/// @param self The actor handle.
meta_store_type::behavior_type
simple_store(simple_store_state::actor_ptr self, path dir);

} // namespace vast::system

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

#include <caf/actor.hpp>
#include <caf/stateful_actor.hpp>

#include "vast/aliases.hpp"
#include "vast/filesystem.hpp"
#include "vast/type.hpp"

namespace vast::system {

/// @relates partition
struct partition_meta_data {
  /// Maps type digests (used as directory name for an indexer) to types.
  std::map<std::string, type> types;
  bool dirty = false;
};

/// @relates partition_meta_data
template <class Inspector>
auto inspect(Inspector& f, partition_meta_data& x) {
  return f(x.types);
}

/// @relates partition
struct partition_state {
  std::unordered_map<type, caf::actor> indexers;
  partition_meta_data meta_data;
  static inline const char* name = "partition";
};

/// A horizontal partition of the INDEX.
/// For each event batch, PARTITION spawns one event indexer per
/// type occurring in the batch and forwards to them the events.
/// @param dir The directory where to store this partition on the file system.
caf::behavior partition(caf::stateful_actor<partition_state>* self, path dir);

} // namespace vast::system


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

#include "vast/aliases.hpp"
#include "vast/fwd.hpp"

namespace vast::system {

/// Spawns an INDEXER actor.
/// @param parent The parent actor.
/// @param filename File for persistent state.
/// @param column_type The type of the indexed field.
/// @param index_opts Runtime options to parameterize the value index.
/// @param index A handle to the index actor.
/// @param partition_id The partition ID that this INDEXER belongs to.
/// @param m A pointer to the measuring probe used for perfomance data
///        accumulation.
/// @returns the new INDEXER actor.
caf::actor
spawn_indexer(caf::local_actor* parent, path filename, type column_type,
              caf::settings index_opts, caf::actor index, uuid partition_id,
              atomic_measurement* m);

} // namespace vast::system

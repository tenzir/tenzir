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

#include <vector>

#include <caf/fwd.hpp>

#include "vast/fwd.hpp"

namespace vast::system {

/// Spawns an INDEXER actor.
/// @param parent The parent actor.
/// @param dir Base directory for persistent state.
/// @param
caf::actor spawn_indexer(caf::local_actor* parent, path dir,
                         type column_type, size_t column);

} // namespace vast::system

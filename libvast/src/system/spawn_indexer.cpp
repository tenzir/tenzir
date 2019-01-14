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

#include "vast/system/spawn_indexer.hpp"

#include <caf/actor.hpp>
#include <caf/local_actor.hpp>

#include "vast/filesystem.hpp"
#include "vast/logger.hpp"
#include "vast/system/indexer.hpp"
#include "vast/type.hpp"

namespace vast::system {

caf::actor spawn_indexer(caf::local_actor* parent, path dir, type column_type,
                         size_t column, caf::actor index, uuid partition_id) {
  VAST_TRACE(VAST_ARG(dir), VAST_ARG(column_type), VAST_ARG(column),
             VAST_ARG(index), VAST_ARG(partition_id));
  return parent->spawn<caf::lazy_init>(indexer, std::move(dir),
                                       std::move(column_type), column,
                                       std::move(index), partition_id);
}

} // namespace vast::system

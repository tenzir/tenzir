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

#include "vast/logger.hpp"
#include "vast/path.hpp"
#include "vast/system/indexer.hpp"
#include "vast/system/instrumentation.hpp"
#include "vast/type.hpp"

#include <caf/actor.hpp>
#include <caf/local_actor.hpp>
#include <caf/settings.hpp>

namespace vast::system {

caf::actor
spawn_indexer(caf::local_actor* parent, accountant_type accountant,
              path filename, type column_type, caf::settings index_opts,
              caf::actor index, uuid partition_id, std::string fqn) {
  VAST_TRACE(VAST_ARG(filename), VAST_ARG(column_type), VAST_ARG(index_opts),
             VAST_ARG(index), VAST_ARG(partition_id), VAST_ARG(fqn));
  auto actor = parent->spawn<caf::lazy_init>(
    indexer, std::move(filename), std::move(column_type), std::move(index_opts),
    std::move(index), partition_id, fqn);
  if (accountant)
    caf::anon_send(actor, std::move(accountant));
  return actor;
}

} // namespace vast::system

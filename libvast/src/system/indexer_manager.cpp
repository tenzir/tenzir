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

#include "vast/logger.hpp"

#include "vast/system/indexer_manager.hpp"
#include "vast/system/partition.hpp"

namespace vast::system {

indexer_manager::indexer_manager(partition& parent, indexer_factory f)
  : make_indexer_(std::move(f)),
    parent_(parent) {
  // nop
}

std::pair<caf::actor, bool>
indexer_manager::get_or_add(const record_type& key) {
  VAST_TRACE(VAST_ARG(key));
  auto i = indexers_.find(key);
  if (i != indexers_.end())
    return {i->second, false};
  auto digest = to_digest(key);
  parent_.add_layout(digest, key);
  auto res = indexers_.emplace(key, make_indexer(key, digest));
  VAST_ASSERT(res.second == true);
  return {res.first->second, true};
}

caf::actor indexer_manager::make_indexer(const record_type& key,
                                         const std::string& digest) {
  VAST_TRACE(VAST_ARG(key), VAST_ARG(digest));
  VAST_ASSERT(make_indexer_ != nullptr);
  return make_indexer_(parent_.dir() / digest, key);
}

caf::actor indexer_manager::make_indexer(const record_type& key) {
  VAST_TRACE(VAST_ARG(key));
  return make_indexer(key, to_digest(key));
}

} // namespace vast::system

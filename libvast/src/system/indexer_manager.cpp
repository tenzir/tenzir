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

#include "vast/system/indexer_manager.hpp"

#include <caf/event_based_actor.hpp>
#include <caf/exit_reason.hpp>
#include <caf/local_actor.hpp>
#include <caf/send.hpp>

#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/uuid.hpp"
#include "vast/load.hpp"
#include "vast/save.hpp"
#include "vast/system/indexer.hpp"

namespace vast::system {

indexer_manager::indexer_manager(path base_dir, uuid partition_id,
                                 indexer_factory f)
  : make_indexer_(std::move(f)),
    dir_(base_dir / to_string(partition_id)) {
  // If the directory exists already, we must have some state from the past
  // and are pre-loading all INDEXER types we are aware of.
  if (exists(dir_)) {
    if (auto result = load(dir_ / "meta", meta_data_)) {
      for (auto& [str, t] : meta_data_.types)
        indexers_.emplace(t, make_indexer(t));
    } else {
      VAST_ERROR("unable to read INDEXER types:", result.error());
    }
  }
}

indexer_manager::~indexer_manager() noexcept{
  // Save persistent state.
  if (meta_data_.dirty) {
    if (!exists(dir_))
      mkdir(dir_);
    save(dir_ / "meta", meta_data_);
  }
}

std::pair<caf::actor, bool> indexer_manager::get_or_add(const type& key) {
  auto i = indexers_.find(key);
  if (i != indexers_.end())
    return {i->second, false};
  if (!meta_data_.dirty)
    meta_data_.dirty = true;
  auto digest = to_digest(key);
  meta_data_.types.emplace(digest, key);
  auto res = indexers_.emplace(key, make_indexer(key, digest));
  VAST_ASSERT(res.second == true);
  return {res.first->second, true};
}

std::vector<type> indexer_manager::types() const {
  std::vector<type> result;
  result.reserve(indexers_.size());
  for (auto& [t, a] : indexers_)
    result.emplace_back(t);
  return result;
}

caf::actor indexer_manager::make_indexer(const type& key, std::string digest) {
  VAST_ASSERT(make_indexer_ != nullptr);
  return make_indexer_(dir_ / digest, key);
}

caf::actor indexer_manager::make_indexer(const type& key) {
  return make_indexer(key, to_digest(key));
}

std::string indexer_manager::to_digest(const type& x) {
  return to_string(std::hash<type>{}(x));
}

indexer_manager_ptr make_indexer_manager(path base_dir,
                                         uuid partition_id,
                                         indexer_manager::indexer_factory f) {
  return caf::make_counted<indexer_manager>(
    std::move(base_dir), std::move(partition_id), std::move(f));
}

indexer_manager_ptr make_indexer_manager(caf::local_actor* self, path base_dir,
                                         uuid partition_id) {
  auto f = [self](path indexer_path, type indexer_type) {
    VAST_DEBUG(self, "creates event-indexer for type", indexer_type);
    return self->spawn<caf::lazy_init>(indexer, std::move(indexer_path),
                                       std::move(indexer_type));
  };
  return make_indexer_manager(std::move(base_dir), std::move(partition_id), f);
}

} // namespace vast::system

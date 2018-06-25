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

#include "vast/system/partition.hpp"

#include <caf/event_based_actor.hpp>
#include <caf/local_actor.hpp>
#include <caf/stateful_actor.hpp>

#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/concept/printable/vast/uuid.hpp"
#include "vast/detail/assert.hpp"
#include "vast/event.hpp"
#include "vast/ids.hpp"
#include "vast/load.hpp"
#include "vast/logger.hpp"
#include "vast/save.hpp"
#include "vast/system/atoms.hpp"
#include "vast/system/indexer.hpp"
#include "vast/time.hpp"

using namespace std::chrono;
using namespace caf;

namespace vast {
namespace system {

partition::partition(const path& base_dir, uuid id,
                     indexer_manager::indexer_factory factory)
  : mgr_(*this, std::move(factory)),
    dir_(base_dir / to_string(id)),
    id_(std::move(id)) {
  // If the directory already exists, we must have some state from the past and
  // are pre-loading all INDEXER types we are aware of.
  if (exists(dir_)) {
    auto res = load(meta_file(), meta_data_);
    if (!res) {
      VAST_ERROR("unable to read partition meta data:", res.error());
    } else {
      for (auto& kvp : meta_data_.types) {
        // We spawn all INDEXER actors immediately. However, the factory spawns
        // those actors with lazy_init, which means they won't load persisted
        // state from disk until first access.
        mgr_.get_or_add(kvp.second);
      }
    }
  }
}

partition::~partition() noexcept {
  flush_to_disk();
}

// -- persistence ------------------------------------------------------------

caf::error partition::flush_to_disk() {
  if (meta_data_.dirty) {
    if (!exists(dir_))
      mkdir(dir_);
    auto res = save(meta_file(), meta_data_);
    if (!res)
      return std::move(res.error());
    meta_data_.dirty = false;
  }
  return caf::none;
}

std::vector<type> partition::types() const {
  std::vector<type> result;
  auto& ts = meta_data_.types;
  result.reserve(ts.size());
  std::transform(ts.begin(), ts.end(), std::back_inserter(result),
                 [](auto& kvp) { return kvp.second; });
  return result;
}

path partition::meta_file() const {
  return dir_ / "meta";
}

size_t partition::get_indexers(std::vector<caf::actor>& indexers,
                               const expression& expr) {
  return mgr_.for_each_match(expr,
                             [&](caf::actor& x) { indexers.emplace_back(x); });
}

std::vector<caf::actor> partition::get_indexers(const expression& expr) {
  std::vector<caf::actor> result;
  get_indexers(result, expr);
  return result;
}

// -- free functions -----------------------------------------------------------

partition_ptr make_partition(const path& base_dir, uuid id,
                             indexer_manager::indexer_factory f) {
  return caf::make_counted<partition>(base_dir, std::move(id), std::move(f));
}

partition_ptr make_partition(caf::local_actor* self, const path& base_dir,
                             uuid id) {
  auto f = [=](path indexer_path, type indexer_type) {
    VAST_DEBUG(self, "creates INDEXER in partition", id, "for type",
               indexer_type);
    return self->spawn<caf::lazy_init>(indexer, std::move(indexer_path),
                                       std::move(indexer_type));
  };
  return make_partition(base_dir, std::move(id), f);
}

} // namespace system
} // namespace vast

namespace std {

namespace {

using pptr = vast::system::partition_ptr;

} // namespace <anonymous>

size_t hash<pptr>::operator()(const pptr& ptr) const {
  hash<vast::uuid> f;
  return ptr != nullptr ? f(ptr->id()) : 0u;
}

} // namespace std

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
#include <caf/make_counted.hpp>
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
#include "vast/system/index.hpp"
#include "vast/system/spawn_indexer.hpp"
#include "vast/system/table_indexer.hpp"
#include "vast/time.hpp"

using namespace std::chrono;
using namespace caf;

namespace vast::system {

partition::partition(index_state* state, uuid id, size_t max_capacity)
  : state_(state),
    id_(std::move(id)),
    capacity_(max_capacity) {
  // If the directory already exists, we must have some state from the past and
  // are pre-loading all INDEXER types we are aware of.
  VAST_ASSERT(state != nullptr);
}

partition::~partition() noexcept {
  flush_to_disk();
}

// -- persistence --------------------------------------------------------------

caf::error partition::init() {
  VAST_TRACE("");
  auto file_path = meta_file();
  if (!exists(file_path))
    return ec::no_such_file;
  if (auto err = load(state_->self->system(), file_path , meta_data_))
    return err;
  VAST_DEBUG(state_->self, "loaded partition", id_, "from disk with",
             meta_data_.types.size(), "layouts");
  return caf::none;
}

caf::error partition::flush_to_disk() {
  if (meta_data_.dirty) {
    if (auto err = save(state_->self->system(), meta_file(), meta_data_))
      return err;
    meta_data_.dirty = false;
  }
  return caf::none;
}

// -- properties ---------------------------------------------------------------

std::vector<caf::actor> partition::indexers(const expression&) {
  // TODO: use given expression as layout filter.
  std::vector<caf::actor> result;
  for (auto layout : layouts()) {
    auto& tbl = get_or_add(layout).first;
    tbl.materialize();
    tbl.for_each_indexer([&](auto &hdl) {
      result.emplace_back(hdl);
    });
  }
  return result;
}

std::vector<record_type> partition::layouts() const {
  std::vector<record_type> result;
  auto& ts = meta_data_.types;
  result.reserve(ts.size());
  std::transform(ts.begin(), ts.end(), std::back_inserter(result),
                 [](auto& kvp) { return kvp.second; });
  return result;
}

path partition::base_dir() const {
  return state_->dir / to_string(id_);
}

path partition::meta_file() const {
  return base_dir() / "meta";
}

std::pair<table_indexer&, bool> partition::get_or_add(const record_type& key) {
  VAST_TRACE(VAST_ARG(key));
  auto i = table_indexers_.find(key);
  if (i != table_indexers_.end())
    return {i->second, false};
  auto digest = to_digest(key);
  add_layout(digest, key);
  auto result = table_indexers_.emplace(key, table_indexer{this, key});
  VAST_ASSERT(result.second == true);
  return {result.first->second, true};

}

} // namespace vast::system

namespace std {

namespace {

using pptr = vast::system::partition_ptr;

} // namespace <anonymous>

size_t hash<pptr>::operator()(const pptr& ptr) const {
  hash<vast::uuid> f;
  return ptr != nullptr ? f(ptr->id()) : 0u;
}

} // namespace std

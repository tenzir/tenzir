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

#include <caf/make_counted.hpp>

#include "vast/bitmap_algorithms.hpp"
#include "vast/error.hpp"
#include "vast/event.hpp"
#include "vast/ids.hpp"
#include "vast/load.hpp"
#include "vast/logger.hpp"
#include "vast/save.hpp"
#include "vast/segment_store.hpp"

#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/error.hpp"
#include "vast/concept/printable/vast/filesystem.hpp"
#include "vast/concept/printable/vast/uuid.hpp"

#include "vast/const_table_slice_handle.hpp"
#include "vast/segment_store.hpp"
#include "vast/table_slice.hpp"
#include "vast/to_events.hpp"

namespace vast {

segment_store_ptr segment_store::make(caf::actor_system& sys, path dir,
                                      size_t max_segment_size,
                                      size_t in_memory_segments) {
  VAST_ASSERT(max_segment_size > 0);
  auto x = caf::make_counted<segment_store>(
    sys, std::move(dir), max_segment_size, in_memory_segments);
  // Materialize meta data of existing segments.
  if (exists(x->meta_path()))
    if (auto result = load(x->meta_path(), x->segments_); !result) {
      VAST_ERROR("failed to unarchive meta data:", to_string(result.error()));
      return nullptr;
    }
  return x;
}

segment_store::~segment_store() {
  // nop
}

caf::error segment_store::put(const_table_slice_handle xs) {
  if (auto error = builder_.add(xs))
    return error;
  if (!segments_.inject(xs->offset(), xs->offset() + xs->rows(), builder_.id()))
    return make_error(ec::unspecified, "failed to update range_map");
  if (builder_.table_slice_bytes() < max_segment_size_)
    return caf::none;
  // We have exceeded our maximum segment size and now finish
  auto x = builder_.finish();
  if (!x)
    return x.error();
  auto seg_ptr = *x;
  if (!exists(segment_path()))
    if (auto result = mkdir(segment_path()); !result)
      return result.error();
  auto filename = segment_path() / to_string(seg_ptr->id());
  if (auto result = save(filename, seg_ptr); !result)
    return result.error();
  VAST_DEBUG("wrote new segment to", filename.trim(-3));
  // Keep new segment in the cache.
  cache_.emplace(seg_ptr->id(), seg_ptr);
  return flush();
}

caf::error segment_store::flush() {
  auto result = save(meta_path(), segments_);
  return result ? caf::none : result.error();
}

caf::expected<std::vector<const_table_slice_handle>>
segment_store::get(const ids& xs) {
  // Collect candidate segments by seeking through the ID set and
  // probing each ID interval.
  std::vector<const uuid*> candidates;
  auto f = [](auto x) { return std::pair{x.left, x.right}; };
  auto g = [&](auto x) {
    auto ptr = &x.value;
    if (candidates.empty() || candidates.back() != ptr)
      candidates.push_back(ptr);
    return caf::none;
  };
  auto begin = segments_.begin();
  auto end = segments_.end();
  if (auto error = traverse(xs, begin, end, f, g))
    return error;
  // Process candidates in reverse order for maximum LRU cache hits.
  std::vector<const_table_slice_handle> result;
  VAST_DEBUG("processing", candidates.size(), "candidates");
  for (auto cand = candidates.rbegin(); cand != candidates.rend(); ++cand) {
    auto& id = **cand;
    caf::expected<std::vector<const_table_slice_handle>> slices{caf::no_error};
    if (id == builder_.id()) {
      VAST_DEBUG("looking into builder");
      slices = builder_.lookup(xs);
    } else {
      segment_ptr seg_ptr = nullptr;
      auto i = cache_.find(id);
      if (i != cache_.end()) {
        VAST_DEBUG("got cache hit for segment", id);
        seg_ptr = i->second;
      } else {
        VAST_DEBUG("got cache miss for segment", id);
        if (auto res = load(segment_path() / to_string(id), seg_ptr); !res)
          return res.error();
        i = cache_.emplace(id, seg_ptr).first;
      }
      VAST_ASSERT(seg_ptr != nullptr);
      slices = seg_ptr->lookup(xs);
    }
    if (!slices)
      return slices.error();
    result.reserve(result.size() + slices->size());
    result.insert(result.end(), slices->begin(), slices->end());
  }
  return result;
}

segment_store::segment_store(caf::actor_system& sys, path dir,
                             uint64_t max_segment_size, size_t in_memory_segments)
  : actor_system_{sys},
    dir_{std::move(dir)},
    max_segment_size_{max_segment_size},
    cache_{in_memory_segments},
    builder_{actor_system_} {
  // nop
}

} // namespace vast

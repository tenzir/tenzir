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

#include "vast/segment_store.hpp"

#include <caf/config_value.hpp>
#include <caf/dictionary.hpp>

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

#include "vast/table_slice.hpp"
#include "vast/to_events.hpp"

namespace vast {

segment_store_ptr segment_store::make(caf::actor_system& sys, path dir,
                                      size_t max_segment_size,
                                      size_t in_memory_segments) {
  VAST_TRACE(VAST_ARG(dir), VAST_ARG(max_segment_size),
             VAST_ARG(in_memory_segments));
  VAST_ASSERT(max_segment_size > 0);
  auto x = std::make_unique<segment_store>(
    sys, std::move(dir), max_segment_size, in_memory_segments);
  // Materialize meta data of existing segments.
  if (exists(x->meta_path())) {
    VAST_DEBUG_ANON(__func__, "loads segment meta data from", x->meta_path());
    if (auto err = load(sys, x->meta_path(), x->segments_)) {
      VAST_ERROR_ANON(__func__, "failed to unarchive meta data:", sys.render(err));
      return nullptr;
    }
  }
  return x;
}

segment_store::~segment_store() {
  // nop
}

caf::error segment_store::put(table_slice_ptr xs) {
  VAST_TRACE(VAST_ARG(xs));
  VAST_DEBUG(this, "adds a table slice");
  if (auto error = builder_.add(xs))
    return error;
  if (!segments_.inject(xs->offset(), xs->offset() + xs->rows(), builder_.id()))
    return make_error(ec::unspecified, "failed to update range_map");
  if (builder_.table_slice_bytes() < max_segment_size_)
    return caf::none;
  // We have exceeded our maximum segment size and now finish.
  return flush();
}

caf::error segment_store::flush() {
  auto x = builder_.finish();
  if (!x)
    return x.error();
  auto seg_ptr = *x;
  auto filename = segment_path() / to_string(seg_ptr->id());
  if (auto err = save(sys_, filename, seg_ptr))
    return err;
  // Keep new segment in the cache.
  cache_.emplace(seg_ptr->id(), seg_ptr);
  VAST_DEBUG(this, "wrote new segment to", filename.trim(-3));
  VAST_DEBUG(this, "saves segment meta data");
  return save(sys_, meta_path(), segments_);
}

caf::expected<std::vector<table_slice_ptr>>
segment_store::get(const ids& xs) {
  VAST_TRACE(VAST_ARG(xs));
  // Collect candidate segments by seeking through the ID set and
  // probing each ID interval.
  VAST_DEBUG(this, "retrieves table slices with requested ids");
  std::vector<uuid> candidates;
  auto f = [](auto x) { return std::pair{x.left, x.right}; };
  auto g = [&](auto x) {
    auto id = x.value;
    if (candidates.empty() || candidates.back() != id)
      candidates.push_back(id);
    return caf::none;
  };
  auto begin = segments_.begin();
  auto end = segments_.end();
  if (auto error = select_with(xs, begin, end, f, g))
    return error;
  // Process candidates in reverse order for maximum LRU cache hits.
  std::vector<table_slice_ptr> result;
  VAST_DEBUG(this, "processes", candidates.size(), "candidates");
  for (auto cand = candidates.rbegin(); cand != candidates.rend(); ++cand) {
    auto& id = *cand;
    caf::expected<std::vector<table_slice_ptr>> slices{caf::no_error};
    if (id == builder_.id()) {
      VAST_DEBUG(this, "looks into the active segement");
      slices = builder_.lookup(xs);
    } else {
      segment_ptr seg_ptr = nullptr;
      auto i = cache_.find(id);
      if (i != cache_.end()) {
        VAST_DEBUG(this, "got cache hit for segment", id);
        seg_ptr = i->second;
      } else {
        VAST_DEBUG(this, "got cache miss for segment", id);
        auto fname = segment_path() / to_string(id);
        if (auto err = load(sys_, fname, seg_ptr)) {
          VAST_ERROR(this, "unable to load segment:", sys_.render(err));
          return err;
        }
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

void segment_store::inspect_status(caf::dictionary<caf::config_value>& dict) {
  using caf::put;
  put(dict, "meta-path", meta_path().str());
  put(dict, "segment-path", segment_path().str());
  put(dict, "max-segment-size", max_segment_size_);
  auto& segments = put_dictionary(dict, "segments");
  // Note: `for (auto& kvp : segments_)` does not compile.
  for (auto i = segments_.begin(); i != segments_.end(); ++i) {
    std::string range = "[";
    range += std::to_string(i->left);
    range += ", ";
    range += std::to_string(i->right);
    range += ")";
    put(segments, range, to_string(i->value));
  }
  auto& cached = put_list(dict, "cached");
  for (auto& kvp : cache_)
    cached.emplace_back(to_string(kvp.first));
  auto& current = put_dictionary(dict, "current-segment");
  put(current, "id", to_string(builder_.id()));
  put(current, "size", builder_.table_slice_bytes());
}

segment_store::segment_store(caf::actor_system& sys, path dir,
                             uint64_t max_segment_size, size_t in_memory_segments)
  : sys_{sys},
    dir_{std::move(dir)},
    max_segment_size_{max_segment_size},
    cache_{in_memory_segments},
    builder_{sys_} {
  // nop
}

} // namespace vast

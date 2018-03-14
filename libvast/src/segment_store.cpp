#include <algorithm>

#include "vast/error.hpp"
#include "vast/event.hpp"
#include "vast/load.hpp"
#include "vast/logger.hpp"
#include "vast/save.hpp"
#include "vast/segment_store.hpp"

#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/error.hpp"
#include "vast/concept/printable/vast/filesystem.hpp"
#include "vast/concept/printable/vast/uuid.hpp"

namespace vast {

void segment_store::segment::add(batch&& x) {
  auto min = select(x.ids(), 1);
  VAST_ASSERT(min != invalid_event_id);
  VAST_ASSERT(batches_.find(min) == batches_.end());
  bytes_ += bytes(x);
  batches_.emplace(min, std::move(x));
}

// FIXME: this algorithm is not very efficient. It operates in O(MN) time where
// M is the number of batches and N the size of the bitmap. Conceptually,
// there's a straight-forward O(log M * N) or even O(N) algorithm by walking
// through the query bitmap in lock-step with the batches.
expected<std::vector<event>>
segment_store::segment::extract(const bitmap& xs) const {
  auto min = select(xs, 1);
  auto max = select(xs, -1);
  // FIXME: what we really want here is detail::range_map, but it's currently
  // missing lower_bound()/upper_bound() functionality, so we emulate it here.
  auto begin = batches_.lower_bound(min);
  if (begin != batches_.begin()
      && (begin == batches_.end() || begin->first > min))
    --begin;
  auto end = batches_.upper_bound(max);
  std::vector<event> result;
  for (; begin != end; ++begin) {
    batch::reader reader{begin->second};
    auto events = reader.read(xs);
    if (!events)
      return events;
    result.reserve(result.size() + events->size());
    std::move(events->begin(), events->end(), std::back_inserter(result));
  }
  return result;
}

const uuid& segment_store::segment::id() const {
  return id_;
}

uint64_t bytes(const segment_store::segment& x) {
  return x.bytes_;
}


segment_store::segment_store(path dir, size_t max_segment_size,
                             size_t in_memory_segments)
  : dir_{std::move(dir)},
    max_segment_size_{max_segment_size},
    cache_{in_memory_segments} {
  VAST_ASSERT(max_segment_size > 0);
  // Load meta data about existing segments.
  if (exists(dir_ / "meta"))
    if (auto result = load(dir / "meta", segments_); !result) {
      // TODO: factor into a separate function and do not do this work in the
      // constructor.
      VAST_ERROR("failed to unarchive meta data:", to_string(result.error()));
      segments_ = {};
    }

}

expected<void> segment_store::put(const std::vector<event>& xs) {
  VAST_ASSERT(!xs.empty());
  // Ensure that all events have strictly monotonic IDs
  auto non_monotonic = [](auto& x, auto& y) { return x.id() != y.id() - 1; };
  if (std::adjacent_find(xs.begin(), xs.end(), non_monotonic) != xs.end())
    return make_error(ec::unspecified, "got batch with non-monotonic IDs");
  batch::writer writer{compression::lz4};
  for (auto& e : xs)
    if (!writer.write(e))
      return make_error(ec::unspecified, "failed to create batch");
  auto b = writer.seal();
  auto first = xs.front().id();
  auto last  = xs.back().id();
  b.ids(first, last + 1);
  // If the batch would cause the segment to exceed its maximum size, then
  // flush and replace the active segment.
  if (bytes(active_) >= max_segment_size_) {
    VAST_ASSERT(bytes(active_) != 0); // must not be empty
    if (auto result = flush(); !result)
      return result.error();
  }
  // Append batch to active segment.
  segments_.inject(first, last + 1, active_.id());
  active_.add(std::move(b));
  return no_error;
}

expected<void> segment_store::flush() {
  if (bytes(active_) == 0)
    return no_error;
  // Write segment to file system.
  if (!exists(dir_))
    if (auto result = mkdir(dir_); !result)
      return result.error();
  auto filename = dir_ / to_string(active_.id());
  auto result = save(filename, segment::magic, segment::version, active_);
  if (!result)
    return result.error();
  // Move active segment into cache.
  auto segment_id = active_.id();
  cache_.emplace(segment_id, std::move(active_));
  active_ = {};
  // Update persistent meta data.
  if (auto result = save(dir_ / "meta", segments_); !result)
    return result.error();
  VAST_DEBUG("wrote active segment to", filename.trim(-3));
  return no_error;
}

expected<std::vector<event>> segment_store::get(const ids& xs) {
  // Collect candidate segments by seeking through the query bitmap and
  // probing each ID interval.
  std::vector<const uuid*> candidates;
  auto ones = select(xs);
  auto i = segments_.begin();
  auto end = segments_.end();
  while (ones && i != end) {
    if (ones.get() < i->left) {
      // Bitmap must catch up, segment is ahead.
      ones.skip(i->left - ones.get());
    } else if (ones.get() < i->right) {
      // Match: bitmap is within an existing segment.
      candidates.push_back(&i->value);
      ones.skip(i->right - ones.get());
      ++i;
    } else {
      // Segment must catch up, bitmap is ahead.
      ++i;
    }
  }
  // Process candidates *in reverse order* to get maximum LRU cache hits.
  std::vector<event> result;
  VAST_DEBUG("processing", candidates.size(), "candidates");
  for (auto id = candidates.rbegin(); id != candidates.rend(); ++id) {
    segment* s = nullptr;
    // If the segment turns out to be the active segment, we can
    // can query it immediately.
    if (**id == active_.id()) {
      VAST_DEBUG("looking into active segment");
      s = &active_;
    } else {
      // Otherwise we look into the cache.
      auto i = cache_.find(**id);
      if (i != cache_.end()) {
        VAST_DEBUG("got cache hit for segment", **id);
        s = &i->second;
      } else {
        VAST_DEBUG("got cache miss for segment", **id);
        segment::magic_type m;
        segment::version_type v;
        segment seg;
        if (auto result = load(dir_ / to_string(**id), m, v, seg); !result)
          return result.error();
        if (m != segment::magic)
          return make_error(ec::unspecified, "segment magic error");
        if (v < segment::version)
          return make_error(ec::version_error, v, segment::version);
        i = cache_.emplace(**id, std::move(seg)).first;
        s = &i->second;
      }
    }
    // Perform lookup in segment and append extracted events to result.
    VAST_ASSERT(s != nullptr);
    auto events = s->extract(xs);
    if (!events)
      return events.error();
    result.reserve(result.size() + events->size());
    std::move(events->begin(), events->end(), std::back_inserter(result));
  }
  return result;
}

} // namespace vast

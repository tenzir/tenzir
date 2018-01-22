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

#include <algorithm>

#include "vast/logger.hpp"

#include "vast/batch.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/error.hpp"
#include "vast/concept/printable/vast/filesystem.hpp"
#include "vast/concept/printable/vast/uuid.hpp"
#include "vast/detail/assert.hpp"
#include "vast/expected.hpp"
#include "vast/load.hpp"
#include "vast/save.hpp"

#include "vast/system/archive.hpp"

using std::chrono::steady_clock;
using std::chrono::duration_cast;
using std::chrono::microseconds;
using namespace caf;

namespace vast {
namespace system {

const segment::magic_type segment::magic;
const segment::version_type segment::version;

void segment::add(batch&& b) {
  auto min = select(b.ids(), 1);
  VAST_ASSERT(min != invalid_event_id);
  VAST_ASSERT(batches_.find(min) == batches_.end());
  bytes_ += bytes(b);
  batches_.emplace(min, std::move(b));
}

// FIXME: this algorithm is not very efficient. It operates in O(MN) time where
// M is the number of batches and N the size of the bitmap. Conceptually,
// there's a straight-forward O(log M * N) or even O(N) algorithm by walking
// through the query bitmap in lock-step with the batches.
expected<std::vector<event>> segment::extract(bitmap const& bm) const {
  std::vector<event> result;
  auto min = select(bm, 1);
  auto max = select(bm, -1);
  // FIXME: what we really want here is detail::range_map, but it's currently
  // missing lower_bound()/upper_bound() functionality, so we emulate it here.
  auto begin = batches_.lower_bound(min);
  if (begin != batches_.begin()
      && (begin == batches_.end() || begin->first > min))
    --begin;
  auto end = batches_.upper_bound(max);
  for (; begin != end; ++begin) {
    batch::reader reader{begin->second};
    auto xs = reader.read(bm);
    if (!xs)
      return xs;
    result.reserve(result.size() + xs->size());
    std::move(xs->begin(), xs->end(), std::back_inserter(result));
  }
  return result;
}

uuid const& segment::id() const {
  return id_;
}

uint64_t bytes(segment const& s) {
  return s.bytes_;
}

namespace {

template <class Actor>
expected<void> flush_active_segment(Actor* self) {
  VAST_DEBUG(self, "flushes current segment", self->state.active.id());
  // Don't touch filesystem if we have nothing to do.
  if (bytes(self->state.active) == 0)
    return {};
  // Write segment to file system.
  if (!exists(self->state.dir)) {
    auto result = mkdir(self->state.dir);
    if (!result)
      return result.error();
  }
  auto id = self->state.active.id();
  auto filename = self->state.dir / to_string(id);
  auto start = steady_clock::now();
  auto result = save(filename, segment::magic, segment::version,
                     self->state.active);
  if (!result)
    return result.error();
  if (self->state.accountant) {
    auto stop = steady_clock::now();
    auto unit = duration_cast<microseconds>(stop - start).count();
    auto rate = bytes(self->state.active) * 1e6 / unit;
    self->send(self->state.accountant, "archive.flush.rate", rate);
  }
  VAST_DEBUG(self, "wrote active segment to", filename.trim(-3));
  self->state.cache.emplace(id, std::move(self->state.active));
  self->state.active = {};
  // Update meta data on filessytem.
  auto t = save(self->state.dir / "meta", self->state.segments);
  if (!t)
    return t.error();
  VAST_DEBUG(self, "updated persistent meta data");
  return {};
}

using flush_promise = typed_response_promise<ok_atom>;
using lookup_promise = typed_response_promise<std::vector<event>>;

} // namespace <anonymous>

archive_type::behavior_type
archive(archive_type::stateful_pointer<archive_state> self,
        path dir, size_t capacity, size_t max_segment_size) {
  VAST_ASSERT(max_segment_size > 0);
  self->state.dir = std::move(dir);
  self->state.max_segment_size = max_segment_size;
  self->state.cache.capacity(capacity);
  self->state.cache.on_evict(
    [=](uuid& id, segment&) {
      VAST_IGNORE_UNUSED(id);
      VAST_DEBUG(self, "evicts cache entry: segment", id);
    }
  );
  // Load meta data about existing segments.
  if (exists(self->state.dir / "meta")) {
    auto t = load(self->state.dir / "meta", self->state.segments);
    if (!t) {
      VAST_ERROR(self, "failed to unarchive meta data:",
                 self->system().render(t.error()));
      self->quit(t.error());
    }
  }
  self->set_exit_handler(
    [=](const exit_msg& msg) {
      flush_active_segment(self);
      self->quit(msg.reason);
    }
  );
  // Register the accountant, if available.
  auto acc = self->system().registry().get(accountant_atom::value);
  if (acc) {
    VAST_DEBUG(self, "registers accountant", acc);
    self->state.accountant = actor_cast<accountant_type>(acc);
  }
  return {
    [=](std::vector<event> const& events) {
      VAST_ASSERT(!events.empty());
      // Ensure that all events have strictly monotonic IDs
      auto non_monotonic = [](auto& x, auto& y) {
        return x.id() != y.id() - 1;
      };
      auto valid = std::adjacent_find(events.begin(), events.end(),
                                      non_monotonic) == events.end();
      if (!valid) {
        VAST_WARNING(self, "ignores", events.size(),
                     "events with non-monotonic IDs");
        return;
      }
      // Construct a batch from the events.
      auto first_id = events.front().id();
      auto last_id  = events.back().id();
      VAST_DEBUG(self, "got", events.size(),
                 "events [" << first_id << ',' << (last_id + 1) << ')');
      auto start = steady_clock::now();
      batch::writer writer{compression::lz4};
      for (auto& e : events)
        if (!writer.write(e)) {
          self->quit(make_error(ec::unspecified, "failed to create batch"));
          return;
        }
      auto b = writer.seal();
      b.ids(first_id, last_id + 1);
      auto stop = steady_clock::now();
      if (self->state.accountant) {
        auto runtime = stop - start;
        auto unit = duration_cast<microseconds>(runtime).count();
        auto rate = events.size() * 1e6 / unit;
        self->send(self->state.accountant, "archive.compression.rate", rate);
        uint64_t num = events.size();
        self->send(self->state.accountant, "archive.events.per.batch", num);
      }
      // If the batch would cause the segment to exceed its maximum size, then
      // flush the active segment and append the batch to the new one.
      auto too_big = bytes(self->state.active) >= self->state.max_segment_size;
      auto empty = bytes(self->state.active) == 0;
      if (!empty && too_big) {
        auto result = flush_active_segment(self);
        if (!result) {
          self->quit(result.error());
          return;
        }
      }
      auto active_id = self->state.active.id();
      self->state.segments.inject(first_id, last_id + 1, active_id);
      self->state.active.add(std::move(b));
    },
    [=](flush_atom) -> flush_promise {
      auto rp = self->make_response_promise<flush_promise>();
      auto result = flush_active_segment(self);
      if (!result) {
        rp.deliver(result.error());
        self->quit(result.error());
      } else {
        rp.deliver(ok_atom::value);
      }
      return rp;
    },
    [=](bitmap const& bm) -> lookup_promise {
      VAST_ASSERT(rank(bm) > 0);
      VAST_DEBUG(self, "got query for", rank(bm), "events in range ["
                 << select(bm, 1) << ',' << (select(bm, -1) + 1) << ')');
      auto rp = self->make_response_promise<lookup_promise>();
      // Collect candidate segments by seeking through the query bitmap and
      // probing each ID interval.
      std::vector<uuid const*> candidates;
      auto ones = select(bm);
      auto i = self->state.segments.begin();
      auto end = self->state.segments.end();
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
      VAST_DEBUG(self, "processing", candidates.size(), "candidates");
      for (auto c = candidates.rbegin(); c != candidates.rend(); ++c) {
        segment* s = nullptr;
        // If the segment turns out to be the active segment, we can
        // can query it immediately.
        if (**c == self->state.active.id()) {
          VAST_DEBUG(self, "looking into active segment");
          s = &self->state.active;
        } else {
          // Otherwise we look into the cache.
          auto i = self->state.cache.find(**c);
          if (i != self->state.cache.end()) {
            VAST_DEBUG(self, "got cache hit for segment", **c);
            s = &i->second;
          } else {
            VAST_DEBUG(self, "got cache miss for segment", **c);
            auto filename = self->state.dir / to_string(**c);
            segment::magic_type m;
            segment::version_type v;
            segment seg;
            auto result = load(filename, m, v, seg);
            if (!result) {
              rp.deliver(result.error());
              return rp;
            }
            if (m != segment::magic) {
              rp.deliver(make_error(ec::unspecified, "segment magic error"));
              return rp;
            }
            if (v < segment::version) {
              rp.deliver(make_error(ec::version_error, v, segment::version));
              return rp;
            }
            i = self->state.cache.emplace(**c, std::move(seg)).first;
            s = &i->second;
          }
        }
        // Perform lookup in segment and append extracted events to result.
        VAST_ASSERT(s != nullptr);
        auto xs = s->extract(bm);
        if (!xs) {
          VAST_ERROR(self, self->system().render(xs.error()));
          rp.deliver(xs.error());
          return rp;
        }
        result.reserve(result.size() + xs->size());
        std::move(xs->begin(), xs->end(), std::back_inserter(result));
      }
      VAST_DEBUG(self, "delivers", result.size(), "events");
      rp.deliver(std::move(result));
      return rp;
    },
  };
}

} // namespace system
} // namespace vast

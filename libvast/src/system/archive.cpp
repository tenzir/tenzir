#include <algorithm>

#include "vast/logger.hpp"

#include "vast/bitmap.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/error.hpp"
#include "vast/concept/printable/vast/filesystem.hpp"
#include "vast/concept/printable/vast/uuid.hpp"
#include "vast/expected.hpp"
#include "vast/load.hpp"
#include "vast/save.hpp"

#include "vast/system/accountant.hpp"
#include "vast/system/archive.hpp"

#include "vast/detail/assert.hpp"

using std::chrono::steady_clock;
using std::chrono::duration_cast;
using std::chrono::microseconds;
using namespace caf;

namespace vast {
namespace system {

archive_type::behavior_type
archive(archive_type::stateful_pointer<archive_state> self,
        path dir, size_t capacity, size_t max_segment_size) {
  VAST_ASSERT(max_segment_size > 0);
  auto active_segment_filename = [=] {
    return dir / to_string(self->state.builder_id);
  };
  self->state.builder =
    segment_builder{active_segment_filename(), max_segment_size};
  self->state.cache.capacity(capacity);
  self->state.cache.on_evict(
    [=](uuid& id, segment_viewer&) {
      VAST_DEBUG(self, "evicts cache entry: segment", id);
      // Segment destructor will reduce refcount of underlying chunk.
    }
  );
  // Load meta data of existing segments.
  if (exists(dir / "meta"))
    for (auto& filename : directory{dir / "meta"}) {
      // TODO: Load [from,to) event ID range and place in known segments.
      //auto segment_uuid = to<uuid>(filename.basename().str());
      //if (!t) {
      //  VAST_ERROR(self, "failed to unarchive meta data:",
      //             self->system().render(t.error()));
      //  self->quit(t.error());
      //}
    }
  // Register accountant, if available.
  auto accountant = accountant_type{};
  if (auto acc = self->system().registry().get(accountant_atom::value))
    accountant = actor_cast<accountant_type>(acc);
  // On exit, we attempt to complete the currenly active segment.
  self->set_exit_handler(
    [=](const exit_msg& msg) {
      auto seg = self->state.builder.finish();
      self->quit(seg ? msg.reason : seg.error());
    }
  );
  return {
    [=](std::vector<event> const& events) {
      VAST_ASSERT(!events.empty());
      auto first = events.front().id();
      auto last  = events.back().id();
      VAST_DEBUG(self, "got", events.size(),
                 "events [" << first << ',' << (last + 1) << ')');
      // Assert event ID contiguity.
      VAST_ASSERT(events.size() == last - first);
      // If the new events would cause the segment to exceed its maximum size,
      // then start anew.
      auto too_big = [=] {
        auto empty = bytes(self->state.builder) == 0;
        auto full = bytes(self->state.builder) >= max_segment_size;
        return !empty && full;
      };
      auto start = steady_clock::now();
      auto j = size_t{0};
      for (auto i = 0u; i < events.size(); ++i) {
        while (!too_big()) {
          auto result = self->state.builder.put(events[i]);
          if (!result) {
            VAST_ERROR(self, "failed to put event into segment",
                       self->system().render(result.error()));
            self->state.builder.finish(); // Try to complete segment.
            self->quit(result.error());
            return;
          }
        }
        VAST_DEBUG(self, "exceeded maximum segment size");
        // The current segment is too big. Put the active one into the cache
        // and start over with a new one.
        self->state.segments.inject(events[j].id(), events[i].id(),
                                    self->state.builder_id);
        auto chk = self->state.builder.finish();
        if (!chk) {
          self->quit(chk.error());
          return;
        }
        self->state.cache.emplace(self->state.builder_id,
                                  segment_viewer{std::move(*chk)});
        self->state.builder_id = uuid::random();
        self->state.builder =
          segment_builder{active_segment_filename(), max_segment_size};
        j = i;
      }
      VAST_DEBUG(self, "records events in ["
                         << events[j].id() << ',' << events.back().id() << ')');
      self->state.segments.inject(events[j].id(), events.back().id(),
                                  self->state.builder_id);
      auto stop = steady_clock::now();
      // Record statistics.
      if (accountant) {
        auto runtime = stop - start;
        auto unit = duration_cast<microseconds>(runtime).count();
        auto rate = events.size() * 1e6 / unit;
        self->send(accountant, "archive.throughput", rate);
        uint64_t num = events.size();
        self->send(accountant, "archive.batch_size", num);
      }
    },
    [=](bitmap const& bm) -> result<std::vector<event>> {
      VAST_ASSERT(rank(bm) > 0);
      VAST_DEBUG(self, "got query for", rank(bm), "events in range ["
                 << select(bm, 1) << ',' << (select(bm, -1) + 1) << ')');
      auto rp = self->make_response_promise<std::vector<event>>();
      // Collect candidate segments by seeking through the query bitmap and
      // probing each ID interval.
      std::vector<uuid> candidates;
      auto ones = select(bm);
      auto i = self->state.segments.begin();
      auto end = self->state.segments.end();
      while (ones && i != end) {
        if (ones.get() < i->left) {
          // Bitmap must catch up, segment is ahead.
          ones.skip(i->left - ones.get());
        } else if (ones.get() >= i->right) {
          // Segment must catch up, bitmap is ahead.
          ++i;
        } else {
          // Match: bitmap is within an existing segment.
          candidates.push_back(i->value);
          ones.skip(i->right - ones.get());
          ++i;
        }
      }
      VAST_DEBUG(self, "processes", candidates.size(), "candidates");
      // Assumption: more recent segments are more likely to be cached.
      std::reverse(candidates.begin(), candidates.end());
      // Look into cached segments first, otherwise we'll evict some just to
      // swap them in later again.
      auto cached = [&](auto& x) { return self->state.cache.count(x) > 0; };
      std::stable_partition(candidates.begin(), candidates.end(), cached);
      // Proceed with event extraction.
      std::vector<event> result;
      for (auto& candidate : candidates) {
        auto xs = expected<std::vector<event>>{no_error};
        if (candidate == self->state.builder_id) {
          VAST_DEBUG(self, "looks into segment builder");
          xs = self->state.builder.get(bm);
        } else {
          VAST_DEBUG(self, "looks into segment cache");
          auto i = self->state.cache.find(candidate);
          if (i == self->state.cache.end()) {
            VAST_DEBUG(self, "got cache miss for segment", candidate);
            auto chk = chunk::mmap(dir / to_string(candidate));
            if (!chk)
              return rp.deliver(make_error(ec::unspecified, "chunk::mmap"));
            i = self->state.cache.emplace(candidate, segment_viewer{chk}).first;
          }
          xs = i->second.get(bm);
        }
        if (!xs)
          return rp.deliver(xs.error());
        VAST_DEBUG(self, "extracted", xs->size(), "events");
        result.reserve(result.size() + xs->size());
        result.insert(result.end(),
                      std::make_move_iterator(xs->begin()),
                      std::make_move_iterator(xs->end()));
      }
      VAST_DEBUG(self, "delivers", result.size(), "events");
      rp.deliver(std::move(result));
      return rp;
    },
  };
}

} // namespace system
} // namespace vast

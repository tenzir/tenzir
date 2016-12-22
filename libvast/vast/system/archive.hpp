#ifndef VAST_SYSTEM_ARCHIVE_HPP
#define VAST_SYSTEM_ARCHIVE_HPP

#include <map>
#include <vector>

#include <caf/all.hpp>

#include "vast/aliases.hpp"
#include "vast/batch.hpp"
#include "vast/detail/cache.hpp"
#include "vast/detail/range_map.hpp"
#include "vast/die.hpp"
#include "vast/event.hpp"
#include "vast/filesystem.hpp"
#include "vast/uuid.hpp"
#include "vast/compression.hpp"

#include "vast/system/atoms.hpp"
#include "vast/system/accountant.hpp"

namespace vast {
namespace system {

/// A sequence of batches.
class segment {
public:
  using magic_type = uint32_t;
  using version_type = uint32_t;

  static constexpr magic_type magic = 0x2a2a2a2a;
  static constexpr version_type version = 1;

  void add(batch&& b);

  expected<std::vector<event>> extract(bitmap const& bm) const;

  uuid const& id() const;

  template <class Inspector>
  friend auto inspect(Inspector& f, segment& s) {
    return f(s.batches_, s.bytes_, s.id_);
  }

  friend uint64_t bytes(segment const& s);

private:
  // TODO: use a vector & binary_search for O(1) append and O(log N) search.
  std::map<event_id, batch> batches_;
  uint64_t bytes_ = 0;
  uuid id_ = uuid::random();
};

struct archive_state {
  path dir;
  uint64_t max_segment_size;
  compression method;
  detail::range_map<event_id, uuid> segments;
  detail::cache<uuid, segment> cache;
  segment active;
  accountant_type accountant;
  char const* name = "archive";
};

using archive_type = caf::typed_actor<
  caf::reacts_to<shutdown_atom>,
  caf::reacts_to<accountant_type>,
  caf::reacts_to<std::vector<event>>,
  caf::replies_to<flush_atom>::with<ok_atom>,
  caf::replies_to<bitmap>::with<std::vector<event>>
>;

/// The *ARCHIVE* stores raw events in the form of compressed batches and
/// answers queries for specific bitmaps.
/// @param self The actor handle.
/// @param dir The root directory of the archive.
/// @param capacity The number of segments to cache in memory.
/// @param max_segment_size The maximum segment size in bytes.
/// @pre `max_segment_size > 0`
archive_type::behavior_type
archive(archive_type::stateful_pointer<archive_state> self, path dir,
        size_t capacity, size_t max_segment_size);

} // namespace system
} // namespace vast

#endif

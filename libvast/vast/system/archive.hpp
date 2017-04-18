#ifndef VAST_SYSTEM_ARCHIVE_HPP
#define VAST_SYSTEM_ARCHIVE_HPP

#include <map>
#include <vector>

#include <caf/all.hpp>

#include "vast/aliases.hpp"
#include "vast/event.hpp"
#include "vast/filesystem.hpp"
#include "vast/segment.hpp"
#include "vast/uuid.hpp"

#include "vast/system/atoms.hpp"

#include "vast/detail/cache.hpp"
#include "vast/detail/range_map.hpp"

namespace vast {
namespace system {

struct archive_state {
  detail::range_map<event_id, uuid> segments;
  detail::cache<uuid, segment_viewer> cache;
  uuid builder_id = uuid::random();
  segment_builder builder;
  char const* name = "archive";
};

using archive_type = caf::typed_actor<
  caf::reacts_to<std::vector<event>>,
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

#ifndef VAST_ACTOR_ARCHIVE_H
#define VAST_ACTOR_ARCHIVE_H

#include <unordered_map>

#include "vast/aliases.h"
#include "vast/chunk.h"
#include "vast/filesystem.h"
#include "vast/uuid.h"
#include "vast/actor/atoms.h"
#include "vast/actor/basic_state.h"
#include "vast/actor/accountant.h"
#include "vast/io/compression.h"
#include "vast/util/cache.h"
#include "vast/util/flat_set.h"
#include "vast/util/range_map.h"

namespace vast {

/// A key-value store for events operating at the granularity of chunks.
struct archive {
  struct chunk_compare {
    bool operator()(chunk const& lhs, chunk const& rhs) const {
      return lhs.meta().ids.find_first() < rhs.meta().ids.find_first();
    };
  };

  using segment = util::flat_set<chunk, chunk_compare>;

  struct state : basic_state {
    state(local_actor* self);

    trial<void> flush();

    path dir;
    size_t max_segment_size;
    io::compression compression;
    util::range_map<event_id, uuid> segments;
    util::cache<uuid, segment> cache;
    segment current;
    uint64_t current_size;
    accountant::type accountant;
  };

  using type = typed_actor<
    reacts_to<accountant::type>,
    reacts_to<std::vector<event>>,
    replies_to<flush_atom>::with_either<ok_atom>::or_else<error>,
    replies_to<event_id>::with_either<chunk>::or_else<empty_atom, event_id>
  >;

  using behavior = type::behavior_type;
  using stateful_pointer = type::stateful_pointer<state>;

  /// Spawns the archive.
  /// @param self The actor handle.
  /// @param dir The root directory of the archive.
  /// @param capacity The number of segments to hold in memory.
  /// @param max_segment_size The maximum size in MB of a segment.
  /// @param compression The compression method to use for chunks.
  /// @pre `max_segment_size > 0`
  static behavior make(stateful_pointer self, path dir, size_t capacity,
                       size_t max_segment_size, io::compression);
};

} // namespace vast

#endif

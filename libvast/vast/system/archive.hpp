#ifndef VAST_SYSTEM_ARCHIVE_HPP
#define VAST_SYSTEM_ARCHIVE_HPP

#include <unordered_map>

#include "vast/aliases.hpp"
#include "vast/batch.hpp"
#include "vast/filesystem.hpp"
#include "vast/uuid.hpp"
#include "vast/actor/atoms.hpp"
#include "vast/actor/basic_state.hpp"
#include "vast/actor/accountant.hpp"
#include "vast/io/compression.hpp"
#include "vast/util/cache.hpp"
#include "vast/util/flat_set.hpp"
#include "vast/util/range_map.hpp"

namespace vast {
namespace system {

/// A key-value store for bulk events operating at the granularity of batches.
struct archive {
  struct batch_compare {
    bool operator()(batch const& lhs, batch const& rhs) const {
      return lhs.meta().ids.find_first() < rhs.meta().ids.find_first();
    };
  };

  using segment = util::flat_set<batch, batch_compare>;

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
    replies_to<event_id>::with_either<batch>::or_else<empty_atom, event_id>
  >;

  using behavior = type::behavior_type;
  using stateful_pointer = type::stateful_pointer<state>;

  /// Spawns the archive.
  /// @param self The actor handle.
  /// @param dir The root directory of the archive.
  /// @param capacity The number of segments to hold in memory.
  /// @param max_segment_size The maximum size in MB of a segment.
  /// @param compression The compression method to use for batches.
  /// @pre `max_segment_size > 0`
  static behavior make(stateful_pointer self, path dir, size_t capacity,
                       size_t max_segment_size, io::compression);
};

} // namespace system
} // namespace vast

#endif

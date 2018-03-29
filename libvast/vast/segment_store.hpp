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

#pragma once

#include <map>

#include "vast/batch.hpp"
#include "vast/filesystem.hpp"
#include "vast/store.hpp"
#include "vast/uuid.hpp"

#include "vast/detail/cache.hpp"
#include "vast/detail/range_map.hpp"

namespace vast {

/// A store that keeps its data in terms of segments.
class segment_store : public store {
public:
  class segment {
  public:
    using magic_type = uint32_t;
    using version_type = uint32_t;

    static inline constexpr magic_type magic = 0x2a2a2a2a;
    static inline constexpr version_type version = 1;

    void add(batch&& x);

    expected<std::vector<event>> extract(const ids& xs) const;

    const uuid& id() const;

    template <class Inspector>
    friend auto inspect(Inspector& f, segment& x) {
      return f(x.batches_, x.bytes_, x.id_);
    }

    friend uint64_t bytes(const segment& x);

  private:
    // TODO: use a vector & binary_search for O(1) append and O(log N) search.
    std::map<event_id, batch> batches_;
    uint64_t bytes_ = 0;
    uuid id_ = uuid::random();
  };

  /// Constructs a segment store.
  /// @param dir The directory where to store state.
  /// @param max_segment_size The maximum segment size in bytes.
  /// @param in_memory_segments The number of semgents to cache in memory.
  /// @pre `max_segment_size > 0`
  segment_store(path dir, size_t max_segment_size, size_t in_memory_segments);

  expected<void> put(const std::vector<event>& xs) override;

  expected<std::vector<event>> get(const ids& xs) override;

  expected<void> flush() override;

private:
  path dir_;
  uint64_t max_segment_size_;
  detail::range_map<event_id, uuid> segments_;
  detail::cache<uuid, segment> cache_;
  segment active_;
};

} // namespace vast


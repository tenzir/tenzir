//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/detail/cache.hpp"
#include "vast/detail/range_map.hpp"
#include "vast/segment.hpp"
#include "vast/segment_builder.hpp"
#include "vast/uuid.hpp"

// std::vector<table_slice> needs the definition on older versions of libstdc++.
#if VAST_GCC && __GNUC__ <= 8
#  include "vast/table_slice.hpp"
#endif

#include <filesystem>

namespace vast {

/// @relates segment_store
using segment_store_ptr = std::unique_ptr<segment_store>;

/// A store that keeps its data in terms of segments.
class segment_store {
public:
  // -- helper types -----------------------------------------------------------

  /// A session type for managing the state of a lookup.
  class lookup {
  public:
    using uuid_iterator = std::vector<uuid>::iterator;

    lookup(const segment_store& store, ids xs, std::vector<uuid>&& candidates);

    caf::expected<table_slice> next();

  private:
    caf::expected<std::vector<table_slice>> handle_segment();

    const segment_store& store_;
    ids xs_;
    std::vector<uuid> candidates_;
    uuid_iterator first_ = candidates_.begin();
    caf::expected<std::vector<table_slice>> buffer_{caf::no_error};
    std::vector<table_slice>::iterator it_;
  };

  // -- constructors, destructors, and assignment operators --------------------

  /// Constructs a segment store.
  /// @param dir The directory where to store state.
  /// @param max_segment_size The maximum segment size in bytes.
  /// @param in_memory_segments The number of semgents to cache in memory.
  /// @pre `max_segment_size > 0`
  static segment_store_ptr
  make(std::filesystem::path dir, size_t max_segment_size,
       size_t in_memory_segments);

  // -- properties -------------------------------------------------------------

  /// @returns the path for storing the segments.
  std::filesystem::path segment_path() const {
    return dir_ / "segments";
  }

  /// @returns whether the store has no unwritten data pending.
  bool dirty() const noexcept {
    return builder_.table_slice_bytes() != 0;
  }

  /// @returns the ID of the active segment.
  const uuid& active_id() const noexcept {
    return builder_.id();
  }

  /// @returns whether `x` is currently a cached segment.
  bool cached(const uuid& x) const noexcept {
    return cache_.count(x) != 0;
  }

  // -- cache management -------------------------------------------------------

  /// Evicts all segments from the cache.
  void clear_cache() {
    cache_.clear();
  }

  // -- implementation of store ------------------------------------------------

  caf::error put(table_slice xs);

  std::unique_ptr<lookup> extract(const ids& xs) const;

  caf::error erase(const ids& xs);

  caf::expected<std::vector<table_slice>> get(const ids& xs);

  caf::error flush();

  void inspect_status(caf::settings& xs, system::status_verbosity v);

private:
  segment_store(std::filesystem::path dir, uint64_t max_segment_size,
                size_t in_memory_segments);

  // -- utility functions ------------------------------------------------------

  caf::error register_segments();

  caf::error register_segment(const std::filesystem::path& filename);

  caf::expected<segment> load_segment(uuid id) const;

  /// Fills `candidates` with all segments that qualify for `selection`.
  caf::error
  select_segments(const ids& selection, std::vector<uuid>& candidates) const;

  /// Drops an entire segment and erases its content from disk.
  /// @param x The segment to drop.
  /// @returns The number of events in `x`.
  uint64_t drop(segment& x);

  /// Drops a segment-under-construction by resetting the builder and forcing
  /// it to generate a new segment ID.
  /// @param x The segment-under-construction to drop.
  /// @returns The number of events in `x`.
  uint64_t drop(segment_builder& x);

  // -- member variables -------------------------------------------------------

  /// Identifies the base directory for segments.
  std::filesystem::path dir_;

  /// Configures the limit each segment until we seal and flush it.
  uint64_t max_segment_size_;

  uint64_t num_events_ = 0;

  /// Maps event IDs to candidate segments.
  detail::range_map<id, uuid> segments_;

  /// Optimizes access times into segments by keeping some segments in memory.
  mutable detail::cache<uuid, segment> cache_;

  /// Serializes table slices into contiguous chunks of memory.
  segment_builder builder_;
};

} // namespace vast

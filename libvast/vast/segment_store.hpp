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

#include <caf/fwd.hpp>

#include "vast/filesystem.hpp"
#include "vast/fwd.hpp"
#include "vast/segment.hpp"
#include "vast/segment_builder.hpp"
#include "vast/store.hpp"
#include "vast/uuid.hpp"

#include "vast/detail/cache.hpp"
#include "vast/detail/range_map.hpp"

namespace vast {

/// @relates segment_store
using segment_store_ptr = std::unique_ptr<segment_store>;

/// A store that keeps its data in terms of segments.
class segment_store : public store {
public:
  /// Constructs a segment store.
  /// @param dir The directory where to store state.
  /// @param max_segment_size The maximum segment size in bytes.
  /// @param in_memory_segments The number of semgents to cache in memory.
  /// @pre `max_segment_size > 0`
  static segment_store_ptr make(path dir, size_t max_segment_size,
                                size_t in_memory_segments);

  ~segment_store();

  error put(table_slice_ptr xs) override;

  std::unique_ptr<store::lookup> extract(const ids& xs) const override;

  caf::error erase(const ids& xs) override;

  caf::expected<std::vector<table_slice_ptr>> get(const ids& xs) override;

  caf::error flush() override;

  void inspect_status(caf::settings& dict) override;

  /// @cond PRIVATE

  segment_store(path dir, uint64_t max_segment_size, size_t in_memory_segments);

  /// @endcond

private:
  path meta_path() const {
    return dir_ / "meta";
  }

  path segment_path() const {
    return dir_ / "segments";
  }

  caf::expected<segment_ptr> load_segment(uuid id) const;

  path dir_;
  uint64_t max_segment_size_;
  detail::range_map<id, uuid> segments_;
  mutable detail::cache<uuid, segment_ptr> cache_;
  segment_builder builder_;
  std::vector<segment_ptr> builder_slices_;
};

} // namespace vast

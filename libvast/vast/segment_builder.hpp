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

#include <cstddef>
#include <vector>

#include <caf/expected.hpp>
#include <caf/fwd.hpp>

#include "vast/aliases.hpp"
#include "vast/segment.hpp"
#include "vast/uuid.hpp"

namespace vast {

/// A builder to create a segment from table slices.
/// @relates segment
class segment_builder {
public:
  /// Constructs a segment builder.
  segment_builder();

  /// Adds a table slice to the segment.
  /// @returns An error if adding the table slice failed.
  /// @pre The table slice offset (`x.offset()`) must be greater than the
  ///      offset of the previously added table slice. This requirement enables
  ///      efficient lookup of table slices from a sequence of IDs.
  caf::error add(table_slice_ptr x);

  /// Constructs a segment from previously added table slices.
  /// @post The builder can now be reused to contruct a new segment.
  segment_ptr finish();

  /// Locates previously added table slices for a given set of IDs.
  /// @param xs The IDs to lookup.
  /// @returns The table slices according to *xs*.
  caf::expected<std::vector<table_slice_ptr>>
  lookup(const ids& xs) const;

  /// @returns The UUID for the segment under construction.
  const uuid& id() const;

  /// @returns The number of bytes of the current segment.
  size_t table_slice_bytes() const;

  /// @returns the event IDs of each stored table slice.
  std::vector<ids> get_slice_ids() const {
    return meta_.get_slice_ids();
  }

  /// Resets the builder state to start with a new segment.
  void reset();

private:
  // Segment state
  segment::meta_data meta_;
  uuid id_;
  // Table slice state
  vast::id min_table_slice_offset_;
  std::vector<char> table_slice_buffer_;
  // Lookup cache
  std::vector<table_slice_ptr> slices_;
};

} // namespace vast

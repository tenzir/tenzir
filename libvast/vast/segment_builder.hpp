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

#include <caf/actor_system.hpp>
#include <caf/expected.hpp>
#include <caf/fwd.hpp>
#include <caf/stream_serializer.hpp>
#include <caf/streambuf.hpp>

#include "vast/aliases.hpp"
#include "vast/segment.hpp"
#include "vast/uuid.hpp"

namespace vast {

/// A builder to create a segment from table slices.
/// @relates segment
class segment_builder {
public:
  /// Constructs a segment builder.
  /// @param sys The actor system used to construct segments (and deserialize
  ///            table slices).
  segment_builder(caf::actor_system& sys);

  /// Adds a table slice to the segment.
  /// @returns An error if adding the table slice failed.
  /// @pre The table slice offset (`x.offset()`) must be greater than the
  ///      offset of the previously added table slice. This requirement enables
  ///      efficient lookup of table slices from a sequence of IDs.
  caf::error add(const_table_slice_handle x);

  /// Constructs a segment from previously added table slices.
  /// @post The builder can now be reused to contruct a new segment.
  caf::expected<segment_ptr> finish();

  /// @returns The UUID for the segment under construction.
  const uuid& id() const;

  /// @returns The number of bytes of the current segment.
  size_t table_slice_bytes() const;

private:
  // Resets the builder state to start with a new segment.
  void reset();

  caf::actor_system& actor_system_;
  // Segment state
  std::vector<char> segment_buffer_;
  segment::meta_data meta_;
  uuid id_;
  // Table slice state
  vast::id min_table_slice_offset_;
  std::vector<char> table_slice_buffer_;
  caf::vectorbuf table_slice_streambuf_;
  caf::stream_serializer<caf::vectorbuf&> table_slice_serializer_;
};

} // namespace vast

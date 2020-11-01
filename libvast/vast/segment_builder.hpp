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

#include "vast/aliases.hpp"
#include "vast/fbs/segment.hpp"
#include "vast/fbs/table_builder.hpp"
#include "vast/fbs/table_slice.hpp"
#include "vast/segment.hpp"
#include "vast/uuid.hpp"

#include <caf/expected.hpp>
#include <caf/fwd.hpp>

#include <cstddef>
#include <vector>

namespace vast {

/// Defer table_builder template instantiation for segment.
extern template class fbs::table_builder<segment, fbs::SegmentIdentifier>;

/// A builder to create a segment from table slices.
/// @relates segment
class segment_builder final
  : public fbs::table_builder<segment, fbs::SegmentIdentifier> {
public:
  /// Constructs a segment builder.
  segment_builder() noexcept;

  /// Adds a table slice to the segment.
  /// @returns An error if adding the table slice failed.
  /// @pre The table slice offset (`slice.offset()`) must be greater than the
  ///      offset of the previously added table slice. This requirement enables
  ///      efficient lookup of table slices from a sequence of IDs.
  caf::error add(table_slice_ptr slice);

  /// Locates previously added table slices for a given set of IDs.
  /// @param xs The IDs to lookup.
  /// @returns The table slices according to *xs*.
  caf::expected<std::vector<table_slice_ptr>> lookup(const vast::ids& xs) const;

  /// @returns The UUID for the segment under construction.
  const uuid& id() const;

  /// @returns The IDs for the contained table slices.
  vast::ids ids() const;

  /// @returns The currently buffered table slices.
  const std::vector<table_slice_ptr>& slices() const;

private:
  // -- implementation details -------------------------------------------------

  void do_reset() override;

  offset_type create() override;

  uuid id_ = uuid::random();
  vast::id min_table_slice_offset_ = 0;
  uint64_t num_events_ = 0;
  std::vector<table_slice_ptr> slices_ = {};
  std::vector<flatbuffers::Offset<fbs::table_slice_buffer::v0>> flat_slices_
    = {};
  std::vector<fbs::interval::v0> intervals_ = {};
};

} // namespace vast

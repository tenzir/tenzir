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

#include "vast/fbs/table.hpp"
#include "vast/fwd.hpp"

#include <vector>

namespace vast {

/// Defer table template instantiation for segment.
extern template class fbs::table<segment, fbs::Segment, fbs::SegmentIdentifier>;

/// A sequence of table slices.
class segment final
  : public fbs::table<segment, fbs::Segment, fbs::SegmentIdentifier> {
public:
  /// Selectively opt-in to the table interface.
  using table::add_deletion_step;
  using table::table;

  /// @returns The unique ID of this segment.
  uuid id() const;

  /// @returns the event IDs of all contained table slice.
  vast::ids ids() const;

  // @returns The number of table slices in this segment.
  size_t num_slices() const;

  /// Locates the table slices for a given set of IDs.
  /// @param xs The IDs to lookup.
  /// @returns The table slices according to *xs*.
  caf::expected<std::vector<table_slice_ptr>> lookup(const vast::ids& xs) const;
};

} // namespace vast

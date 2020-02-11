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
#include "vast/chunk.hpp"
#include "vast/fwd.hpp"
#include "vast/ids.hpp"
#include "vast/segment_header.hpp"
#include "vast/uuid.hpp"

#include <caf/expected.hpp>
#include <caf/fwd.hpp>
#include <caf/intrusive_ptr.hpp>
#include <caf/ref_counted.hpp>

#include <cstdint>
#include <iterator>
#include <memory>
#include <vector>

namespace vast {

/// @relates segment
using segment_ptr = caf::intrusive_ptr<segment>;

// TODO: remove the reference semantics and make this a plain value. The
// intrusive pointer is not longer needed since the segment only wraps a
// reference-counted pointer now.
/// A sequence of table slices.
class segment : public caf::ref_counted {
  friend segment_builder;

public:
  using value_type = table_slice_ptr;
  using size_type = size_t;

  /// Constructs a segment.
  /// @param header The header of the segment.
  /// @param chunk The chunk holding the segment data.
  static segment_ptr make(chunk_ptr chunk);

  /// @returns The unique ID of this segment.
  uuid id() const;

  /// @returns the event IDs of all contained table slice.
  vast::ids ids() const;

  // Alias for size().
  size_t num_slices() const;

  /// @returns The underlying chunk.
  chunk_ptr chunk() const;

  /// Locates the table slices for a given set of IDs.
  /// @param xs The IDs to lookup.
  /// @returns The table slices according to *xs*.
  caf::expected<std::vector<table_slice_ptr>> lookup(const vast::ids& xs) const;

  // -- concepts --------------------------------------------------------------

  /// @pre `x != nullptr`
  friend caf::error inspect(caf::serializer& sink, const segment_ptr& x);

  friend caf::error inspect(caf::deserializer& source, segment_ptr& x);

  /// @pre `x != nullptr`
  friend caf::error save(const path& filename, const segment_ptr& x);

  friend caf::error load(const path& filename, segment_ptr& x);

private:
  segment() = default;

  explicit segment(chunk_ptr chk);

  chunk_ptr chunk_;
};

} // namespace vast

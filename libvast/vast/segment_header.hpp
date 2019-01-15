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

#include <cstdint>

#include "vast/uuid.hpp"

namespace vast {

/// @relates segment_header
using segment_magic_type = uint32_t;

/// @relates segment_header
using segment_version_type = uint32_t;

/// The header of a segment.
/// @relates segment
struct segment_header {
  segment_magic_type magic;       ///< Magic constant to identify segments.
  segment_version_type version;   ///< Version of the segment format.
  uuid id;                        ///< The UUID of the segment.
  uint64_t payload_offset;        ///< The offset to the table slices.
};

// Guarantee proper layout of the header, since we're going to rely on its
// in-memory representation.
static_assert(sizeof(segment_header) == 32);

/// @relates segment_header
template <class Inspector>
auto inspect(Inspector& f, segment_header& x) {
  return f(x.magic, x.version, x.id, x.payload_offset);
}

} // namespace vast

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
#include <memory>
#include <vector>

#include <caf/actor_system.hpp>
#include <caf/expected.hpp>
#include <caf/fwd.hpp>
#include <caf/intrusive_ptr.hpp>
#include <caf/ref_counted.hpp>
#include <caf/stream_serializer.hpp>
#include <caf/streambuf.hpp>

#include "vast/aliases.hpp"
#include "vast/chunk.hpp"
#include "vast/fwd.hpp"
#include "vast/uuid.hpp"

namespace vast {

// TODO: use a format that's more conducive to mmap'ing, e.g., flatbuffers.
// Right now we use a packed struct for the header as a poor-man's abstraction
// for this, but it's inconvenient.

/// @relates segment
using segment_ptr = caf::intrusive_ptr<segment>;

/// A sequence of [@ref table_slice](table slices) optimized for persistent
/// storage. The layout has the following format:
///
///               +--------------------+--------------------+
///               |       magic        |      version       | \
///               +--------------------+--------------------+ |
///               |                 segment                 | | fixed size
///               |                  UUID                   | /
///               +-----------------------------------------+ |
///            +--|             table slice offset          | /
///            |  +-----------------------------------------+
///            |  .                                         . \
///            |  .                meta data                . | variable size
///            |  .                                         . /
///            |  +-----------------------------------------+
///            +->.                                         . \
///               .                                         . |
///               .               table slices              . | variable size
///               .                                         . |
///               .                                         . /
///               +-----------------------------------------+
///
class segment : public caf::ref_counted {
  friend segment_builder;

public:
  using magic_type = uint32_t;
  using version_type = uint32_t;

  /// A magic constant that identifies segment files. The four bytes represent
  /// the multiplication of the vector `(1, 2, 3, 4)` with the value `42`,
  /// converted to hex bytes: `42 * (1, 2, 3, 4) = [2a, 65, 7e, a8]`.
  static inline constexpr magic_type magic = 0x2a547ea8;

  /// The current version of the segment format.
  static inline constexpr version_type version = 1;

  /// The fixed-size header for every segment.
  struct header {
    magic_type magic;        ///< Magic constant to identify segments.
    version_type version;    ///< Version of the segment format.
    uuid id;                 ///< The UUID of the segment.
    uint64_t payload_offset; ///< The offset to the table slices.
  };

  // Guarantee proper layout of the header, since we're going to rely on its
  // in-memory representation.
  static_assert(sizeof(header) == 32);

  /// Per-slice meta data.
  struct table_slice_synopsis {
    int64_t start;    ///< The byte offset from the beginning of the payload.
    int64_t end;      ///< The byte offset to one past the end of the slice.
    id offset;        ///< The offset in the ID space where the slice starts.
    uint64_t size;    ///< The number of rows in the slice.
  };

  /// Meta data for a segment.
  struct meta_data {
    std::vector<table_slice_synopsis> slices;
  };

  /// Constructs a segment.
  /// @param sys The actor system that stores factory to deserialize table
  ///            slices.
  /// @param chunk The chunk holding the segment data.
  static caf::expected<segment_ptr> make(caf::actor_system& sys, chunk_ptr chunk);

  /// @returns The unique ID of this segment.
  const uuid& id() const;

  /// @returns The underlying chunk.
  chunk_ptr chunk() const;

  /// @returns the number of tables slices in the segment.
  size_t num_slices() const;

  /// Locates the table slices for a given set of IDs.
  /// @param xs The IDs to lookup.
  /// @returns The table slices according to *xs*.
  caf::expected<std::vector<const_table_slice_handle>>
  lookup(const ids& xs) const;

  /// @cond PRIVATE

  segment(caf::actor_system& sys, chunk_ptr chunk);

  /// @endcond

private:
  caf::expected<const_table_slice_handle>
  make_slice(const table_slice_synopsis& slice) const;

  caf::actor_system& actor_system_;
  chunk_ptr chunk_;
  header header_;
  meta_data meta_;
};

/// @relates segment::header
template <class Inspector>
auto inspect(Inspector& f, segment::header& x) {
  return f(x.magic, x.version, x.id, x.payload_offset);
}

/// @relates segment::table_slice_synopsis
template <class Inspector>
auto inspect(Inspector& f, segment::table_slice_synopsis& x) {
  return f(x.start, x.end, x.offset, x.size);
}

/// @relates segment::meta_data
template <class Inspector>
auto inspect(Inspector& f, segment::meta_data& x) {
  return f(x.slices);
}

/// @relates segment
/// @pre `x != nullptr`
caf::error inspect(caf::serializer& sink, const segment_ptr& x);

/// @relates segment
caf::error inspect(caf::deserializer& source, segment_ptr& x);

} // namespace vast

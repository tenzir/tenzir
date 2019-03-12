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

#include <caf/expected.hpp>
#include <caf/fwd.hpp>
#include <caf/intrusive_ptr.hpp>
#include <caf/ref_counted.hpp>

#include "vast/aliases.hpp"
#include "vast/chunk.hpp"
#include "vast/fwd.hpp"
#include "vast/ids.hpp"
#include "vast/segment_header.hpp"
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
///               |       magic        |      version       | ^
///               +--------------------+--------------------+ |
///               |                 segment                 | | segment header
///               |                  UUID                   | v
///               +-----------------------------------------+
///               .                                         . ^
///               .                meta data                . | variable size
///               .                                         . v
///               +-----------------------------------------+
///               .                                         . ^
///               .                                         . |
///               .               table slices              . | variable size
///               .                                         . |
///               .                                         . v
///               +-----------------------------------------+
///
class segment : public caf::ref_counted {
  friend segment_builder;

public:
  /// A magic constant that identifies segment files. The four bytes represent
  /// the multiplication of the vector `(1, 2, 3, 4)` with the value `42`,
  /// converted to hex bytes: `42 * (1, 2, 3, 4) = [2a, 65, 7e, a8]`.
  static inline constexpr segment_magic_type magic = 0x2a547ea8;

  /// The current version of the segment format.
  static inline constexpr segment_version_type version = 1;

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

    /// Visits all ID ranges of all table slices.
    template <class F>
    void visit_ids(F fun) const {
      for (auto& synopsis : slices) {
        auto ids_begin = synopsis.offset;
        auto ids_end = ids_begin + synopsis.size;
        fun(make_ids({{ids_begin, ids_end}}));
      }
    }

    /// @returns the event IDs of each stored table slice in a single bitmap.
    ids get_flat_slice_ids() const;

    /// @returns the event IDs of each stored table slice.
    std::vector<ids> get_slice_ids() const;
  };

  /// Constructs a segment.
  /// @param header The header of the segment.
  /// @param chunk The chunk holding the segment data.
  static segment_ptr make(chunk_ptr chunk);

  /// @returns The unique ID of this segment.
  const uuid& id() const;

  /// @returns The underlying chunk.
  chunk_ptr chunk() const;

  /// @returns the number of tables slices in the segment.
  size_t num_slices() const;

  /// Locates the table slices for a given set of IDs.
  /// @param xs The IDs to lookup.
  /// @returns The table slices according to *xs*.
  caf::expected<std::vector<table_slice_ptr>>
  lookup(const ids& xs) const;

  /// @returns the meta data for the segment.
  const auto& meta() const {
    return meta_;
  }

  // -- concepts --------------------------------------------------------------

  /// @pre `x != nullptr`
  friend caf::error inspect(caf::serializer& sink, const segment_ptr& x);

  friend caf::error inspect(caf::deserializer& source, segment_ptr& x);

private:
  segment() = default;

  caf::expected<table_slice_ptr>
  make_slice(const table_slice_synopsis& slice) const;

  meta_data meta_;
  chunk_ptr chunk_;
  segment_header header_;
};

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

} // namespace vast

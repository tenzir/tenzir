//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/aliases.hpp"
#include "vast/chunk.hpp"
#include "vast/flatbuffer.hpp"
#include "vast/ids.hpp"
#include "vast/uuid.hpp"

#include <caf/expected.hpp>
#include <caf/fwd.hpp>

#include <cstdint>
#include <iterator>
#include <memory>
#include <vector>

namespace vast {

/// A sequence of table slices.
class segment {
  friend segment_builder;

public:
  /// Constructs a segment.
  /// @param chunk The chunk holding the segment data.
  static caf::expected<segment> make(chunk_ptr&& chunk);

  /// Create a new segment that is a copy of the given segment excluding
  /// the given ids. The returned segment will have the same segment id
  /// as the original.
  static caf::expected<segment>
  copy_without(const vast::segment&, const vast::ids&);

  /// @returns The unique ID of this segment.
  [[nodiscard]] uuid id() const;

  /// @returns the event IDs of all contained table slice.
  [[nodiscard]] vast::ids ids() const;

  // @returns The number of table slices in this segment.
  [[nodiscard]] size_t num_slices() const;

  /// @returns The underlying chunk.
  [[nodiscard]] chunk_ptr chunk() const;

  /// Locates the table slices for a given set of IDs.
  /// @param xs The IDs to lookup.
  /// @returns The table slices according to *xs*.
  [[nodiscard]] caf::expected<std::vector<table_slice>>
  lookup(const vast::ids& xs) const;

  /// Creates new table slices that contain all events *not*
  /// included in `xs`.
  /// @param xs The IDs to exclude.
  [[nodiscard]] caf::expected<std::vector<table_slice>>
  erase(const vast::ids& xs) const;

private:
  explicit segment(flatbuffer<fbs::Segment> flatbuffer);

  flatbuffer<fbs::Segment> flatbuffer_ = {};
};

} // namespace vast

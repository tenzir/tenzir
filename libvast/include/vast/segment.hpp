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
#include "vast/detail/iterator.hpp"
#include "vast/fbs/flatbuffer_container.hpp"
#include "vast/fbs/segment.hpp"
#include "vast/flatbuffer.hpp"
#include "vast/ids.hpp"
#include "vast/table_slice.hpp"
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
public:
  class iterator
    : public detail::iterator_facade<
        iterator, table_slice, std::random_access_iterator_tag, table_slice> {
    friend detail::iterator_access;
    using flat_slice_iterator = flatbuffers::Vector<
      flatbuffers::Offset<fbs::FlatTableSlice>>::const_iterator;
    using interval_iterator
      = flatbuffers::Vector<const fbs::uinterval*>::const_iterator;

  public:
    iterator(size_t slice_idx, interval_iterator intervals,
             const segment* parent);

    [[nodiscard]] table_slice dereference() const;

    void increment();

    void decrement();

    void advance(size_t n);

    [[nodiscard]] bool equals(iterator other) const;

    [[nodiscard]] segment::iterator::difference_type
    distance_to(iterator other) const;

  private:
    size_t slice_idx_;
    interval_iterator intervals_;
    const segment* parent_;
  };

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

  /// @returns The event IDs of all contained table slice.
  [[nodiscard]] vast::ids ids() const;

  /// @returns The number of table slices in this segment.
  [[nodiscard]] size_t num_slices() const;

  /// @returns An iterator pointing to the first slice in the segment.
  [[nodiscard]] iterator begin() const;

  /// @returns An iterator pointing to the end of the segment.
  [[nodiscard]] iterator end() const;

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

  explicit segment(fbs::flatbuffer_container container);

  [[nodiscard]] std::vector<const vast::fbs::FlatTableSlice*>
  flat_slices_() const;

  [[nodiscard]] vast::table_slice get_slice_(size_t idx) const;

  flatbuffer<fbs::Segment> flatbuffer_ = {};

  // Optionally, a container to store the table slices that
  // exceed 2GiB.
  std::optional<fbs::flatbuffer_container> container_ = std::nullopt;
};

} // namespace vast

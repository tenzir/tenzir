//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/aliases.hpp"
#include "vast/fbs/segment.hpp"
#include "vast/fbs/table_slice.hpp"
#include "vast/segment.hpp"
#include "vast/uuid.hpp"

#include <caf/expected.hpp>
#include <caf/fwd.hpp>

#include <cstddef>
#include <vector>

namespace vast {

/// A builder to create a segment from table slices.
/// @relates segment
class segment_builder {
public:
  /// Constructs a segment builder.
  /// @param id The id of the new segment. If not provided, a random
  ///           uuid will be generated.
  explicit segment_builder(size_t initial_buffer_size,
                           const std::optional<uuid>& id = std::nullopt);

  /// Adds a table slice to the segment.
  /// @returns An error if adding the table slice failed.
  /// @pre The table slice offset (`x.offset()`) must be greater than the
  ///      offset of the previously added table slice. This requirement enables
  ///      efficient lookup of table slices from a sequence of IDs.
  caf::error add(table_slice x);

  /// Constructs a segment from previously added table slices.
  /// @post The builder can now be reused to contruct a new segment.
  segment finish();

  /// Locates previously added table slices for a given set of IDs.
  /// @param xs The IDs to lookup.
  /// @returns The table slices according to *xs*.
  [[nodiscard]] caf::expected<std::vector<table_slice>>
  lookup(const vast::ids& xs) const;

  /// @returns The UUID for the segment under construction.
  [[nodiscard]] const uuid& id() const;

  /// @returns The IDs for the contained table slices.
  [[nodiscard]] vast::ids ids() const;

  /// @returns The number of bytes of the current segment.
  [[nodiscard]] size_t table_slice_bytes() const;

  /// @returns The currently buffered table slices.
  [[nodiscard]] const std::vector<table_slice>& table_slices() const;

  /// Resets the builder state to start with a new segment.
  /// @param id The id of the new segment. If not provided, a random
  ///           uuid will be generated.
  void reset(const std::optional<uuid>& id = std::nullopt);

private:
  uuid id_;
  vast::id min_table_slice_offset_;
  uint64_t num_events_;
  flatbuffers::FlatBufferBuilder builder_;
  std::vector<flatbuffers::Offset<fbs::FlatTableSlice>> flat_slices_;
  std::vector<table_slice> slices_; // For queries to an unfinished segment.
  std::vector<fbs::interval::v0> intervals_;
};

} // namespace vast

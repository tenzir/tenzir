//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/qualified_record_field.hpp"
#include "vast/synopsis.hpp"
#include "vast/table_slice.hpp"

namespace vast {

/// Contains one synopsis per partition column.
struct partition_synopsis {
  /// Add data to the synopsis.
  void add(const table_slice& slice, const caf::settings& synopsis_options);

  /// Optimizes the partition synopsis contents for size.
  /// @related buffered_synopsis
  void shrink();

  /// Estimate the memory footprint of this partition synopsis.
  /// @returns A best-effort estimate of the amount of memory used by this
  ///          synopsis.
  size_t memusage() const;

  /// Id of the first event in the partition.
  uint64_t offset;

  // Number of events in the partition.
  uint64_t events;

  /// Synopsis data structures for types.
  std::unordered_map<type, synopsis_ptr> type_synopses_;

  /// Synopsis data structures for individual columns.
  std::unordered_map<qualified_record_field, synopsis_ptr> field_synopses_;

  // -- flatbuffer -------------------------------------------------------------

  friend caf::expected<flatbuffers::Offset<fbs::partition_synopsis::v0>>
  pack(flatbuffers::FlatBufferBuilder& builder, const partition_synopsis&);

  friend caf::error
  unpack(const fbs::partition_synopsis::v0&, partition_synopsis&);
};

} // namespace vast

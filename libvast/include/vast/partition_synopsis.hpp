//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/flat_map.hpp"
#include "vast/detail/friend_attribute.hpp"
#include "vast/fbs/partition_synopsis.hpp"
#include "vast/index_config.hpp"
#include "vast/index_statistics.hpp"
#include "vast/qualified_record_field.hpp"
#include "vast/sketch/sketch.hpp"
#include "vast/synopsis.hpp"
#include "vast/table_slice.hpp"
#include "vast/uuid.hpp"

namespace caf {

// Forward declaration to be able to befriend the unshare implementation.
template <typename T>
T* default_intrusive_cow_ptr_unshare(T*&);

} // namespace caf

namespace vast {

/// Contains one synopsis per partition column.
struct partition_synopsis final : public caf::ref_counted {
  partition_synopsis() = default;
  ~partition_synopsis() override = default;
  partition_synopsis(const partition_synopsis&) = delete;
  partition_synopsis(partition_synopsis&& that) noexcept;

  // TODO: Split this into an `active_partition_synopsis` for which
  // table slices can be added and which can be shrinked, and a
  // `passive_partition_synopsis` which is just the mmapped flatbuffer.
  partition_synopsis(const index_config&) = delete; // FIXME

  partition_synopsis& operator=(const partition_synopsis&) = delete;
  partition_synopsis& operator=(partition_synopsis&& that) noexcept;

  /// Add data to the synopsis.
  // TODO: It would make sense to pass an index_config to partition synopsis
  // constructor instead.
  void add(const table_slice& slice, size_t partition_capacity,
           const index_config& synopsis_options);

  /// Optimizes the partition synopsis contents for size.
  /// @related buffered_synopsis
  void shrink();

  /// Estimate the memory footprint of this partition synopsis.
  /// @returns A best-effort estimate of the amount of memory used by this
  ///          synopsis.
  size_t memusage() const;

  /// Id of the first event in the partition.
  uint64_t offset = invalid_id;

  // Number of events in the partition.
  uint64_t events = 0;

  /// The minimum import timestamp of all contained table slices.
  time min_import_time = time::max();

  /// The maximum import timestamp of all contained table slices.
  time max_import_time = time::min();

  /// The version number of this partition.
  uint64_t version = version::partition_version;

  /// The schema of this partition. This is only set for partition synopses with
  /// a version >= 1, because they are guaranteed to be homogenous.
  type schema = {};

  /// Whether this partition synopsis holds its data as sketch
  /// or as synopsis.
  //  Only one of these may be nonempty at the same time, and
  //  the "synopses" way will probably be removed in the future.
  //  TODO: We still use `false` as default for unit tests that
  //  construct a `partition_synopsis` directly and don't know
  //  about sketches yet.
  bool use_sketches = false;

  /// Synopsis data structures for types.
  std::unordered_map<type, synopsis_ptr> type_synopses_;

  /// Synopsis data structures for individual columns.
  std::unordered_map<qualified_record_field, synopsis_ptr> field_synopses_;

  /// Sketch data structures for types.
  /// TODO: Measure if flat_map or unordered_map or something else
  /// makes more sense for storage.
  //  FIXME: `type_synopses_` has some users checking for null pointers;
  //  double-check whether that's a mistake or if we need `optional` as key.
  detail::flat_map<type, sketch::sketch> type_sketches_;

  /// Sketch data structures for individual columns.
  detail::flat_map<qualified_record_field, std::optional<sketch::sketch>>
    field_sketches_;

  // -- flatbuffer -------------------------------------------------------------

  FRIEND_ATTRIBUTE_NODISCARD friend caf::expected<
    flatbuffers::Offset<fbs::partition_synopsis::LegacyPartitionSynopsis>>
  pack_legacy(flatbuffers::FlatBufferBuilder& builder,
              const partition_synopsis&);

  FRIEND_ATTRIBUTE_NODISCARD friend caf::expected<
    flatbuffers::Offset<fbs::partition_synopsis::PartitionSynopsisV1>>
  pack(flatbuffers::FlatBufferBuilder& builder, const partition_synopsis&);

  FRIEND_ATTRIBUTE_NODISCARD friend caf::error
  unpack(const fbs::partition_synopsis::LegacyPartitionSynopsis&,
         partition_synopsis&);

  FRIEND_ATTRIBUTE_NODISCARD friend caf::error
  unpack(const fbs::partition_synopsis::LegacyPartitionSynopsis& x,
         partition_synopsis& ps, uint64_t offset, uint64_t events);

  FRIEND_ATTRIBUTE_NODISCARD friend caf::error
  unpack(const fbs::partition_synopsis::PartitionSynopsisV1& x,
         partition_synopsis& ps);

private:
  // Returns a raw pointer to a deep copy of this partition synopsis.
  // For use by the `caf::intrusive_cow_ptr`.
  friend partition_synopsis* ::caf::default_intrusive_cow_ptr_unshare<
    partition_synopsis>(partition_synopsis*& ptr);
  partition_synopsis* copy() const;

  // Cached memory usage.
  mutable std::atomic<size_t> memusage_ = 0ull;
};

/// Some quantitative information about a partition.
struct partition_info {
  static constexpr bool use_deep_to_string_formatter = true;

  /// The partition id.
  vast::uuid uuid = vast::uuid::nil();

  /// Total number of events in the partition. The sum of all
  /// values in `stats`.
  size_t events = 0ull;

  /// The newest import timestamp of the table slices in this partition.
  time max_import_time = {};

  /// The schema of the partition.
  type schema = {};

  friend std::strong_ordering
  operator<=>(const partition_info& lhs, const partition_info& rhs) noexcept {
    return lhs.uuid <=> rhs.uuid;
  }

  friend std::strong_ordering
  operator<=>(const partition_info& lhs, const class uuid& rhs) noexcept {
    return lhs.uuid <=> rhs;
  }

  friend bool
  operator==(const partition_info& lhs, const partition_info& rhs) noexcept {
    return lhs.uuid == rhs.uuid;
  }

  friend bool
  operator==(const partition_info& lhs, const class uuid& rhs) noexcept {
    return lhs.uuid == rhs;
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, partition_info& x) {
    return f(caf::meta::type_name("partition_info"), x.uuid, x.events,
             x.max_import_time, x.schema);
  }
};

/// A partition synopsis with some additional information.
//  A `augmented_partition_synopsis` is only created for new
//  partitions, so it can not be a heterogenous legacy partition
//  but must have exactly one type.
struct augmented_partition_synopsis {
  vast::uuid uuid;
  vast::type type;
  partition_synopsis_ptr synopsis;
};

/// A partition synopsis and a uuid.
struct partition_synopsis_pair {
  vast::uuid uuid;
  partition_synopsis_ptr synopsis;
};

} // namespace vast

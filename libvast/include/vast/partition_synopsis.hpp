//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/friend_attribute.hpp"
#include "vast/fbs/partition_synopsis.hpp"
#include "vast/index_config.hpp"
#include "vast/index_statistics.hpp"
#include "vast/qualified_record_field.hpp"
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
  uint64_t version = version::current_partition_version;

  /// The schema of this partition. This is only set for partition synopses with
  /// a version >= 1, because they are guaranteed to be homogenous.
  type schema = {};

  /// Synopsis data structures for types.
  std::unordered_map<type, synopsis_ptr> type_synopses_;

  /// Synopsis data structures for individual columns.
  std::unordered_map<qualified_record_field, synopsis_ptr> field_synopses_;

  // -- flatbuffer -------------------------------------------------------------

  FRIEND_ATTRIBUTE_NODISCARD friend caf::expected<
    flatbuffers::Offset<fbs::partition_synopsis::LegacyPartitionSynopsis>>
  pack(flatbuffers::FlatBufferBuilder& builder, const partition_synopsis&);

  FRIEND_ATTRIBUTE_NODISCARD friend caf::error
  unpack(const fbs::partition_synopsis::LegacyPartitionSynopsis&,
         partition_synopsis&);

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

  partition_info() noexcept = default;

  partition_info(class uuid uuid, size_t events, time max_import_time,
                 type schema, uint64_t version) noexcept
    : uuid{uuid},
      events{events},
      max_import_time{max_import_time},
      schema{std::move(schema)},
      version{version} {
    // nop
  }

  /// The partition id.
  vast::uuid uuid = vast::uuid::nil();

  /// Total number of events in the partition. The sum of all
  /// values in `stats`.
  size_t events = 0ull;

  /// The newest import timestamp of the table slices in this partition.
  time max_import_time = {};

  /// The schema of the partition.
  type schema = {};

  /// The internal version of the partition.
  uint64_t version = {};

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
    return f.object(x)
      .pretty_name("vast.partition-info")
      .fields(f.field("uuid", x.uuid), f.field("events", x.events),
              f.field("max-import-time", x.max_import_time),
              f.field("schema", x.schema), f.field("version", x.version));
  }
};

/// A partition synopsis and a uuid.
struct partition_synopsis_pair {
  vast::uuid uuid;
  partition_synopsis_ptr synopsis;

  template <class Inspector>
  friend auto inspect(Inspector& f, partition_synopsis_pair& x) {
    return f.object(x)
      .pretty_name("vast.partition-synopsis-pair")
      .fields(f.field("uuid", x.uuid), f.field("synopsis", x.synopsis));
  }
};

} // namespace vast

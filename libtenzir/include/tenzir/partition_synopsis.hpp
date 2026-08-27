//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/flat_map.hpp"
#include "tenzir/detail/friend_attribute.hpp"
#include "tenzir/fbs/partition_synopsis.hpp"
#include "tenzir/index_config.hpp"
#include "tenzir/qualified_record_field.hpp"
#include "tenzir/resource.hpp"
#include "tenzir/synopsis.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/uuid.hpp"

namespace caf {

// Forward declaration to be able to befriend the unshare implementation.
template <typename T>
T* default_intrusive_cow_ptr_unshare(T*&);

} // namespace caf

namespace tenzir {

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

  /// Drops the Bloom-filter sketches (string and IP fields), matching what
  /// `unpack` does with `lazy_sketches`: field-level sketches are kept as null
  /// entries and type-level Bloom-filter sketches are removed. Min/max, time,
  /// and bool synopses are retained. Used to keep newly merged synopses out of
  /// resident memory when lazy sketches are enabled.
  void defer_bloom_filters();

  /// Estimate the memory footprint of this partition synopsis.
  /// @returns A best-effort estimate of the amount of memory used by this
  ///          synopsis.
  size_t memusage() const;

  // Information about the raw data storage.
  resource store_file = {};

  // Information about the dense indexes.
  resource indexes_file = {};

  // Information about the sparse indexes.
  resource sketches_file = {};

  // Number of events in the partition.
  uint64_t events = 0;

  /// The minimum import timestamp of all contained table slices.
  time min_import_time = time::max();

  /// The maximum import timestamp of all contained table slices.
  time max_import_time = time::min();

  /// The version number of this partition.
  uint64_t version = version::current_partition_version;

  /// Best-effort estimate of the decoded in-memory size of the partition data.
  uint64_t approx_bytes = 0;

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
         partition_synopsis&, bool lazy_sketches);

  // Returns a raw pointer to a deep copy of this partition synopsis.
  // For use by the `caf::intrusive_cow_ptr`.
  partition_synopsis* copy() const;

private:
  // Cached memory usage.
  mutable std::atomic<size_t> memusage_ = 0ull;
};

/// Unpacks a partition synopsis from its FlatBuffers representation.
/// @param x The serialized partition synopsis.
/// @param ps The synopsis to populate.
/// @param lazy_sketches When set, Bloom-filter sketches (string and IP fields)
/// are not deserialized. Such fields are still registered with a null synopsis
/// so that the catalog conservatively treats predicates on them as candidates
/// (never a false negative), trading sketch-based pruning for much lower
/// resident memory and faster startup. Min/max, time, and bool synopses are
/// always loaded.
[[nodiscard]] caf::error
unpack(const fbs::partition_synopsis::LegacyPartitionSynopsis& x,
       partition_synopsis& ps, bool lazy_sketches = false);

/// Some quantitative information about a partition.
struct partition_info {
  static constexpr bool use_deep_to_string_formatter = true;

  partition_info() noexcept = default;

  partition_info(class uuid uuid, size_t events, time max_import_time,
                 type schema, uint64_t version, uint64_t approx_bytes = 0,
                 uint64_t store_bytes = 0) noexcept
    : uuid{uuid},
      events{events},
      max_import_time{max_import_time},
      schema{std::move(schema)},
      version{version},
      approx_bytes{approx_bytes},
      store_bytes{store_bytes} {
    // nop
  }

  partition_info(class uuid uuid, const partition_synopsis& synopsis)
    : partition_info{uuid,
                     synopsis.events,
                     synopsis.max_import_time,
                     synopsis.schema,
                     synopsis.version,
                     synopsis.approx_bytes,
                     synopsis.store_file.size} {
    // nop
  }

  /// The partition id.
  tenzir::uuid uuid = tenzir::uuid::null();

  /// Total number of events in the partition. The sum of all
  /// values in `stats`.
  size_t events = 0ull;

  /// The newest import timestamp of the table slices in this partition.
  time max_import_time = {};

  /// The schema of the partition.
  type schema = {};

  /// The internal version of the partition.
  uint64_t version = {};

  /// Best-effort estimate of the decoded in-memory size of the partition data.
  uint64_t approx_bytes = 0;

  /// Size of the persisted store file in bytes.
  uint64_t store_bytes = 0;

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
      .pretty_name("tenzir.partition-info")
      .fields(f.field("uuid", x.uuid), f.field("events", x.events),
              f.field("max-import-time", x.max_import_time),
              f.field("schema", x.schema), f.field("version", x.version),
              f.field("approx-bytes", x.approx_bytes),
              f.field("store-bytes", x.store_bytes));
  }
};

/// A partition synopsis and a uuid.
struct partition_synopsis_pair {
  tenzir::uuid uuid;
  partition_synopsis_ptr synopsis;

  template <class Inspector>
  friend auto inspect(Inspector& f, partition_synopsis_pair& x) {
    return f.object(x)
      .pretty_name("tenzir.partition-synopsis-pair")
      .fields(f.field("uuid", x.uuid), f.field("synopsis", x.synopsis));
  }
};

/// The synopses of all partitions of one schema, sorted by partition id.
/// Immutable once published: the catalog clones a schema's map to mutate it,
/// so shared references -- the snapshots held by lookup workers -- stay valid
/// for as long as their holders need them.
using schema_synopsis_map = detail::flat_map<uuid, partition_synopsis_ptr>;

/// One generation of the catalog's synopsis set. Taking a snapshot is a
/// single shared_ptr copy; the catalog publishes a new generation on every
/// mutation, and a generation lives exactly as long as some reader holds it.
struct catalog_snapshot {
  using map_type
    = std::unordered_map<type, std::shared_ptr<const schema_synopsis_map>>;

  std::shared_ptr<const map_type> synopses = {};

  /// Never serialized in practice -- lookup workers live in the catalog's
  /// process -- but the type system wants a faithful round-trip anyway, so
  /// this flattens to (uuid, synopsis) pairs and rebuilds.
  template <class Inspector>
  friend auto inspect(Inspector& f, catalog_snapshot& x) {
    if constexpr (Inspector::is_loading) {
      auto flat = std::vector<partition_synopsis_pair>{};
      if (not f.apply(flat)) {
        return false;
      }
      auto grouped
        = std::unordered_map<type, std::shared_ptr<schema_synopsis_map>>{};
      for (auto& pair : flat) {
        auto& schema_map = grouped[pair.synopsis->schema];
        if (not schema_map) {
          schema_map = std::make_shared<schema_synopsis_map>();
        }
        (*schema_map)[pair.uuid] = std::move(pair.synopsis);
      }
      auto result = std::make_shared<catalog_snapshot::map_type>();
      for (auto& [schema, schema_map] : grouped) {
        (*result)[schema] = std::move(schema_map);
      }
      x.synopses = std::move(result);
      return true;
    } else {
      auto flat = std::vector<partition_synopsis_pair>{};
      if (x.synopses) {
        for (const auto& [schema, schema_map] : *x.synopses) {
          for (const auto& [id, synopsis] : *schema_map) {
            flat.push_back({id, synopsis});
          }
        }
      }
      return f.apply(flat);
    }
  }
};

struct partition_transformer_result {
  std::vector<partition_info> input_partitions;
  std::vector<partition_synopsis_pair> output_partitions;
  bool input_complete = true;
  bool skipped = false;

  template <class Inspector>
  friend auto inspect(Inspector& f, partition_transformer_result& x) {
    return f.object(x)
      .pretty_name("tenzir.partition-transformer-result")
      .fields(f.field("input-partitions", x.input_partitions),
              f.field("output-partitions", x.output_partitions),
              f.field("input-complete", x.input_complete),
              f.field("skipped", x.skipped));
  }
};

struct partition_apply_result {
  std::vector<partition_info> input_partitions;
  std::vector<partition_info> output_partitions;
  bool input_complete = true;
  bool skipped = false;

  /// The transform marker, held for a policy-driven transform until the
  /// caller reports the policy flush done. Until the commit is durable the
  /// marker is its only record; deleting it first would let a crash forget a
  /// completed non-idempotent rule. Empty for transforms without a token.
  std::string marker = {};

  template <class Inspector>
  friend auto inspect(Inspector& f, partition_apply_result& x) {
    return f.object(x)
      .pretty_name("tenzir.partition-apply-result")
      .fields(f.field("input-partitions", x.input_partitions),
              f.field("output-partitions", x.output_partitions),
              f.field("input-complete", x.input_complete),
              f.field("marker", x.marker),
              f.field("skipped", x.skipped));
  }
};

} // namespace tenzir

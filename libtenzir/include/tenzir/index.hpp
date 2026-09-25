//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/active_partition.hpp"
#include "tenzir/actors.hpp"
#include "tenzir/catalog.hpp"
#include "tenzir/importer.hpp"
#include "tenzir/partition_paths.hpp"
#include "tenzir/plugin_fwd.hpp"
#include "tenzir/query_context.hpp"
#include "tenzir/uuid.hpp"

#include <caf/actor.hpp>
#include <caf/behavior.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/typed_response_promise.hpp>

#include <queue>
#include <unordered_map>
#include <vector>

namespace tenzir {

/// The state of the active partition.
struct active_partition_info {
  /// The partition actor.
  active_partition_actor actor = {};

  /// The number of events in the partition.
  size_t events = 0;

  /// The UUID of the partition.
  uuid id = {};

  template <class Inspector>
  friend auto inspect(Inspector& f, active_partition_info& x) {
    return f.object(x)
      .pretty_name("active_partition_info")
      .fields(f.field("actor", x.actor), f.field("events", x.events),
              f.field("id", x.id));
  }
};

/// The state of the index actor.
struct index_state {
  // -- constructor ------------------------------------------------------------

  explicit index_state(index_actor::pointer self);

  // -- inbound path -----------------------------------------------------------

  void handle_slice(table_slice slice);

  // -- partition handling -----------------------------------------------------

  /// Creates a new active partition.
  /// @param schema The schema of the new partition. All events routed to the
  /// partition are assumed to have the exact same schema.
  /// @returns An iterator to the new active partition.
  [[nodiscard]] caf::expected<
    std::unordered_map<type, active_partition_info>::iterator>
  create_active_partition(const type& schema);

  /// Decommissions the active partition.
  /// @param schema The schema of the active partition to decommission.
  /// @param completion The completion handler; called in the actor context of
  /// the index when the partition was decommissioned.
  /// @note This invalidates iterators to the *active_partitions* map.
  void decommission_active_partition(
    type schema, std::function<void(const caf::error&)> completion);

  auto flush() -> caf::typed_response_promise<void>;

  void pin_recent_partition(const uuid& id);

  void unpin_recent_partition(const uuid& id);

  void retire_partition(const uuid& id, caf::error reason);

  void drain_retired_partitions(caf::error reason);

  // -- introspection ----------------------------------------------------------

  size_t memusage() const;

  // -- data members -----------------------------------------------------------

  /// Pointer to the parent actor.
  index_actor::pointer self;

  /// One active (read/write) partition per schema.
  std::unordered_map<type, active_partition_info> active_partitions = {};

  /// Partitions that are currently in the process of persisting.
  // TODO: An alternative to keeping an explicit set of unpersisted partitions
  // would be to add functionality to the LRU cache to "pin" certain items.
  // Then (assuming the query interface for both types of partition stays
  // identical) we could just use the same cache for unpersisted partitions and
  // unpin them after they're safely on disk.
  struct unpersisted_partition_info {
    type schema = {};
    active_partition_actor actor = {};
    // The index keeps one reference until the partition is safely retired.
    // Each in-flight recent snapshot increments the count while it still
    // needs to query the partition actor.
    size_t ref_count = 1;
    bool visible_for_recent = true;
    bool exit_sent = false;
    caf::error exit_reason = caf::none;
  };

  std::unordered_map<uuid, unpersisted_partition_info> unpersisted = {};

  /// The maximum number of events that a partition can hold.
  size_t partition_capacity = {};

  /// The total number of events in active partitions.
  size_t buffered_events = 0;

  /// The maximum total number of events in active partitions.
  size_t max_buffered_events = {};

  /// Timeout after which an active partition is forcibly flushed.
  duration active_partition_timeout = {};

  /// The CATALOG actor.
  catalog_actor catalog = {};

  /// The on-disk locations of the partition files.
  partition_paths paths = {};

  bool shutting_down = false;

  /// Plugin responsible for spawning new partition-local stores.
  const tenzir::store_actor_plugin* store_actor_plugin = {};

  /// Actor handle of the filesystem actor.
  filesystem_actor filesystem = {};

  /// Config options to be used for new synopses; passed to active partitions.
  index_config synopsis_opts;

  /// Config options for the index.
  caf::settings index_opts;

  /// The taxonomies for querying.
  std::shared_ptr<tenzir::taxonomies> taxonomies = {};

  constexpr static inline auto name = "index";
};

/// Indexes events in horizontal partitions.
/// @param filesystem The filesystem actor. Not used by the index itself but
/// forwarded to partitions.
/// @param catalog The catalog actor.
/// @param dir The directory of the index.
/// @param store_backend The store backend to use for new partitions.
/// @param partition_capacity The maximum number of events per partition.
/// @param active_partition_timeout Timeout after which an active partition is
/// forcibly flushed.
/// @param catalog_dir The directory used by the catalog.
/// @param index_config The meta-index configuration of the false-positives
/// rates for the types and fields.
/// @pre `partition_capacity > 0
//  TODO: Use a settings struct for the various parameters.
index_actor::behavior_type
index(index_actor::stateful_pointer<index_state> self,
      filesystem_actor filesystem, catalog_actor catalog,
      const std::filesystem::path& dir, std::string store_backend,
      size_t max_buffered_events, size_t partition_capacity,
      duration active_partition_timeout,
      const std::filesystem::path& catalog_dir, index_config index_config);

} // namespace tenzir

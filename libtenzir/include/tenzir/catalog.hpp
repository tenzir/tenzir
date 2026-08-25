//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/actors.hpp"
#include "tenzir/detail/flat_map.hpp"
#include "tenzir/detail/inspection_common.hpp"
#include "tenzir/detail/stable_set.hpp"
#include "tenzir/expression.hpp"
#include "tenzir/index_config.hpp"
#include "tenzir/instrumentation.hpp"
#include "tenzir/option.hpp"
#include "tenzir/partition_paths.hpp"
#include "tenzir/partition_synopsis.hpp"
#include "tenzir/plugin_fwd.hpp"
#include "tenzir/taxonomies.hpp"
#include "tenzir/uuid.hpp"

#include <caf/mail_cache.hpp>
#include <caf/response_type.hpp>
#include <caf/settings.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <list>
#include <unordered_set>
#include <vector>

namespace tenzir {

/// The transformer replaces the old partition with the new one or keeps it
/// depending on the value of keep_original_partition.
enum class keep_original_partition : bool {
  yes = true,
  no = false,
};

template <class Inspector>
auto inspect(Inspector& f, keep_original_partition& x) {
  return detail::inspect_enum(f, x);
}

// New partition creation listeners will be sent the initial state of the
// whole database if they set this to 'yes'.
enum class send_initial_dbstate : bool {
  yes = true,
  no = false,
};

template <class Inspector>
auto inspect(Inspector& f, send_initial_dbstate& x) {
  return detail::inspect_enum(f, x);
}

/// The result of a catalog query.
struct catalog_lookup_result {
  struct candidate_info {
    expression exp;
    std::vector<partition_info> partition_infos;

    template <class Inspector>
    friend auto inspect(Inspector& f, candidate_info& x) -> bool {
      return f.object(x)
        .pretty_name("tenzir.system.catalog_result.candidate_info")
        .fields(f.field("expression", x.exp),
                f.field("partition-infos", x.partition_infos));
    }
  };

  std::unordered_map<type, candidate_info> candidate_infos;

  auto empty() const noexcept -> bool;

  auto size() const noexcept -> size_t;

  template <class Inspector>
  friend auto inspect(Inspector& f, catalog_lookup_result& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.catalog_lookup_result")
      .fields(f.field("candidate-infos", x.candidate_infos));
  }
};

/// A memory-bounded LRU cache of fully-loaded partition synopses. It serves
/// Bloom-filter pruning for partitions whose sketches were deferred at startup
/// (see `tenzir.index.lazy-sketches`): the authoritative synopses keep their
/// deferred (null) sketches, and the catalog loads them on demand into this
/// cache when a query needs them. Entries are owned solely by the cache and
/// never alias the resident synopses, so eviction is O(1) and cannot disturb
/// resident state.
///
/// This is intentionally separate from `detail::lru_cache`, which bounds the
/// *number* of items. Here the bound must be on *memory* (`sketch-cache-bytes`)
/// because synopsis sizes vary by orders of magnitude, so a count bound would
/// not cap resident memory — the very problem lazy sketches address. The loader
/// is also fallible (a failed/remote load leaves the partition deferred) and
/// reports the bytes actually cached, which the generic factory-on-miss
/// interface does not express.
class sketch_cache {
public:
  sketch_cache() = default;
  explicit sketch_cache(size_t budget_bytes) : budget_{budget_bytes} {
  }

  /// Returns the cached synopsis for `id` without changing its recency, or
  /// nullptr on a miss. Used on the read (lookup) path, which must stay const.
  [[nodiscard]] auto peek(const uuid& id) const -> partition_synopsis_ptr;

  /// Inserts a loaded synopsis and marks it most-recently-used, evicting
  /// least-recently-used entries until the total is within budget. Returns the
  /// number of bytes actually cached, which is zero if the entry alone exceeds
  /// the budget (it is then not cached at all).
  auto put(const uuid& id, partition_synopsis_ptr synopsis) -> size_t;

  /// Removes `id` from the cache if present (used on erase/merge/replace).
  void erase(const uuid& id);

  [[nodiscard]] auto budget() const -> size_t {
    return budget_;
  }
  [[nodiscard]] auto used() const -> size_t {
    return used_;
  }

private:
  struct entry {
    partition_synopsis_ptr synopsis = {};
    size_t bytes = 0;
    std::list<uuid>::iterator pos = {};
  };
  size_t budget_ = 0;
  size_t used_ = 0;
  std::list<uuid> lru_ = {}; // front = most-recently-used
  std::unordered_map<uuid, entry> entries_ = {};
};

/// One outstanding candidate set, keyed by `query_context::id`.
///
/// The lease pins its partitions: their files stay on disk until the lease
/// goes away, even if the partition is replaced or erased in the meantime, so
/// a reader that already received the candidate set can still open it.
struct partition_lease {
  caf::actor_addr owner = {};
  std::vector<uuid> partitions = {};
};

/// A partition that left the catalog while a retriever still held a pin on it.
/// Its files stay on disk until the last pin goes away; the tombstone marker
/// makes sure that a crash in between does not resurrect it, because a
/// partition file without a synopsis has its synopsis regenerated at startup.
struct deferred_erase {
  /// Carries the resource paths of the partition's files.
  partition_synopsis_ptr synopsis = {};

  /// The tombstone marker recording that this partition is to be erased.
  std::filesystem::path marker = {};

  /// Set if the partition is being quarantined rather than erased; holds the
  /// rendered error that triggered the quarantine.
  Option<std::string> quarantine_error = None{};

  /// When the files go regardless of remaining pins. A retriever that wedges
  /// would otherwise keep an erased partition on disk forever and the disk
  /// budget could never be met. Unset when forcing is disabled.
  Option<time> deadline = None{};
};

/// Builds a partition transform marker. `in` names the transform's inputs and
/// `out` its outputs. With `keep == yes` the inputs are omitted: they survive
/// the transform, so a replay must not erase them. A marker with inputs but no
/// outputs is a tombstone: replaying it erases the inputs and nothing else.
auto create_marker(const std::vector<uuid>& in, const std::vector<uuid>& out,
                   keep_original_partition keep) -> chunk_ptr;

/// The state of the CATALOG actor.
struct catalog_state {
public:
  catalog_state() = default;

  constexpr static auto name = "catalog";

  /// Rebuilds the catalog from the database directory. Runs synchronously
  /// during startup, before the catalog installs its behavior, so requests
  /// that arrive in the meantime simply wait in the mailbox.
  auto load_from_disk() -> caf::error;

  /// Finishes up transforms that were interrupted by the last shutdown:
  /// erases the inputs of a committed transform and moves its outputs into
  /// place. Part of `load_from_disk`.
  void replay_markers();

  /// Creates the catalog from a set of partition synopses.
  auto initialize(std::vector<partition_synopsis_pair> partitions)
    -> caf::error;

  /// Add a new partition synopsis.
  auto merge(std::vector<partition_synopsis_pair> partitions)
    -> caf::result<atom::ok>;

  /// Erase this partition from the catalog. Leaves the on-disk files alone.
  void erase(const uuid& partition);

  /// Records an outstanding candidate set and starts monitoring its owner, so
  /// that the lease is released even if the owner never gets around to it.
  void add_lease(const uuid& query, const caf::strong_actor_ptr& owner,
                 std::vector<uuid> partitions);

  /// Drops part of a lease. Readers release each partition as they finish with
  /// it, so a pin covers one partition read rather than a whole export.
  void release_lease(const uuid& query, const std::vector<uuid>& partitions);

  /// Drops a whole lease.
  void release_lease(const uuid& query);

  /// Drops every lease held by the given owner.
  void release_leases_of(const caf::actor_addr& owner);

  /// Decrements the pin count of a partition and, if that was the last pin on
  /// a partition that already left the catalog, deletes its files.
  void unpin(const uuid& partition);

  /// Removes the partition from the catalog and disposes of its files, either
  /// right away or, if a retriever still holds a pin, once the last pin goes
  /// away. Writes a tombstone marker in the latter case.
  auto retire(const uuid& partition, Option<std::string> quarantine_error)
    -> caf::result<atom::done>;

  /// Disposes of a partition that already left the catalog, deferring the
  /// deletion while it is pinned. `marker` names the tombstone that already
  /// records the erasure.
  void retire_erased(const uuid& partition, partition_synopsis_ptr synopsis,
                     std::filesystem::path marker);

  /// Deletes (or, when quarantining, moves aside) the files of a partition
  /// that has left the catalog and is no longer pinned. Reports through `rp`
  /// if one is given.
  void dispose_of(const uuid& partition, deferred_erase entry,
                  Option<caf::typed_response_promise<atom::done>> rp);

  /// Erases a tombstone marker once no deferred erasure needs it any more.
  void erase_marker_if_unreferenced(const std::filesystem::path& marker);

  /// Deletes deferred partitions whose deadline has passed, pins and all.
  void sweep_deferred();

  /// The point at which a deferral parked now would be forced, if at all.
  auto deferred_erase_deadline() const -> Option<time>;

  /// Applies a pipeline to a set of partitions, writing the results into new
  /// partitions. With `keep == keep_original_partition::no` the inputs are
  /// replaced by the outputs and erased from disk once the outputs are safely
  /// in place.
  auto apply(ast::pipeline pipe, std::vector<partition_info> selected,
             keep_original_partition keep, std::string origin)
    -> caf::result<partition_apply_result>;

  /// Adds a new partition creation listener.
  void
  add_partition_creation_listener(partition_creation_listener_actor listener);

  /// Erases this partition from the catalog and deletes its on-disk files.
  /// The store is located by probing the archive for the known extensions; if
  /// that fails, the partition itself is loaded so its store header can name
  /// the file. Refused while the partition is an input to a running transform.
  auto erase_from_disk(const uuid& partition) -> caf::result<atom::done>;

  /// Returns the resident synopsis of a partition, or nullptr if the catalog
  /// does not know it.
  auto find_synopsis(const uuid& partition) const -> partition_synopsis_ptr;

  /// Quarantines this partition: moves its store file aside into a
  /// "quarantined" directory, deletes its other on-disk files, and erases it
  /// from the catalog.
  /// @param partition The partition to quarantine.
  /// @param error The rendered error that triggered the quarantine, kept for
  /// logging.
  auto erase_and_extract(const uuid& partition, std::string error)
    -> caf::result<atom::done>;

  /// Retrieves the list of candidate partition IDs for a given expression.
  /// @param expr The expression to lookup.
  /// @returns A lookup result of candidate partitions categorized by type.
  auto lookup(expression expr) -> caf::expected<catalog_lookup_result>;

  /// Applies the finishing touches shared by every `lookup` path: sorts each
  /// schema's candidates by recency and reports the timing. `start` is when
  /// the enclosing lookup began.
  auto finalize_lookup(catalog_lookup_result&& candidates,
                       stopwatch::time_point start) const
    -> catalog_lookup_result;

  /// Evaluates `expr` against the given partition collection of `schema`.
  /// The collection is a parameter (rather than always the full per-schema map)
  /// so the same logic can evaluate a single partition during on-demand sketch
  /// pruning. This is only sound because the evaluation has no cross-partition
  /// state: evaluating a set equals the union of evaluating each partition
  /// alone. Keep it that way.
  /// @param deferred_sketch_partitions Receives the ids of partitions kept only
  /// because a Bloom-filter sketch was deferred and could prune them if loaded.
  /// `lookup` uses this to restrict on-demand loading to exactly those
  /// candidates instead of every surviving candidate in every schema.
  auto lookup_impl(
    const expression& expr, const type& schema,
    const detail::flat_map<uuid, partition_synopsis_ptr>& partition_synopses,
    std::unordered_set<uuid>& deferred_sketch_partitions) const
    -> catalog_lookup_result::candidate_info;

  /// Loads the deferred Bloom-filter sketches of the given partition into the
  /// sketch cache, if possible. Returns the number of bytes loaded, or 0 if
  /// the partition was already cached, has no loadable sketches, or could not
  /// be loaded (in which case the partition stays a conservative candidate).
  auto ensure_sketches_loaded(const uuid& id, const type& schema) -> size_t;

  /// @returns A best-effort estimate of the amount of memory used for this
  /// catalog (in bytes).
  auto memusage() const -> size_t;

  // -- data members -----------------------------------------------------------

  /// A pointer to the parent actor.
  catalog_actor::pointer self = {};

  /// Used to move/erase on-disk partition files.
  filesystem_actor filesystem = {};

  /// The on-disk locations of the partition files.
  partition_paths paths = {};

  /// Plugin responsible for spawning stores for transform outputs.
  const tenzir::store_actor_plugin* store_actor_plugin = {};

  /// Config options to be used for new synopses.
  index_config synopsis_opts = {};

  /// Config options for value indices.
  caf::settings index_opts = {};

  /// The partitions that are inputs to a running transform. They must not be
  /// erased underneath it: the transform would write its outputs anyway and
  /// the erased data would come back.
  detail::stable_set<uuid> in_transformation = {};

  /// The outstanding candidate sets, keyed by `query_context::id`.
  std::unordered_map<uuid, partition_lease> leases = {};

  /// The owners of the outstanding candidate sets, with the number of leases
  /// each holds and the monitor that reports their termination.
  std::unordered_map<caf::actor_addr, std::pair<size_t, caf::disposable>>
    consumers = {};

  /// How many leases reference each partition.
  std::unordered_map<uuid, size_t> pin_counts = {};

  /// Partitions that left the catalog while still pinned.
  std::unordered_map<uuid, deferred_erase> deferred = {};

  /// How long an erased partition may linger because a reader still pins it.
  /// Zero disables forcing.
  duration deferred_erase_timeout = {};

  /// The collection of currently active transformers. These need to be
  /// explicitly shut down when the catalog exits. The `disposable` refers to
  /// the monitor that would otherwise automatically remove the actor from the
  /// list if it finished on its own. It must be disposed of before shutdown.
  std::unordered_map<caf::actor_addr, caf::disposable> active_transformers = {};

  /// List of actors that want to be notified about new partitions.
  std::vector<partition_creation_listener_actor> partition_creation_listeners
    = {};

  /// For each type, maps a partition ID to the synopses for that partition.
  // We mainly iterate over the whole map and return a sorted set, for which
  // the `flat_map` proves to be much faster than `std::{unordered_,}set`.
  // See also ae9dbed.
  std::unordered_map<tenzir::type,
                     detail::flat_map<uuid, partition_synopsis_ptr>>
    synopses_per_type;

  /// On-demand cache of partitions whose deferred Bloom-filter sketches have
  /// been loaded for pruning. Bounded by `tenzir.index.sketch-cache-bytes`.
  sketch_cache sketches;

  /// Whether Bloom-filter sketches are deferred (see `tenzir.index.lazy-
  /// sketches`). When set, newly merged synopses also have their Bloom filters
  /// dropped so that ongoing ingest does not accumulate them in resident
  /// memory; they are loaded on demand from disk instead.
  bool lazy_sketches = false;

  tenzir::taxonomies taxonomies = {};
};

/// The CATALOG is the first index actor that queries hit. The result
/// represents a list of candidate partition IDs that may contain the desired
/// data. The CATALOG may return false positives but never false negatives.
/// @param self The actor handle.
/// @param filesystem Used to move/erase on-disk partition files.
/// @param paths The on-disk locations of the partition files.
/// @param deferred_erase_timeout How long an erased partition may linger
/// because a retriever still pins it; zero never forces the deletion.
/// @param store_backend The store backend to use for transform outputs.
/// @param synopsis_opts The false-positive rates for the types and fields of
/// newly created synopses.
/// @param partition_capacity The maximum number of events per partition.
/// @param sketch_cache_bytes Memory budget for on-demand loading of deferred
/// Bloom-filter sketches; zero disables on-demand loading.
/// @param lazy_sketches Whether Bloom-filter sketches are deferred; when set,
/// merged synopses also have their Bloom filters dropped to keep resident
/// memory bounded during ongoing ingest.
auto catalog(catalog_actor::stateful_pointer<catalog_state> self,
             filesystem_actor filesystem, partition_paths paths,
             std::string store_backend, index_config synopsis_opts,
             size_t partition_capacity, duration deferred_erase_timeout,
             size_t sketch_cache_bytes = 0, bool lazy_sketches = false)
  -> catalog_actor::behavior_type;

} // namespace tenzir

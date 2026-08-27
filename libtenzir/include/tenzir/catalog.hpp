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
#include "tenzir/dbdir_size.hpp"
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
#include "tenzir/series_builder.hpp"
#include "tenzir/taxonomies.hpp"
#include "tenzir/uuid.hpp"

#include <caf/mail_cache.hpp>
#include <caf/response_type.hpp>
#include <caf/settings.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <limits>
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

/// What a rebuild run should select and how hard it should work at it.
struct rebuild_options {
  /// Rebuild every candidate, not just the outdated and undersized ones.
  bool all = false;

  /// Also rebuild partitions that are current but smaller than
  /// `undersized_threshold` of the maximum partition size.
  bool undersized = false;

  /// How many batches the run transforms at once, and the divisor of its
  /// per-batch memory budget.
  size_t parallel = 1;

  /// Stop after selecting this many partitions.
  size_t max_partitions = std::numeric_limits<size_t>::max();

  /// Restricts the run to the partitions matching this expression.
  expression expression = {};

  /// Return as soon as the run has started rather than when it finishes.
  bool detached = false;

  /// Whether this is the periodic source rather than a user-issued run. The
  /// automatic source keeps picking up newly ingested partitions; a manual run
  /// bounds itself to the partitions that existed when it started, so it
  /// terminates under ongoing ingest.
  bool automatic = false;

  friend auto inspect(auto& f, rebuild_options& x) {
    return f.object(x)
      .pretty_name("tenzir.rebuild_options")
      .fields(f.field("all", x.all), f.field("undersized", x.undersized),
              f.field("parallel", x.parallel),
              f.field("max-partitions", x.max_partitions),
              f.field("expression", x.expression),
              f.field("detached", x.detached),
              f.field("automatic", x.automatic));
  }
};

/// How a rebuild run should be stopped.
struct rebuild_stop_options {
  /// Return immediately instead of waiting for the run to wind down.
  bool detached = false;

  friend auto inspect(auto& f, rebuild_stop_options& x) {
    return f.object(x)
      .pretty_name("tenzir.rebuild_stop_options")
      .fields(f.field("detached", x.detached));
  }
};

/// How the catalog runs storage maintenance itself.
///
/// Until the cutover this selects between two mechanisms and never enables
/// both: with `enabled` the node runs no standalone rebuilder, compactor, or
/// disk monitor and the catalog drives that work; without it the catalog's
/// sources stay stopped and the standalone actors behave exactly as they do
/// today.
struct maintenance_options {
  bool enabled = false;

  /// The automatic rebuild source's parallelism. Zero disables that source
  /// alone; everything else keeps running.
  size_t automatic_rebuild = 1;

  /// How often the automatic source re-selects, first pass at half interval.
  duration rebuild_interval = {};

  /// The disk budget loop: water marks, step size, scan interval, and the
  /// optional external size command. A zero high water mark disables it, which
  /// is what an unconfigured budget looks like.
  disk_monitor_config space = {};

  /// How many policy pipelines may run at once. Separate from rebuild
  /// parallelism, so that a slow user-authored pipeline cannot stall a
  /// rebuild, nor a large rebuild batch delay a retention rule.
  size_t compaction_slots = 1;

};

/// The threshold at which a partition counts as undersized, relative to the
/// configured `tenzir.max-partition-size`.
inline constexpr auto undersized_threshold = 0.8;

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

/// A rebuild run in progress inside the catalog.
struct rebuild_run {
  rebuild_options options = {};

  /// Distinguishes this run from the one it superseded. A manual run can
  /// displace an automatic one whose batches are still in flight; their
  /// continuations then find a run engaged and would otherwise settle their
  /// accounts against it.
  uint64_t generation = 0;

  /// A manual run only considers partitions that already existed when it
  /// started, so it terminates under ongoing ingest. Unset for the automatic
  /// source, whose whole point is to keep picking up new partitions.
  Option<time> horizon = None{};

  /// Runtime byte estimates per schema, for legacy partitions that carry no
  /// `approx_bytes`. The catalog sees every transform result, so it learns
  /// these as it goes.
  std::unordered_map<type, uint64_t> approx_bytes_per_event = {};

  /// Every partition this run has already selected, plus every partition it
  /// produced. Rebuilding a partition yields a fresh id, so without this a
  /// run would keep finding its own output eligible and never terminate --
  /// `--all` most obviously, since its output matches the same filter.
  std::unordered_set<uuid> visited = {};

  /// How many partitions the run has handed to a transform, against
  /// `options.max_partitions`.
  size_t selected = 0;

  /// How many batches are in flight, against `options.parallel`.
  size_t running = 0;

  /// Statistics, as `rebuild show` reports them.
  size_t transformed = 0;
  size_t results = 0;
  size_t quarantined = 0;

  /// Set once a stop was requested; the run winds down instead of selecting
  /// more work.
  bool stopping = false;

  /// Answered when the run finishes.
  std::vector<caf::typed_response_promise<void>> stop_requests = {};
};

/// The state of the CATALOG actor.
struct catalog_state {
public:
  catalog_state();

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

  // -- rebuild ----------------------------------------------------------------

  /// Selects the next batch of rebuild work, empty when no schema has enough
  /// eligible partitions left. Evaluated against live state on every call, so
  /// a run picks up partitions ingested after it started and skips ones erased
  /// or claimed by another transform underneath it.
  auto select_rebuild_batch(rebuild_run& run) -> std::vector<partition_info>;

  /// Starts a rebuild run, or joins the one already in progress.
  auto start_rebuild(rebuild_options options) -> caf::result<void>;

  /// Winds the current run down: it selects no further work and finishes once
  /// its in-flight batches land.
  auto stop_rebuild(const rebuild_stop_options& options) -> caf::result<void>;

  /// Fills the run's free batch slots with freshly selected work. Called when
  /// a run starts and whenever a batch lands.
  void schedule_rebuild();

  /// Ends the run and answers everyone waiting on it.
  void finish_rebuild();

  /// Continues the run after a batch landed, or ends it if it was winding down.
  void schedule_rebuild_or_finish();

  /// Defined out of line, like the constructor above: `policy` is incomplete
  /// in this header, so the deleter it needs cannot be generated here.
  ~catalog_state();

  /// Builds the storage policy from whichever plugin contributes one.
  void make_policy();

  /// Measures the database directory off this thread, continuing in
  /// `on_space_measured`.
  void measure_space();

  /// Acts on a completed measurement, evicting while over budget.
  void on_space_measured(uint64_t size);

  /// The bytes held by partitions that are erased but still pinned. They are
  /// already spoken for and will come back without erasing anything further,
  /// so the budget loop must not count them against the water marks.
  auto parked_bytes() const -> uint64_t;

  /// How urgently a partition should be evicted; higher goes sooner. Falls
  /// back to its age, which is the scale a policy weight is expressed in.
  auto eviction_weight_of(const uuid& partition,
                          const partition_synopsis& synopsis) const -> double;

  /// The next partitions to evict, heaviest first, at most `limit`.
  auto select_eviction_batch(size_t limit) const -> std::vector<uuid>;

  /// Gives a compaction slot back and lets anything queued take it. Every
  /// release goes through here so that no path can free a slot without
  /// waking a run that is waiting for one.
  void release_compaction_slot();

  /// Asks the policy about each partition in turn and starts what it asks
  /// for, up to the free slots in the compaction pool.
  void maintenance_pass();

  /// Runs one policy action, holding a slot until it lands.
  void run_maintenance_action(const uuid& partition, storage_action action);

  /// Runs the policy's eviction action for a partition, if it has one.
  /// @returns Whether an action was started; `false` means erase it instead.
  auto run_eviction_action(const uuid& partition) -> bool;

  /// Reports the disk budget loop's state.
  auto space_status() const -> record;

  /// Describes one run's statistics and options, shared between the live
  /// `current-run` and the historical `last-run` status entries.
  static auto describe_rebuild_run(const rebuild_run& run) -> record;

  /// Reports the current and last run, plus every partition quarantined so
  /// far. The quarantine set outlives the run that found it.
  auto rebuild_status() const -> record;

  /// Emits a `tenzir.metrics.rebuild_quarantine` event. Only on demand, when a
  /// partition is actually quarantined, so unlike the periodic rebuild metric
  /// it does not sit on a timer.
  void report_quarantine(const uuid& partition, const caf::error& error);

  /// Whether a partition is worth handing to a rebuild at all.
  auto is_rebuild_candidate(const uuid& partition,
                            const partition_synopsis& synopsis,
                            const rebuild_run& run) const -> bool;

  /// Sets up a rebuild run and starts its first batches. Fails if a run is
  /// already going that this one must not displace.
  auto begin_rebuild(rebuild_options options) -> caf::error;

  /// Folds a partition'"'"'s size into the per-schema byte-per-event estimate
  /// that covers legacy partitions without `approx_bytes`.
  static void
  learn_size_estimate(rebuild_run& run, const partition_info& partition);

  /// The estimated decoded size of a partition, falling back to the learned
  /// per-schema estimate and then to `unknown`.
  static auto
  estimate_approx_bytes(const rebuild_run& run, const partition_info& partition,
                        uint64_t unknown) -> uint64_t;

  /// Whether we can say how large a partition decodes to. Partitions written
  /// before `approx_bytes` existed cannot be measured until one of their
  /// schema has been rebuilt once.
  static auto has_size_estimate(const rebuild_run& run,
                                const partition_info& partition) -> bool;

  /// Whether a batch that holds a single partition is worth running.
  static auto is_worth_rebuilding_alone(const rebuild_run& run,
                                        const partition_info& partition)
    -> bool;

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

  /// The maximum number of events in a partition.
  size_t partition_capacity = {};

  /// The batch size a rebuild rebatches its output to.
  size_t desired_batch_size = {};

  /// The rebuild run in progress, if any.
  Option<rebuild_run> rebuild = None{};

  /// The storage policy, or null when no plugin contributes one. The catalog
  /// keeps its built-in behavior in that case.
  std::unique_ptr<storage_policy> policy = {};

  /// How the catalog runs storage maintenance.
  maintenance_options maintenance = {};

  /// Set while a database size measurement is in flight. The measurement runs
  /// off this thread, so a scan slower than the interval must not queue more
  /// of itself up behind it.
  bool measuring_space = false;

  /// Set while an eviction pass is deleting. The pass runs until the database
  /// is back under the low water mark, so the periodic check must not start a
  /// second one on top of it.
  bool evicting = false;

  /// The most recently measured size of the database directory.
  Option<uint64_t> dbdir_size = None{};

  /// How many partitions the budget loop has evicted.
  size_t evicted = 0;

  /// Policy pipelines in flight, against `maintenance.compaction_slots`.
  size_t compacting = 0;

  /// How many policy actions have landed.
  size_t compacted = 0;

  /// The most recently finished run, so that `rebuild show` still describes
  /// it once the run itself is gone.
  Option<rebuild_run> last_rebuild = None{};

  /// Partitions quarantined for an unrecoverable format error, and the
  /// rendered error that caused it. Kept outside `rebuild` so that
  /// `rebuild show` keeps reporting every quarantined partition after the
  /// run that found it has finished. In-memory only: a quarantined partition
  /// is erased from disk, so it can never be reselected after a restart
  /// either way.
  std::unordered_map<uuid, std::string> quarantined_partitions = {};

  /// Builders for the rebuild metrics, unset while no importer is registered.
  Option<series_builder> rebuild_metric = None{};
  Option<series_builder> quarantine_metric = None{};

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
/// @param desired_batch_size The batch size a rebuild rebatches its output to.
/// @param maintenance Whether and how the catalog runs storage maintenance.
/// @param sketch_cache_bytes Memory budget for on-demand loading of deferred
/// Bloom-filter sketches; zero disables on-demand loading.
/// @param lazy_sketches Whether Bloom-filter sketches are deferred; when set,
/// merged synopses also have their Bloom filters dropped to keep resident
/// memory bounded during ongoing ingest.
auto catalog(catalog_actor::stateful_pointer<catalog_state> self,
             filesystem_actor filesystem, partition_paths paths,
             std::string store_backend, index_config synopsis_opts,
             size_t partition_capacity, size_t desired_batch_size,
             maintenance_options maintenance, duration deferred_erase_timeout,
             size_t sketch_cache_bytes = 0, bool lazy_sketches = false)
  -> catalog_actor::behavior_type;

} // namespace tenzir

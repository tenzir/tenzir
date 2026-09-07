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
#include "tenzir/partition_transformer.hpp"
#include "tenzir/plugin_fwd.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/taxonomies.hpp"
#include "tenzir/uuid.hpp"

#include <arrow/vendored/datetime.h>
#include <caf/mail_cache.hpp>
#include <caf/response_type.hpp>
#include <caf/settings.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <chrono>
#include <limits>
#include <list>
#include <map>
#include <set>
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

  /// Automatic runs process closed hourly collections. Manual runs include
  /// the open hour but still have a fixed catalog-admission horizon.
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

/// How the catalog runs storage maintenance.
///
/// Each source is disabled by its own setting rather than collectively: a zero
/// rebuild parallelism, a zero high water mark, or a zero compaction interval
/// stops that one and leaves the rest running.
struct maintenance_options {
  /// The automatic rebuild source's parallelism. Zero disables that source
  /// alone; everything else keeps running.
  size_t automatic_rebuild = 1;

  /// Legacy off switch. Positive values now use hourly collection.
  duration rebuild_interval = {};

  /// Empty selects the node's system timezone.
  std::string rebuild_timezone = {};

  /// An explicit zero disables the decoded-byte budget.
  Option<uint64_t> rebuild_memory_budget = None{};
  double rebuild_merge_margin = 0.6;

  /// The disk budget loop: water marks, step size, scan interval, and the
  /// optional external size command. A zero high water mark disables it, which
  /// is what an unconfigured budget looks like.
  disk_monitor_config space = {};

  /// How many policy pipelines may run at once. Separate from rebuild
  /// parallelism. Policy still wins when both would select the same inputs.
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

/// The pure candidate-evaluation machinery, separated from the catalog's
/// mutable bookkeeping so that lookup workers can run it against a snapshot
/// of the partition synopses. Evaluation has no cross-partition state and no
/// reference back to the catalog; each worker owns one engine, and the
/// catalog keeps one of its own -- without a sketch budget -- for internal,
/// non-query lookups such as rebuild filters.
struct catalog_lookup_engine {
  using universe = catalog_snapshot::map_type;

  /// Evaluates an expression against the given synopses. Two phases: prune
  /// with the resident synopses, then load deferred Bloom-filter sketches on
  /// demand for exactly the candidates such a sketch could still prune.
  auto lookup(expression expr, const universe& synopses_per_type)
    -> caf::expected<catalog_lookup_result>;

  /// Evaluates one schema's synopses. The collection is a parameter (rather
  /// than always a full per-schema map) so the same logic can evaluate a
  /// single partition during on-demand sketch pruning. This is only sound
  /// because the evaluation has no cross-partition state: evaluating a set
  /// equals the union of evaluating each partition alone. Keep it that way.
  /// @param deferred_sketch_partitions Receives the ids of partitions kept
  /// only because a Bloom-filter sketch was deferred and could prune them if
  /// loaded. `lookup` uses this to restrict on-demand loading to exactly
  /// those candidates instead of every surviving candidate in every schema.
  auto lookup_impl(
    const expression& expr, const type& schema,
    const detail::flat_map<uuid, partition_synopsis_ptr>& partition_synopses,
    std::unordered_set<uuid>& deferred_sketch_partitions) const
    -> catalog_lookup_result::candidate_info;

  /// Loads the deferred Bloom-filter sketches of the given partition into the
  /// sketch cache, if possible. Returns the number of bytes loaded, or 0 if
  /// the partition was already cached, has no loadable sketches, or could not
  /// be loaded (in which case the partition stays a conservative candidate).
  auto ensure_sketches_loaded(const uuid& id,
                              const partition_synopsis_ptr& resident) -> size_t;

  /// Taxonomy definitions for concept resolution, snapshotted at spawn.
  tenzir::taxonomies taxonomies = {};

  /// On-demand cache of partitions whose deferred Bloom-filter sketches have
  /// been loaded for pruning. Bounded by a share of
  /// `tenzir.index.sketch-cache-bytes`.
  sketch_cache sketches = {};
};

/// A lookup worker: one engine, driven by delegated candidate requests.
struct catalog_lookup_worker_state {
  static inline constexpr auto name = "catalog-lookup-worker";

  catalog_lookup_engine engine = {};
};

/// Spawns a catalog lookup worker.
auto catalog_lookup_worker(
  catalog_lookup_worker_actor::stateful_pointer<catalog_lookup_worker_state>
    self,
  tenzir::taxonomies taxonomies, size_t sketch_cache_bytes)
  -> catalog_lookup_worker_actor::behavior_type;

/// One outstanding candidate set, keyed by `query_context::id`.
///
/// The lease pins its partitions: their files stay on disk until the lease
/// goes away, even if the partition is replaced or erased in the meantime, so
/// a reader that already received the candidate set can still open it.
struct partition_lease {
  caf::actor_addr owner = {};
  std::vector<uuid> partitions = {};

  /// Distinguishes this lease from a superseded one under the same query id,
  /// so that the asynchronous narrowing after a delegated lookup never
  /// releases pins belonging to a newer lease.
  uint64_t generation = 0;
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

  /// When a *failed* disposal is retried. Set on re-parking after a deletion
  /// or quarantine move failed; independent of `deadline`, because retrying
  /// broken filesystem operations has nothing to do with forcing pinned
  /// erasures, and must work even when forcing is disabled.
  Option<time> retry_at = None{};
};

/// Builds a partition transform marker. `in` names the transform's inputs and
/// `out` its outputs. With `keep == yes` the inputs are omitted: they survive
/// the transform, so a replay must not erase them. A marker with inputs but no
/// outputs is a tombstone: replaying it erases the inputs and nothing else.
auto create_marker(const std::vector<uuid>& in, const std::vector<uuid>& out,
                   keep_original_partition keep, bool quarantine = false,
                   std::string_view policy_token = {},
                   Option<uuid> token_input = None{}, bool finalized = false)
  -> chunk_ptr;

/// A rebuild run in progress inside the catalog.
using RebuildGroups = std::set<std::pair<type, int64_t>>;

struct rebuild_run {
  rebuild_options options = {};

  /// Distinguishes this run from the one it superseded. A manual run can
  /// displace an automatic one whose batches are still in flight; their
  /// continuations then find a run engaged and would otherwise settle their
  /// accounts against it.
  uint64_t generation = 0;

  /// Latest catalog admission this run may consume: its start for manual
  /// work, the closed collection boundary for automatic work.
  uint64_t horizon = std::numeric_limits<uint64_t>::max();

  time started_at = time::clock::now();
  uint64_t batch_byte_budget = 0;

  /// Built once per run; batches still revalidate live claims and policy.
  Option<std::vector<std::list<partition_info>>> groups = None{};
  Option<RebuildGroups> collected_groups = None{};

  /// Eligible inputs withheld by higher-priority policy or an existing claim.
  size_t deferred = 0;

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

  /// How many partitions those batches hold, which is what the rebuild metric
  /// reports.
  size_t running_partitions = 0;

  /// Statistics, as `rebuild show` reports them.
  size_t transformed = 0;
  size_t results = 0;
  size_t quarantined = 0;

  /// Set once a stop was requested; the run winds down instead of selecting
  /// more work.
  bool stopping = false;

  /// The first unrecoverable batch failure. It aborts the run, and everyone
  /// waiting on the run receives it instead of a silent success that skipped
  /// partitions. Corrupt stores do not count: quarantining one is progress.
  Option<caf::error> failure = None{};

  /// Answered when the run finishes.
  std::vector<caf::typed_response_promise<void>> stop_requests = {};
};

struct TransformOptions {
  size_t minimum_partition_reduction = 0;
  double minimum_reduction_ratio = 0;
  std::vector<uuid> required_inputs = {};
  uint64_t input_byte_budget = 0;
  size_t rebuild_batch_size = 0;
};

struct ActivePartitionTransform {
  std::shared_ptr<PartitionTransformProgress> progress = {};
  std::vector<partition_info> input_partitions = {};
  std::string origin = {};
  time started_at = time::clock::now();
  bool stall_reported = false;
};

/// A `compaction run` in progress. The work is queued rather than fired at
/// once: a rule can match every partition in the database, and one transformer
/// per partition would each claim a quarter of available memory.
///
/// Only the partition ids are held. The action is asked for again when the
/// partition's turn comes, so a rule that stopped applying while the run was
/// queued is not carried out against a stale decision.
struct named_rule_run {
  std::string rule = {};
  Option<duration> older_than = None{};
  Option<duration> newer_than = None{};
  std::vector<uuid> pending = {};
  size_t running = 0;

  /// Partitions the run could not process because another transform held
  /// them. Their replacements carry new ids the run never saw, so reporting
  /// plain success would overstate what the rule covered; the run answers
  /// with a retry hint instead.
  size_t skipped = 0;

  caf::error failure = {};
  caf::typed_response_promise<atom::done> promise = {};
};

/// What came of asking the policy to evict a partition.
enum class eviction_outcome {
  /// The policy had no action, so the partition is erased instead.
  none,
  /// An action was started and holds a compaction slot.
  started,
  /// The policy has an action but the pool is full. The partition is left
  /// alone: erasing it would throw away the rewrite the policy asked for.
  deferred,
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
  /// moves the outputs of a committed transform into place and erases its
  /// inputs. Part of `load_from_disk`. Returns every input uuid the markers
  /// name, so the startup scan skips them: their erasure runs asynchronously,
  /// and scanning them back in would resurrect them in memory while their
  /// files disappear underneath.
  auto replay_markers() -> std::unordered_set<uuid>;

  /// Creates the catalog from a set of partition synopses.
  auto initialize(std::vector<partition_synopsis_pair> partitions)
    -> caf::error;

  /// Add a new partition synopsis.
  auto merge(std::vector<partition_synopsis_pair> partitions)
    -> caf::result<atom::ok>;

  /// Whether the policy hears about an erasure. The apply handler passes `no`
  /// for the inputs of a replacement, which it reports through `on_replaced`
  /// instead -- an `on_erased` there would make the policy drop state that the
  /// outputs are supposed to inherit.
  enum class notify_policy : bool { no, yes };

  /// Erase this partition from the catalog. Leaves the on-disk files alone.
  void erase(const uuid& partition, notify_policy notify = notify_policy::yes);

  /// Records an outstanding candidate set and starts monitoring its owner, so
  /// that the lease is released even if the owner never gets around to it.
  /// Returns the lease's generation, or zero when no lease was recorded.
  auto add_lease(const uuid& query, const caf::strong_actor_ptr& owner,
                 std::vector<uuid> partitions) -> uint64_t;

  /// Narrows a lease to the given partitions, releasing the pins of
  /// everything else it held; an empty set releases the lease. No-op unless
  /// the lease still has the given generation: candidates requests take a
  /// provisional lease on the whole snapshot they delegate, and the narrowing
  /// arrives asynchronously with the worker's result.
  void narrow_lease(const uuid& query, uint64_t generation,
                    const std::vector<uuid>& keep);

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

  /// Whether anything still depends on the marker: a disposal or flush hold,
  /// or a deferred erasure that names it as its tombstone.
  [[nodiscard]] auto
  marker_referenced(const std::filesystem::path& marker) const -> bool;

  /// Drops one `markers_in_disposal` reference and erases the marker if it
  /// was the last. Counterpart of the hold a policy-driven transform takes
  /// until its commit is flushed.
  void release_marker_hold(const std::filesystem::path& marker);

  /// Retries writing a finalized marker as long as a hold still references
  /// it. A marker stuck in non-finalized form gates its replay on output
  /// confirmation, and an output that is legitimately erased in the meantime
  /// would strand the payload; once the hold is released the commit is
  /// durable and the content no longer matters.
  void retry_finalize_marker(std::filesystem::path marker, chunk_ptr content);

  /// Flushes the policy and releases the marker hold once the flush
  /// *succeeds*. A failed flush keeps the marker -- it is the commit's only
  /// durable record until the history write lands -- and checks back after
  /// the policy's background retry has had its chance. Without a policy, a
  /// durable history invalidation replaces the flush.
  void release_marker_after_flush(std::filesystem::path marker);

  /// Invalidates stale policy history before discarding policy-less lineage.
  auto invalidate_policy_history() -> caf::error;

  /// Deletes deferred partitions whose deadline has passed, pins and all.
  void sweep_deferred();

  // -- rebuild ----------------------------------------------------------------

  /// Selects the next batch of rebuild work, empty when no schema has enough
  /// eligible partitions left. Rechecks live claims within the fixed admission
  /// horizon; later arrivals belong to a subsequent collection.
  auto select_rebuild_batch(rebuild_run& run, time now = time::clock::now())
    -> std::vector<partition_info>;

  /// Starts a rebuild run, or joins the one already in progress.
  auto start_rebuild(rebuild_options options) -> caf::result<void>;

  /// Winds the current run down: it selects no further work and finishes once
  /// its in-flight batches land.
  auto stop_rebuild(const rebuild_stop_options& options) -> caf::result<void>;

  /// Fills the run's free batch slots with freshly selected work. Called when
  /// a run starts and whenever a batch lands.
  void schedule_rebuild(time now);

  /// Ends the run and answers everyone waiting on it.
  void finish_rebuild();

  /// The only maintenance selector. Call after publishing a complete transition.
  void advance_maintenance(time now);
  void arm_maintenance_wakeup(time now);

  /// Resolves the timezone and initializes deadlines after startup recovery.
  auto initialize_maintenance(time now) -> caf::error;

  auto rebuild_day(time imported) const -> int64_t;
  auto next_rebuild_hour(time now) const -> time;
  void close_rebuild_collection(time now);
  auto blocks_rebuild(uuid const& id, partition_synopsis const& synopsis,
                      time now = time::clock::now()) const -> bool;

  /// Reconcile accounting, then let the shared selector enforce the budget.
  void enforce_disk_budget(time now = time::clock::now());

  /// Defined out of line, like the constructor above: `policy` is incomplete
  /// in this header, so the deleter it needs cannot be generated here.
  ~catalog_state();

  /// Builds the storage policy from whichever plugin contributes one.
  void make_policy();

  /// Replays completed transforms in dependency order into the policy.
  void replay_policy_transforms();

  /// Measures the database directory off this thread, continuing in
  /// `on_space_measured`.
  void measure_space();

  /// Acts on a completed measurement, evicting while over budget.
  void on_space_measured(uint64_t size);

  /// The bytes held by partitions that are erased but still pinned. They are
  /// already spoken for and will come back without erasing anything further,
  /// so the budget loop must not count them against the water marks.
  auto parked_bytes() const -> uint64_t;

  /// The bytes held by partitions whose file deletions are in flight. Same
  /// reasoning as `parked_bytes`: a measurement that races the filesystem
  /// actor still sees them, and counting them would make the loop evict more
  /// than the budget asks for.
  auto deleting_bytes() const -> uint64_t;

  /// How urgently a partition should be evicted; higher goes sooner. Falls
  /// back to its age, which is the scale a policy weight is expressed in.
  auto eviction_weight_of(const uuid& partition,
                          const partition_synopsis& synopsis, time now) const
    -> double;

  /// The next partitions to evict, heaviest first, at most `limit`. Skips
  /// `excluded`, which the eviction pass fills with victims it could not act
  /// on, so that one deferred heavyweight does not shadow every actionable
  /// partition behind it.
  auto select_eviction_batch(size_t limit,
                             const std::unordered_set<uuid>& excluded = {},
                             time now = time::clock::now()) const
    -> std::vector<uuid>;

  /// Gives a compaction slot back and lets anything queued take it. Every
  /// release goes through here so that no path can free a slot without
  /// waking a run that is waiting for one.
  void release_compaction_slot();

  /// Asks the policy about each partition in turn and starts what it asks
  /// for, up to the free slots in the compaction pool.
  void schedule_compaction(time now);

  /// Runs one policy action, holding a slot until it lands.
  void run_policy_action(const uuid& partition, storage_action action,
                         bool eviction = false);

  /// Suppress a rewrite that failed to reclaim bytes, including its new IDs.
  void record_eviction_result(uuid const& input, uint64_t input_bytes,
                              keep_original_partition keep,
                              partition_apply_result const& result);

  /// Runs a named policy rule over every partition it applies to, answering
  /// once the run finishes.
  auto run_named_rule(std::string rule, Option<duration> older_than,
                      Option<duration> newer_than) -> caf::result<atom::done>;

  /// Starts as much of the queued named run as the compaction pool allows.
  void drain_named_rule(time now = time::clock::now());

  /// Ends the named run and answers its caller: the recorded failure if one
  /// occurred, a retry hint when partitions were skipped over concurrent
  /// transforms, and plain success otherwise.
  void finish_named_run();

  /// Settles one finished batch of a named run and continues or answers.
  void finish_named_rule_batch(caf::error error);

  /// Runs the policy's eviction action for a partition, if it has one.
  auto run_eviction_action(const uuid& partition) -> eviction_outcome;

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
             keep_original_partition keep, std::string origin,
             std::string policy_token) -> caf::result<partition_apply_result>;

  /// Selectors call this directly: inputs are claimed before it returns.
  void transform(ast::pipeline pipe, std::vector<partition_info> selected,
                 keep_original_partition keep, std::string origin,
                 std::string policy_token,
                 std::function<void(partition_apply_result&)> success,
                 std::function<void(caf::error&)> failure,
                 TransformOptions options = {});

  auto active_transformations_status() const -> record;

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

  std::unordered_map<uuid, ActivePartitionTransform> active_transformations;

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

  /// Partitions whose file deletions have been handed to the filesystem actor
  /// but have not completed, and their on-disk footprint. An entry leaves when
  /// its store deletion lands, freed or failed either way.
  std::unordered_map<uuid, uint64_t> deleting = {};

  /// No-progress eviction outputs must not spin through fresh UUIDs.
  std::unordered_set<uuid> eviction_suppressed = {};

  /// How many in-flight disposals still need each tombstone. `deferred`
  /// references a marker while an erasure waits on pins; this covers the
  /// window while its files are actually being deleted, so that nothing
  /// erases the marker before the disk is clean.
  std::map<std::filesystem::path, size_t> markers_in_disposal = {};

  /// How long an erased partition may linger because a reader still pins it.
  /// Zero disables forcing.
  duration deferred_erase_timeout = {};

  /// The maximum number of events in a partition.
  size_t partition_capacity = {};

  /// The batch size a rebuild rebatches its output to.
  size_t desired_batch_size = {};

  /// The rebuild run in progress, if any.
  Option<rebuild_run> rebuild = None{};

  /// A manual run waiting for the automatic run's in-flight batches to land.
  /// Discarding those batches would leave their inputs claimed by transforms
  /// the manual run cannot select, so it starts once they settle.
  Option<rebuild_options> pending_rebuild = None{};

  /// Whoever asked for the queued run; adopted as its stop requests when it
  /// starts.
  std::vector<caf::typed_response_promise<void>> pending_rebuild_waiters = {};

  /// The storage policy, or null when no plugin contributes one. The catalog
  /// keeps its built-in behavior in that case.
  std::unique_ptr<storage_policy> policy = {};

  /// How the catalog runs storage maintenance.
  maintenance_options maintenance = {};

  /// Set while a database size measurement is in flight. The measurement runs
  /// off this thread, so a scan slower than the interval must not queue more
  /// of itself up behind it.
  bool measuring_space = false;

  /// A pressure episode stays active until usage reaches the low water mark.
  bool evicting = false;

  /// Physical usage estimate: live, pending deletion, and reconciled overhead.
  Option<uint64_t> dbdir_size = None{};

  /// How many partitions the budget loop has evicted.
  size_t evicted = 0;

  /// Eviction rewrites also consume the disk-budget step allowance.
  size_t eviction_running = 0;

  /// Policy pipelines in flight, against `maintenance.compaction_slots`.
  size_t compacting = 0;

  /// Selection stays disabled until startup state and policy are complete.
  bool maintenance_ready = false;
  bool deciding_maintenance = false;
  caf::disposable maintenance_wakeup = {};
  Option<time> wakeup_at = None{};
  time next_collection = {};
  time next_policy_check = {};
  time next_space_scan = {};
  time next_disposal_check = {};
  time eviction_retry_at = {};
  arrow_vendored::date::time_zone const* rebuild_zone = nullptr;

  /// Monotonic catalog admissions, independent of imported timestamps.
  uint64_t admission_sequence = 0;
  std::unordered_map<uuid, uint64_t> admissions = {};
  uint64_t closed_admission = 0;
  RebuildGroups open_rebuild_groups = {};
  RebuildGroups closed_rebuild_groups = {};

  /// Only changed partitions need policy classification between clock checks.
  std::unordered_set<uuid> policy_dirty = {};
  std::unordered_set<uuid> policy_pending = {};
  std::unordered_set<uuid> eviction_pending = {};

  /// Bytes owned by live catalog partitions; scans reconcile everything else.
  uint64_t catalog_bytes = 0;
  uint64_t external_bytes = 0;
  bool space_reconciled = false;
  uint64_t storage_generation = 0;

  /// A transform finalized by startup marker replay, for the policy to hear
  /// about once it exists. A crash between the durable marker and the
  /// policy's callbacks would otherwise leave the persisted history naming
  /// the erased inputs and missing the completed rule's watermark;
  /// `make_policy()` feeds these to the fresh policy, whose construction has
  /// already settled its state with blocking reads. Replaying state the
  /// history already saw is a no-op.
  struct replayed_transform {
    std::vector<uuid> inputs = {};
    std::vector<partition_info> outputs = {};
    std::string policy_token = {};
    Option<uuid> token_input = None{};

    /// The marker file, kept by the replay for a token-carrying transform so
    /// the commit stays replayable until the policy has persisted it.
    std::filesystem::path marker = {};
  };
  std::vector<replayed_transform> replayed_transforms = {};

  std::unordered_set<uuid> retiring = {};

  /// The `compaction run` in progress, if any.
  Option<named_rule_run> named_run = None{};

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
  // See also ae9dbed. Both levels are immutable behind shared_ptr: a lookup
  // snapshot is one pointer copy, and each in-flight lookup keeps exactly its
  // generation alive. Mutate only through `update_synopses`.
  using synopsis_map = catalog_snapshot::map_type;
  std::shared_ptr<const synopsis_map> synopses_per_type
    = std::make_shared<synopsis_map>();

  /// Publishes a new generation of the synopsis set: `mutate` receives a
  /// private copy of the outer map, and any per-schema map it wants to change
  /// must be cloned too (see `mutable_schema`), because readers -- the lookup
  /// workers -- share the current ones.
  void update_synopses(auto&& mutate) {
    auto next = std::make_shared<synopsis_map>(*synopses_per_type);
    mutate(*next);
    synopses_per_type = std::move(next);
  }

  /// Installs a mutable clone of one schema's synopses into an
  /// under-construction generation, creating the schema if needed, and
  /// returns a reference to it. Clone at most once per schema per generation.
  static auto mutable_schema(synopsis_map& map, const type& schema)
    -> schema_synopsis_map&;

  /// The catalog's own evaluation engine, for internal lookups. It carries
  /// no sketch budget; the configured budget belongs to the workers.
  catalog_lookup_engine lookup_engine = {};

  /// The workers that evaluate candidate requests off this actor's thread,
  /// and the round-robin cursor over them.
  std::vector<catalog_lookup_worker_actor> lookup_pool = {};
  size_t next_lookup_worker_index = 0;

  /// Returns the next worker, round-robin.
  auto next_lookup_worker() -> const catalog_lookup_worker_actor&;

  /// Drops a partition's cached sketches in every worker. Sends are ordered
  /// per worker, so a lookup delegated after this call cannot see the stale
  /// entry.
  void invalidate_sketches(const uuid& partition);

  /// The source of `partition_lease::generation` values.
  uint64_t lease_generation = 0;

  /// Whether Bloom-filter sketches are deferred (see `tenzir.index.lazy-
  /// sketches`). When set, newly merged synopses also have their Bloom filters
  /// dropped so that ongoing ingest does not accumulate them in resident
  /// memory; they are loaded on demand from disk instead.
  bool lazy_sketches = false;
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
/// @param lookup_parallelism How many lookup workers evaluate candidate
/// requests concurrently; the sketch-cache budget is split among them.
auto catalog(catalog_actor::stateful_pointer<catalog_state> self,
             filesystem_actor filesystem, partition_paths paths,
             std::string store_backend, index_config synopsis_opts,
             size_t partition_capacity, size_t desired_batch_size,
             maintenance_options maintenance, duration deferred_erase_timeout,
             size_t sketch_cache_bytes = 0, bool lazy_sketches = false,
             size_t lookup_parallelism = 1) -> catalog_actor::behavior_type;

} // namespace tenzir

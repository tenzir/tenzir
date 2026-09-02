//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/option.hpp"

#include <tenzir/actors.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/catalog.hpp>
#include <tenzir/concept/parseable/tenzir/expression.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/connect_to_node.hpp>
#include <tenzir/data.hpp>
#include <tenzir/defaults.hpp>
#include <tenzir/detail/available_memory.hpp>
#include <tenzir/detail/inspection_common.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/detail/saturating_arithmetic.hpp>
#include <tenzir/detail/weak_run_delayed.hpp>
#include <tenzir/fwd.hpp>
#include <tenzir/index.hpp>
#include <tenzir/node.hpp>
#include <tenzir/partition_synopsis.hpp>
#include <tenzir/partition_transformer.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/query_context.hpp>
#include <tenzir/read_query.hpp>
#include <tenzir/session.hpp>
#include <tenzir/status.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/ast.hpp>
#include <tenzir/tql2/parser.hpp>
#include <tenzir/uuid.hpp>

#include <arrow/table.h>
#include <arrow/vendored/datetime.h>
#include <caf/actor_registry.hpp>
#include <caf/expected.hpp>
#include <caf/policy/select_all.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/settings.hpp>
#include <caf/type_id.hpp>
#include <caf/typed_event_based_actor.hpp>
#include <fmt/format.h>
namespace date = arrow_vendored::date;

#include <cmath>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace tenzir::plugins::rebuild {

namespace {

/// The parsed options of the `tenzir rebuild start` command.
struct start_options {
  bool all = false;
  bool undersized = false;
  size_t parallel = 1;
  size_t max_partitions = std::numeric_limits<size_t>::max();
  class expression expression = {};
  bool detached = false;
  bool automatic = false;

  friend auto inspect(auto& f, start_options& x) {
    return detail::apply_all(f, x.all, x.undersized, x.parallel,
                             x.max_partitions, x.expression, x.detached,
                             x.automatic);
  }
};

/// The parsed options of the `tenzir rebuild stop` command.
struct stop_options {
  bool detached = false;

  friend auto inspect(auto& f, stop_options& x) {
    return f.apply(x.detached);
  }
};

} // namespace

} // namespace tenzir::plugins::rebuild

CAF_BEGIN_TYPE_ID_BLOCK(tenzir_rebuild_plugin_types, 1400)
  CAF_ADD_TYPE_ID(tenzir_rebuild_plugin_types,
                  (tenzir::plugins::rebuild::start_options))
  CAF_ADD_TYPE_ID(tenzir_rebuild_plugin_types,
                  (tenzir::plugins::rebuild::stop_options))
CAF_END_TYPE_ID_BLOCK(tenzir_rebuild_plugin_types)

namespace tenzir::plugins::rebuild {

namespace {

/// The threshold at which to consider a partition undersized, relative to the
/// configured 'tenzir.max-partition-size'.
inline constexpr auto undersized_threshold = 0.8;

/// The minimum number of partitions an open four-hour bucket must eliminate.
inline constexpr auto minimum_open_bucket_reduction = size_t{5};

/// The default fraction of input partitions a closed bucket must eliminate.
inline constexpr auto default_merge_margin = 0.6;

/// Recent partitions remain in fixed four-hour buckets before being promoted
/// to whole local-day buckets.
inline constexpr auto recent_bucket_span = std::chrono::hours{24};
inline constexpr auto recent_bucket_width = std::chrono::hours{4};

/// How long a batch may be in flight without any partition completing before
/// we report it. Generous, because a large merge legitimately takes minutes.
inline constexpr auto stall_threshold = std::chrono::minutes{1};

struct memory_budget {
  uint64_t bytes = 0;
  detail::available_memory_info available = {};
};

struct partition_bucket {
  int64_t local_start_seconds = {};
  bool daily = false;
  bool open = false;

  friend auto operator==(partition_bucket const&, partition_bucket const&)
    -> bool
    = default;
};

auto bucket_for(time timestamp, time now, date::time_zone const* timezone)
  -> partition_bucket {
  TENZIR_ASSERT(timezone);
  const auto local_timestamp = timezone->to_local(
    std::chrono::time_point_cast<std::chrono::seconds>(timestamp));
  const auto local_now = timezone->to_local(
    std::chrono::time_point_cast<std::chrono::seconds>(now));
  const auto day_start = date::floor<date::days>(local_timestamp);
  const auto day_end = day_start + date::days{1};
  // Keep a day split into four-hour buckets until the whole local day has
  // aged out. Promoting pieces of a day one at a time would make the daily
  // bucket grow across successive runs and recreate the churn this policy
  // avoids.
  if (timezone->to_sys(day_end, date::choose::latest)
      <= now - recent_bucket_span) {
    return {
      .local_start_seconds = std::chrono::duration_cast<std::chrono::seconds>(
                               day_start.time_since_epoch())
                               .count(),
      .daily = true,
    };
  }
  const auto hours_since_midnight
    = date::floor<std::chrono::hours>(local_timestamp - day_start);
  const auto bucket_start
    = day_start
      + recent_bucket_width
          * (hours_since_midnight.count() / recent_bucket_width.count());
  const auto current_day_start = date::floor<date::days>(local_now);
  const auto current_hours_since_midnight
    = date::floor<std::chrono::hours>(local_now - current_day_start);
  const auto current_bucket_start = current_day_start
                                    + recent_bucket_width
                                        * (current_hours_since_midnight.count()
                                           / recent_bucket_width.count());
  return {
    .local_start_seconds = std::chrono::duration_cast<std::chrono::seconds>(
                             bucket_start.time_since_epoch())
                             .count(),
    .open = bucket_start == current_bucket_start,
  };
}

/// Determines how many estimated decoded bytes one batch may select.
///
/// Must agree with the budget the transformer enforces while loading (see
/// `make_memory_budget` in partition_transformer.cpp). Selecting more than the
/// transformer will load requeues the remainder and rewrites the same data
/// repeatedly; selecting less merges fewer partitions than we safely could.
auto rebuild_byte_budget(Option<uint64_t> configured, size_t parallelism)
  -> memory_budget {
  const auto divisor
    = std::max<uint64_t>(detail::narrow_cast<uint64_t>(parallelism), 1);
  if (configured) {
    if (*configured == 0) {
      return {
        .bytes = std::numeric_limits<uint64_t>::max(),
        .available = {.bytes = std::numeric_limits<uint64_t>::max(),
                      .source = "tenzir.rebuild-memory-budget"},
      };
    }
    return {
      .bytes = *configured / divisor,
      .available
      = {.bytes = *configured, .source = "tenzir.rebuild-memory-budget"},
    };
  }
  auto available
    = detail::available_memory().value_or(detail::available_memory_info{
      .bytes = uint64_t{512} * 1024 * 1024,
      .source = "fallback",
    });
  return {
    // The transformer currently holds decoded output slices before persisting
    // the new Feather store, and the store writer builds additional Arrow
    // structures during persist. Admit substantially less than the apparent
    // free memory to leave room for those copies and unrelated node activity.
    .bytes = available.bytes / 4 / divisor,
    .available = std::move(available),
  };
}

auto format_bytes(uint64_t bytes) -> std::string {
  if (bytes == std::numeric_limits<uint64_t>::max()) {
    return "unlimited";
  }
  return fmt::format("{} bytes", bytes);
}

/// Statistics for an ongoing rebuild. Numbers are partitions.
struct statistics {
  size_t num_total = {};
  size_t num_rebuilding = {};
  size_t num_completed = {};
  size_t num_results = {};
  size_t num_quarantined = {};
};

/// Identifies a single rebuild run so continuations from a superseded run can
/// detect that they no longer apply, mirroring the compactor's `run_id`.
enum class run_id : uint64_t {};

/// The state of an in-progress rebuild.
struct run {
  struct batch_progress {
    size_t id = {};
    type schema = {};
    std::vector<uuid> input_partitions = {};
    uint64_t store_bytes = {};
    time started_at = time::clock::now();
    std::shared_ptr<PartitionTransformProgress> transform_progress = {};
    bool stall_reported = false;
  };

  /// Distinguishes this run from any run that starts after it. Continuations
  /// launched for this run must check their captured id against
  /// `rebuilder_state::run->id` before touching `run->` state, since `run`
  /// may have been reset and re-emplaced for a new run by the time they fire.
  run_id id = {};
  /// When this run started, for reporting how long it has been going.
  time started_at = time::clock::now();
  /// Whether the run is still waiting for the catalog to return candidates.
  /// Until that completes every statistic below is legitimately zero, which
  /// is otherwise indistinguishable from a run that is stuck.
  bool selecting = true;
  std::vector<partition_info> remaining_partitions = {};
  struct statistics statistics = {};
  std::vector<batch_progress> active_batches = {};
  size_t next_batch_id = {};
  bool failed = false;
  /// Next percentage milestone emitted for an automatic rebuild. Ten-percent
  /// steps bound progress logging independently of the partition count.
  size_t next_progress_percent = 10;
  start_options options = {};
  std::vector<caf::typed_response_promise<void>> stop_requests = {};
  std::vector<caf::typed_response_promise<void>> delayed_rebuilds = {};
};

/// The interface of the REBUILDER actor.
using rebuilder_actor = typed_actor_fwd<
  // Start a rebuild.
  auto(atom::start, start_options)->caf::result<void>,
  // Stop a rebuild.
  auto(atom::stop, stop_options)->caf::result<void>,
  // INTERNAL: Continue working on the in-progress rebuild run with the given
  // id.
  auto(atom::internal, atom::rebuild, uint64_t)->caf::result<void>,
  // INTERNAL: Continue working on the currently in-progress rebuild.
  auto(atom::internal, atom::schedule)->caf::result<void>>
  // Conform to the protocol of the STATUS CLIENT actor.
  ::extend_with<component_plugin_actor>::unwrap;

/// The state of the REBUILDER actor.
struct rebuilder_state {
  /// The actor name as shown in logs.
  [[maybe_unused]] static constexpr const char* name = "rebuilder";

  /// The constructor of the state.
  /// NOTE: gcc-11 requires that we have this explicitly defaulted. :shrug: -- DL
  rebuilder_state() = default;

  /// Actor handles required for the rebuilder.
  rebuilder_actor::pointer self = {};
  catalog_actor catalog = {};
  index_actor index = {};
  importer_actor importer = {};

  /// Emits a `tenzir.metrics.rebuild_quarantine` event; unset if no importer
  /// is registered. Only used on demand, when a partition is quarantined, so
  /// it deliberately does not run on the periodic metrics timer like the
  /// `tenzir.metrics.rebuild` builder below.
  Option<series_builder> quarantine_builder = None{};

  /// Emits a metric event recording that `partition` was quarantined for
  /// `error`. A no-op if no importer is registered.
  void report_quarantine(const uuid& partition, const caf::error& error) {
    if (not importer or not quarantine_builder) {
      return;
    }
    auto event = quarantine_builder->record();
    event.field("timestamp", time::clock::now());
    event.field("partition", fmt::to_string(partition));
    event.field("error", fmt::to_string(error));
    self->mail(quarantine_builder->finish_assert_one_slice()).send(importer);
  }

  /// Constants read once from the system configuration.
  size_t max_partition_size = 0u;
  size_t desired_batch_size = 0u;
  size_t automatic_rebuild = 0u;
  duration rebuild_interval = {};
  /// An explicitly configured total decoded-byte budget. Zero disables the
  /// budget; absence derives it from the memory available during selection.
  Option<uint64_t> rebuild_memory_budget = None{};
  /// Minimum partition-count reduction for closed time buckets.
  double rebuild_merge_margin = default_merge_margin;
  /// Time zone used to align four-hour and daily bucket boundaries.
  date::time_zone const* rebuild_timezone = nullptr;

  auto
  started_batch(const type& schema,
                const std::vector<partition_info>& partitions,
                std::shared_ptr<PartitionTransformProgress> transform_progress)
    -> size_t {
    TENZIR_ASSERT(run);
    auto store_bytes = uint64_t{0};
    auto input_partitions = std::vector<uuid>{};
    input_partitions.reserve(partitions.size());
    for (const auto& partition : partitions) {
      input_partitions.push_back(partition.uuid);
      store_bytes = detail::saturating_add(store_bytes, partition.store_bytes);
    }
    const auto id = run->next_batch_id++;
    run->active_batches.push_back(run::batch_progress{
      .id = id,
      .schema = schema,
      .input_partitions = std::move(input_partitions),
      .store_bytes = store_bytes,
      .transform_progress = std::move(transform_progress),
    });
    return id;
  }

  void finished_batch(size_t batch_id) {
    TENZIR_ASSERT(run);
    const auto removed = std::erase_if(run->active_batches, [&](const auto& x) {
      return x.id == batch_id;
    });
    TENZIR_ASSERT(removed == 1);
  }

  /// Emits at most one message per ten-percent milestone. A large batch may
  /// cross multiple milestones, but still produces just one message.
  void report_progress(const type& schema) {
    TENZIR_ASSERT(run);
    if (not run->options.automatic or run->statistics.num_total == 0
        or run->statistics.num_completed >= run->statistics.num_total) {
      return;
    }
    const auto percent
      = run->statistics.num_completed * 100 / run->statistics.num_total;
    if (percent < run->next_progress_percent) {
      return;
    }
    const auto remaining = run->statistics.num_total
                           - run->statistics.num_completed
                           - run->statistics.num_rebuilding;
    TENZIR_VERBOSE("{} automatic rebuild progress: {}/{} partitions rebuilt "
                   "({}%, {} in flight, {} queued) after {}; latest schema {}",
                   *self, run->statistics.num_completed,
                   run->statistics.num_total, percent,
                   run->statistics.num_rebuilding, remaining,
                   data{time::clock::now() - run->started_at}, schema);
    run->next_progress_percent = std::min<size_t>(100, percent / 10 * 10 + 10);
  }

  /// Warns for each batch that has remained in flight for a while. Every hop
  /// of the transform chain waits without a timeout, so a wedged batch is
  /// otherwise completely silent.
  void check_for_stall() {
    if (not run) {
      return;
    }
    struct stall_snapshot {
      duration stalled_for = {};
      size_t num_rebuilding = {};
      size_t num_completed = {};
      size_t num_total = {};
    };
    const auto now = time::clock::now();
    for (auto& batch : run->active_batches) {
      const auto stalled_for = now - batch.started_at;
      if (stalled_for < stall_threshold or batch.stall_reported) {
        continue;
      }
      batch.stall_reported = true;
      const auto schema = batch.schema;
      const auto batch_id = batch.id;
      const auto this_run = run->id;
      auto current_stall
        = [this, batch_id, this_run]() -> Option<stall_snapshot> {
        if (not run or run->id != this_run) {
          return None{};
        }
        const auto it = std::ranges::find(run->active_batches, batch_id,
                                          &run::batch_progress::id);
        if (it == run->active_batches.end() or not it->stall_reported) {
          return None{};
        }
        const auto stalled_for = time::clock::now() - it->started_at;
        if (stalled_for < stall_threshold) {
          return None{};
        }
        return stall_snapshot{
          .stalled_for = stalled_for,
          .num_rebuilding = it->input_partitions.size(),
          .num_completed = run->statistics.num_completed,
          .num_total = run->statistics.num_total,
        };
      };
      auto transform_progress = batch.transform_progress;
      const auto stalled_phase
        = transform_progress->phase.load(std::memory_order_relaxed);
      self->mail(atom::status_v, status_verbosity::debug, duration::max())
        .request(index, std::chrono::seconds{5})
        .then(
          [this, schema, current_stall, transform_progress,
           stalled_phase](record& index_status) {
            const auto current = current_stall();
            if (not current) {
              return;
            }
            TENZIR_WARN("{} rebuild of schema {} appears stuck: {} "
                        "partition(s) in flight with no completed batch for "
                        "{} ({}/{} partitions done overall); index status: "
                        "{}",
                        *self, schema, current->num_rebuilding,
                        data{current->stalled_for}, current->num_completed,
                        current->num_total, data{std::move(index_status)});
            transform_progress->report_next_phase_transition_from(
              stalled_phase);
          },
          [this, schema, current_stall, transform_progress,
           stalled_phase](const caf::error& error) {
            const auto current = current_stall();
            if (not current) {
              return;
            }
            TENZIR_WARN("{} rebuild of schema {} appears stuck: {} "
                        "partition(s) in flight with no completed batch for "
                        "{} ({}/{} partitions done overall); failed to get "
                        "index status: {}",
                        *self, schema, current->num_rebuilding,
                        data{current->stalled_for}, current->num_completed,
                        current->num_total, error);
            transform_progress->report_next_phase_transition_from(
              stalled_phase);
          });
    }
  }

  /// The state of the ongoing rebuild.
  Option<struct run> run = None{};
  bool stopping = false;

  /// A snapshot of the most recently completed rebuild run, kept around so
  /// `rebuild show` still has something to report once `run` is reset. Only
  /// the latest run is remembered; it is overwritten the next time a run
  /// finishes.
  Option<struct run> last_run = None{};

  /// Counter to distinguish between successive rebuild runs.
  run_id next_run = {};

  /// Partitions quarantined for an unrecoverable format error, and the
  /// rendered error that caused the quarantine. Kept independently of
  /// `run`/`last_run` (which only ever remember the most recent run's
  /// statistics) so `rebuild show` continues to report every quarantined
  /// partition across runs for the lifetime of the process; this set is
  /// in-memory only and does not survive a node restart.
  std::unordered_map<uuid, std::string> quarantined_partitions = {};

  /// Runtime byte estimates for legacy partitions that do not have persisted
  /// `approx_bytes` metadata yet.
  std::unordered_map<type, uint64_t> approx_bytes_per_event = {};

  /// Describes a single run's statistics and options, shared between the
  /// live `current-run` and the historical `last-run` status entries.
  auto describe_run(const struct run& run, bool completed) const -> record {
    auto batches = list{};
    batches.reserve(run.active_batches.size());
    for (const auto& batch : run.active_batches) {
      auto input_partitions = list{};
      input_partitions.reserve(batch.input_partitions.size());
      for (const auto& partition : batch.input_partitions) {
        input_partitions.emplace_back(fmt::to_string(partition));
      }
      batches.emplace_back(record{
        {"schema", std::string{batch.schema.name()}},
        {"duration", time::clock::now() - batch.started_at},
        {"input",
         record{
           {"partitions", std::move(input_partitions)},
           {"store-bytes", batch.store_bytes},
         }},
      });
    }
    const auto phase = completed       ? run.failed ? "failed" : "completed"
                       : run.selecting ? "selecting candidates"
                                       : "rebuilding";
    auto result = record{
      {"phase", phase},
      {"partitions",
       record{
         {"total", run.statistics.num_total},
         {"transforming", run.statistics.num_rebuilding},
         {"transformed", run.statistics.num_completed},
         // Partitions currently being transformed are neither done nor still
         // waiting, so they must not be counted as remaining.
         {"remaining", run.statistics.num_total - run.statistics.num_completed
                         - run.statistics.num_rebuilding},
         {"results", run.statistics.num_results},
         {"quarantined", run.statistics.num_quarantined},
       }},
      {"options",
       record{
         {"all", run.options.all},
         {"undersized", run.options.undersized},
         {"parallel", run.options.parallel},
         {"max-partitions", run.options.max_partitions},
         {"expression", fmt::to_string(run.options.expression)},
         {"detached", run.options.detached},
         {"automatic", run.options.automatic},
         {"merge-margin", rebuild_merge_margin},
         {"timezone", std::string{rebuild_timezone->name()}},
       }},
    };
    result["batches"] = std::move(batches);
    return result;
  }

  /// Shows the status of a currently ongoing rebuild, plus the last
  /// completed run if there is one.
  auto status([[maybe_unused]] status_verbosity verbosity) -> record {
    auto result = record{};
    if (run) {
      result["current-run"] = describe_run(*run, false);
    }
    if (last_run) {
      result["last-run"] = describe_run(*last_run, true);
    }
    result["quarantined-size"] = quarantined_partitions.size();
    auto quarantined = list{};
    quarantined.reserve(quarantined_partitions.size());
    for (const auto& [partition, error] : quarantined_partitions) {
      quarantined.emplace_back(record{
        {"uuid", fmt::to_string(partition)},
        {"error", error},
      });
    }
    result["quarantined"] = std::move(quarantined);
    return result;
  }

  void learn_size_estimate(const partition_info& partition) {
    if (partition.events == 0 or partition.approx_bytes == 0) {
      return;
    }
    const auto bytes_per_event
      = std::max<uint64_t>(partition.approx_bytes / partition.events, 1);
    auto& estimate = approx_bytes_per_event[partition.schema];
    if (estimate == 0) {
      estimate = bytes_per_event;
    } else if (estimate < bytes_per_event) {
      estimate += (bytes_per_event - estimate) / 2;
    } else {
      estimate = bytes_per_event + (estimate - bytes_per_event) / 2;
    }
  }

  auto has_size_estimate(const partition_info& partition) const -> bool {
    return partition.events == 0 or partition.approx_bytes > 0
           or approx_bytes_per_event.contains(partition.schema);
  }

  auto requires_transform(const partition_info& partition) const -> bool {
    return partition.version != version::current_partition_version
           or partition.events > max_partition_size
           or not has_size_estimate(partition);
  }

  /// The event count at or above which a partition counts as adequately sized.
  /// Partitions below this are candidates for an undersized merge, so a merge
  /// that cannot reach it does not accomplish anything.
  auto undersized_events() const -> size_t {
    return detail::narrow_cast<size_t>(
      detail::narrow_cast<double>(max_partition_size) * undersized_threshold);
  }

  auto estimate_approx_bytes(const partition_info& partition,
                             uint64_t unknown_partition_bytes) const
    -> uint64_t {
    if (partition.approx_bytes > 0 or partition.events == 0) {
      return partition.approx_bytes;
    }
    if (auto it = approx_bytes_per_event.find(partition.schema);
        it != approx_bytes_per_event.end()) {
      return detail::saturating_mul(
        it->second, detail::narrow_cast<uint64_t>(partition.events));
    }
    return unknown_partition_bytes;
  }

  /// Start a new rebuild.
  auto start(start_options options) -> caf::result<void> {
    if (options.parallel == 0) {
      return caf::make_error(ec::invalid_configuration,
                             "rebuild requires a non-zero parallel level");
    }
    if (options.automatic and run) {
      // The scheduler re-arms unconditionally, so a run that never finishes
      // suppresses every later automatic rebuild for the lifetime of the
      // process. Returning silently would make that indistinguishable from
      // automatic rebuild being switched off.
      TENZIR_INFO("{} skips automatic rebuild: a run is still ongoing after "
                  "{} ({}/{} partitions done, {} in flight)",
                  *self, data{time::clock::now() - run->started_at},
                  run->statistics.num_completed, run->statistics.num_total,
                  run->statistics.num_rebuilding);
      return {};
    }
    if (run and not run->options.automatic) {
      return caf::make_error(
        ec::invalid_argument,
        fmt::format(
          "{} refuses to start rebuild while a rebuild is still "
          "ongoing ({}/{} done); consider running 'tenzir-ctl rebuild "
          "stop'",
          *self, run->statistics.num_completed, run->statistics.num_total));
    }
    if (not options.automatic and run and run->options.automatic) {
      auto rp = self->make_response_promise<void>();
      self->mail(atom::stop_v, stop_options{.detached = false})
        .request(static_cast<rebuilder_actor>(self), caf::infinite)
        .then(
          [this, rp, options = std::move(options)]() mutable {
            rp.delegate(static_cast<rebuilder_actor>(self), atom::start_v,
                        std::move(options));
          },
          [rp](caf::error& err) mutable {
            rp.deliver(std::move(err));
          });
      return rp;
    }
    run.emplace();
    const auto this_run = next_run;
    next_run = static_cast<run_id>(static_cast<uint64_t>(next_run) + 1);
    run->id = this_run;
    run->options = std::move(options);
    TENZIR_DEBUG("{} requests {}{} partitions matching the expression {}",
                 *self, run->options.all ? "all" : "outdated",
                 run->options.undersized ? " undersized" : "",
                 run->options.expression);
    auto rp = self->make_response_promise<void>();
    auto finish = [this, rp](caf::error err, bool silent = false) mutable {
      if (not silent) {
        // Only print to INFO when work was actually done, or when the run
        // was manually requested.
        if (run->statistics.num_completed == 0) {
          if (run->options.automatic) {
            TENZIR_VERBOSE("{} had nothing to do", *self);
          } else {
            TENZIR_INFO("{} had nothing to do", *self);
          }
        } else {
          TENZIR_INFO("{} rebuilt {} into {} partitions", *self,
                      run->statistics.num_completed,
                      run->statistics.num_results);
        }
      }
      for (auto&& rp : std::exchange(run->stop_requests, {})) {
        rp.deliver();
      }
      // A failed parallel run may finish while sibling transforms still have
      // callbacks in flight. They belong to the superseded live run, not the
      // historical snapshot.
      run->active_batches.clear();
      run->statistics.num_rebuilding = 0;
      run->failed = err.valid();
      // Any run ending completes a pending stop, so the flag must not leak
      // into the next run: a stale `stopping` would make the corrupt-
      // partition handler silently drop the healthy remainder of a failed
      // batch instead of requeuing it.
      stopping = false;
      if (run->options.detached) {
        last_run = std::exchange(run, None{});
        return;
      }
      if (err.valid()) {
        rp.deliver(std::move(err));
        last_run = std::exchange(run, None{});
        return;
      }
      rp.deliver();
      last_run = std::exchange(run, None{});
    };
    if (run->options.detached) {
      rp.deliver();
    }
    auto query_context
      = query_context::make_extract("rebuild", self, run->options.expression);
    query_context.id = uuid::random();
    // On a database with many partitions this lookup can take a while, and
    // until it returns the run reports all-zero statistics. Announce it so
    // that window is recognizable as candidate selection rather than a stall.
    TENZIR_INFO("{} selects rebuild candidates matching {}", *self,
                run->options.expression);
    self->mail(atom::candidates_v, std::move(query_context))
      .request(catalog, caf::infinite)
      .then(
        [this, finish, this_run](catalog_lookup_result& lookup_result) mutable {
          if (not run or run->id != this_run) {
            TENZIR_DEBUG("{} abandons candidate lookup for a superseded "
                         "rebuild run",
                         *self);
            return;
          }
          run->selecting = false;
          TENZIR_ASSERT(run->statistics.num_total == 0);
          for (auto& [type, result] : lookup_result.candidate_infos) {
            std::erase_if(result.partition_infos,
                          [](const partition_info& partition) {
                            return partition.events == 0;
                          });
            if (not run->options.all) {
              std::erase_if(
                result.partition_infos, [&](const partition_info& partition) {
                  if (partition.version < version::current_partition_version) {
                    return false;
                  }
                  if (run->options.undersized
                      and partition.events < undersized_events()) {
                    return false;
                  }
                  return true;
                });
            }
            for (const auto& partition : result.partition_infos) {
              learn_size_estimate(partition);
            }
            if (run->options.undersized) {
              // Prefer small inputs within each time bucket. This prevents a
              // nearly full partition from consuming the event allowance
              // that a larger, profitable group of tiny partitions needs.
              std::stable_sort(result.partition_infos.begin(),
                               result.partition_infos.end(),
                               [](const auto& lhs, const auto& rhs) {
                                 return lhs.events < rhs.events;
                               });
            }
            run->remaining_partitions.insert(run->remaining_partitions.end(),
                                             result.partition_infos.begin(),
                                             result.partition_infos.end());
          }
          if (run->options.undersized) {
            // Apply the global cap to inputs that independently need a
            // transform first. Otherwise, optional tiny partitions can hide
            // outdated or oversized partitions that happen to occur later.
            std::stable_partition(run->remaining_partitions.begin(),
                                  run->remaining_partitions.end(),
                                  [&](const partition_info& partition) {
                                    return requires_transform(partition);
                                  });
          }
          // Apply the cap across all schemas rather than to each one in turn.
          // Applying it per schema made `-n` bound the work by
          // `max_partitions * schemas`, which on a database with dozens of
          // schemas is not a bound the caller would recognize.
          //
          // Truncating the concatenated list keeps the leading schemas whole
          // instead of taking a slice out of every schema. That suits the
          // batching below, which can only merge partitions that share a
          // schema: a thin slice of many schemas would yield batches too small
          // to be worth rewriting.
          if (run->options.max_partitions < run->remaining_partitions.size()) {
            run->remaining_partitions.erase(
              run->remaining_partitions.begin()
                + detail::narrow<ptrdiff_t>(run->options.max_partitions),
              run->remaining_partitions.end());
          }
          run->statistics.num_total = run->remaining_partitions.size();
          if (run->statistics.num_total == 0) {
            TENZIR_DEBUG("{} ignores rebuild request for 0 partitions", *self);
            return finish({}, true);
          }
          if (run->options.automatic) {
            TENZIR_INFO("{} triggered an automatic run for {} candidate "
                        "partitions with {} threads",
                        *self, run->statistics.num_total,
                        run->options.parallel);
          } else {
            TENZIR_INFO(
              "{} triggered a run for {} candidate partitions with {} "
              "threads",
              *self, run->statistics.num_total, run->options.parallel);
          }
          self
            ->mail(atom::internal_v, atom::rebuild_v,
                   static_cast<uint64_t>(this_run))
            .fan_out_request(std::vector<rebuilder_actor>(run->options.parallel,
                                                          self),
                             caf::infinite, caf::policy::select_all_tag)
            .then(
              [this, finish, this_run]() mutable {
                if (not run or run->id != this_run) {
                  TENZIR_DEBUG("{} abandons completion of a superseded "
                               "rebuild run",
                               *self);
                  return;
                }
                finish({});
              },
              [this, finish, this_run](caf::error& error) mutable {
                if (not run or run->id != this_run) {
                  TENZIR_DEBUG("{} abandons completion of a superseded "
                               "rebuild run",
                               *self);
                  return;
                }
                finish(std::move(error));
              });
        },
        [this, finish, this_run](caf::error& error) mutable {
          if (not run or run->id != this_run) {
            TENZIR_DEBUG("{} abandons candidate lookup for a superseded "
                         "rebuild run",
                         *self);
            return;
          }
          run->selecting = false;
          finish(std::move(error));
        });
    return rp;
  }

  /// Stop a rebuild.
  auto stop(const stop_options& options) -> caf::result<void> {
    if (not run) {
      if (not stopping) {
        TENZIR_DEBUG("{} got request to stop rebuild but no rebuild is running",
                     *self);
      } else {
        TENZIR_INFO("{} stopped ongoing rebuild", *self);
      }
      stopping = false;
      return {};
    }
    stopping = true;
    if (not run->remaining_partitions.empty()) {
      TENZIR_ASSERT(run->remaining_partitions.size()
                    == run->statistics.num_total
                         - run->statistics.num_rebuilding
                         - run->statistics.num_completed);
      TENZIR_INFO("{} schedules stop after rebuild of {} partitions currently "
                  "in rebuilding, and will not touch remaining {} partitions",
                  *self, run->statistics.num_rebuilding,
                  run->remaining_partitions.size());
      run->statistics.num_total -= run->remaining_partitions.size();
      run->remaining_partitions.clear();
    }
    if (options.detached) {
      return {};
    }
    auto rp = self->make_response_promise<void>();
    return run->stop_requests.emplace_back(std::move(rp));
  }

  /// Make progress on the rebuild run identified by `rebuild_run`.
  auto rebuild(uint64_t rebuild_run) -> caf::result<void> {
    if (not run or static_cast<uint64_t>(run->id) != rebuild_run) {
      // A worker delegates a fresh rebuild message to self after every batch,
      // but a failing sibling worker ends the run early: the select-all
      // fan-out in `start` reports the first error immediately and `finish`
      // resets `run`. A delegated message that arrives after that has no work
      // left to pick up — and if a new run started in the meantime, it has
      // its own workers, so a stale message must not join it either.
      return {};
    }
    if (run->remaining_partitions.empty()) {
      return {}; // We're done!
    }
    auto current_run_partitions = std::vector<partition_info>{};
    auto current_run_events = size_t{0};
    auto current_run_bytes = uint64_t{0};
    auto current_run_budget
      = rebuild_byte_budget(rebuild_memory_budget, run->options.parallel);
    if (current_run_budget.bytes == 0) {
      return caf::make_error(ec::out_of_memory,
                             "rebuild has no memory budget available "
                             "({} available from {})",
                             format_bytes(current_run_budget.available.bytes),
                             current_run_budget.available.source);
    }
    // Take the first partition and collect partitions of the same schema and
    // import-time bucket. Closed buckets may produce multiple outputs because
    // no new imports will continuously feed their remainder. The open bucket
    // remains capped at one output so repeated automatic runs converge.
    const auto schema = run->remaining_partitions[0].schema;
    const auto selected_bucket
      = bucket_for(run->remaining_partitions[0].max_import_time,
                   run->started_at, rebuild_timezone);
    const auto allow_multiple_outputs
      = run->options.undersized and not selected_bucket.open;
    const auto first_removed = std::remove_if(
      run->remaining_partitions.begin(), run->remaining_partitions.end(),
      [&](const partition_info& partition) {
        const auto same_bucket
          = not run->options.undersized
            or bucket_for(partition.max_import_time, run->started_at,
                          rebuild_timezone)
                 == selected_bucket;
        if (schema == partition.schema and same_bucket
            and (allow_multiple_outputs
                 or current_run_events < max_partition_size)) {
          const auto partition_bytes
            = estimate_approx_bytes(partition, current_run_budget.bytes);
          if (not current_run_partitions.empty()
              and detail::saturating_add(current_run_bytes, partition_bytes)
                    > current_run_budget.bytes) {
            return false;
          }
          if (not allow_multiple_outputs and not current_run_partitions.empty()
              and current_run_events + partition.events > max_partition_size) {
            return false;
          }
          current_run_bytes
            = detail::saturating_add(current_run_bytes, partition_bytes);
          current_run_events += partition.events;
          current_run_partitions.push_back(partition);
          TENZIR_TRACE("{} selects partition {} (v{}, {}) with "
                       "{} events and {} estimated bytes (total: {} events, "
                       "{} estimated bytes, budget: {})",
                       *self, partition.uuid, partition.version,
                       partition.schema, partition.events, partition_bytes,
                       current_run_events, current_run_bytes,
                       current_run_budget.bytes);
          return true;
        }
        return false;
      });
    run->remaining_partitions.erase(first_removed,
                                    run->remaining_partitions.end());
    run->statistics.num_rebuilding += current_run_partitions.size();
    // Current data must eliminate a fixed number of partitions before it is
    // rewritten. Closed buckets must eliminate at least one partition and use
    // the configured proportional reduction. Oversized or outdated
    // partitions, and those we cannot size, always rebuild.
    auto merged_events = size_t{0};
    auto required_partitions = std::vector<uuid>{};
    if (run->options.undersized) {
      for (const auto& partition : current_run_partitions) {
        if (requires_transform(partition)) {
          required_partitions.push_back(partition.uuid);
        }
        merged_events += partition.events;
      }
    }
    const auto minimum_partition_reduction
      = run->options.undersized ? selected_bucket.open
                                    ? uint64_t{minimum_open_bucket_reduction}
                                    : uint64_t{1}
                                : uint64_t{0};
    const auto minimum_reduction_ratio
      = run->options.undersized and not selected_bucket.open
          ? rebuild_merge_margin
          : double{0};
    const auto estimated_output_partitions
      = merged_events / max_partition_size
        + static_cast<size_t>(merged_events % max_partition_size != 0);
    if (required_partitions.empty() and run->options.undersized
        and not satisfies_partition_reduction(
          current_run_partitions.size(), estimated_output_partitions,
          minimum_partition_reduction, minimum_reduction_ratio)) {
      const auto skipped = current_run_partitions.size();
      run->statistics.num_rebuilding -= skipped;
      run->statistics.num_total -= skipped;
      // Pick up new work until we run out of remainig partitions.
      return self->mail(atom::internal_v, atom::rebuild_v, rebuild_run)
        .delegate(static_cast<rebuilder_actor>(self));
    }
    auto transform_progress = std::make_shared<PartitionTransformProgress>();
    const auto batch_id
      = started_batch(schema, current_run_partitions, transform_progress);
    TENZIR_DEBUG("{} selected {} partition(s) for rebuild of schema {} with {} "
                 "estimated decoded bytes (budget: {}, available: {} from {})",
                 *self, current_run_partitions.size(), schema,
                 format_bytes(current_run_bytes),
                 format_bytes(current_run_budget.bytes),
                 format_bytes(current_run_budget.available.bytes),
                 current_run_budget.available.source);
    // Ask the index to rebuild the partitions we selected.
    auto rp = self->make_response_promise<void>();
    auto dh = null_diagnostic_handler{};
    auto provider = session_provider::make(dh);
    auto pipeline = schema.name().starts_with("suricata")
                      ? fmt::format("where timestamp? != null | batch {}",
                                    desired_batch_size)
                      : fmt::format("batch {}", desired_batch_size);
    auto rebatch = parse_pipeline_with_location_override(
      pipeline, location::unknown, provider.as_session());
    TENZIR_ASSERT(rebatch);
    // Sort the selected partitions from old to new so the rebuild transform
    // sees the batches (and events) in the order they arrived. Put required
    // inputs first without changing their relative order so a memory-limited
    // prefix cannot skip them in favor of optional undersized partitions.
    std::sort(current_run_partitions.begin(), current_run_partitions.end(),
              [](const partition_info& lhs, const partition_info& rhs) {
                return lhs.max_import_time < rhs.max_import_time;
              });
    std::stable_partition(
      current_run_partitions.begin(), current_run_partitions.end(),
      [&](const partition_info& partition) {
        return std::ranges::contains(required_partitions, partition.uuid);
      });
    auto selected_partitions = current_run_partitions;
    auto retry_partitions = current_run_partitions;
    const auto num_partitions = current_run_partitions.size();
    const auto this_run = run->id;
    self
      ->mail(atom::apply_v, std::move(*rebatch),
             std::move(current_run_partitions), keep_original_partition::no,
             std::string{"rebuild"}, minimum_partition_reduction,
             minimum_reduction_ratio, std::move(required_partitions),
             current_run_budget.bytes, std::move(transform_progress))
      .request(index, caf::infinite)
      .then(
        [this, rp, selected_partitions = std::move(selected_partitions),
         num_partitions, batch_id, this_run,
         schema](partition_apply_result& result) mutable {
          if (not run or run->id != this_run) {
            TENZIR_DEBUG("{} abandons rebuild continuation for a superseded "
                         "run",
                         *self);
            rp.deliver();
            return;
          }
          if (result.input_partitions.empty()
              and result.output_partitions.empty()) {
            if (result.skipped) {
              TENZIR_DEBUG("{} skipped {} partitions because the inputs loaded "
                           "at runtime did not meet the merge threshold",
                           *self, num_partitions);
            } else {
              TENZIR_DEBUG("{} skipped {} partitions as they are already being "
                           "transformed by another actor",
                           *self, num_partitions);
            }
            run->statistics.num_total -= num_partitions;
            run->statistics.num_rebuilding -= num_partitions;
            finished_batch(batch_id);
            // Pick up new work until we run out of remaining partitions.
            rp.delegate(static_cast<rebuilder_actor>(self), atom::internal_v,
                        atom::rebuild_v, static_cast<uint64_t>(this_run));
            return;
          }
          auto unconsumed_partitions = selected_partitions;
          std::erase_if(unconsumed_partitions, [&](const auto& partition) {
            return std::find(result.input_partitions.begin(),
                             result.input_partitions.end(), partition)
                   != result.input_partitions.end();
          });
          if (not result.input_complete or not unconsumed_partitions.empty()) {
            TENZIR_DEBUG("{} requeues {} partition(s) that were selected but "
                         "not loaded by the transformer",
                         *self, unconsumed_partitions.size());
            run->remaining_partitions.insert(run->remaining_partitions.begin(),
                                             unconsumed_partitions.begin(),
                                             unconsumed_partitions.end());
          }
          TENZIR_DEBUG("{} rebuilt {} into {} partitions", *self,
                       result.input_partitions.size(),
                       result.output_partitions.size());
          for (const auto& partition : result.output_partitions) {
            learn_size_estimate(partition);
          }
          // If the number of events in the resulting partitions does not
          // match the number of events in the partitions that went in we ran
          // into a conflict with other partition transformations on an
          // overlapping set.
          const auto input_events = std::transform_reduce(
            result.input_partitions.begin(), result.input_partitions.end(),
            size_t{}, std::plus<>{}, [](const partition_info& partition) {
              return partition.events;
            });
          const auto result_events = std::transform_reduce(
            result.output_partitions.begin(), result.output_partitions.end(),
            size_t{}, std::plus<>{}, [](const partition_info& partition) {
              return partition.events;
            });
          if (input_events != result_events) {
            TENZIR_WARN("{} detected a mismatch: rebuilt {} events from {} "
                        "partitions into {} events in {} partitions",
                        *self, input_events, result.input_partitions.size(),
                        result_events, result.output_partitions.size());
          }
          // Adjust the counters, update the indicator, and move back
          // undersized transformed partitions to the list of remainig
          // partitions as desired.
          run->statistics.num_completed += result.input_partitions.size();
          run->statistics.num_results += result.output_partitions.size();
          run->statistics.num_rebuilding -= num_partitions;
          finished_batch(batch_id);
          report_progress(schema);
          // Pick up new work until we run out of remainig partitions.
          rp.delegate(static_cast<rebuilder_actor>(self), atom::internal_v,
                      atom::rebuild_v, static_cast<uint64_t>(this_run));
        },
        [this, retry_partitions = std::move(retry_partitions), num_partitions,
         batch_id, this_run, rp, schema](caf::error& error) mutable {
          if (not run or run->id != this_run) {
            TENZIR_DEBUG("{} abandons rebuild continuation for a superseded "
                         "run",
                         *self);
            rp.deliver();
            return;
          }
          // The partition transformer attributes a store decode failure
          // (e.g. a corrupt/truncated store file) to the specific partition
          // it came from (see `store_error_partition`), so a batch failure
          // never needs to be treated as "some partition in here is
          // corrupt" — we can drop exactly the culprit and retry the rest
          // of the batch normally. The partition is also quarantined by the
          // catalog below so it is never selected as a rebuild candidate
          // again; without that, a future run would reselect it and fail
          // again instead of being skipped.
          if (const auto corrupt = store_error_partition(error)) {
            const auto it
              = std::find_if(retry_partitions.begin(), retry_partitions.end(),
                             [&](const partition_info& partition) {
                               return partition.uuid == *corrupt;
                             });
            TENZIR_ASSERT(it != retry_partitions.end());
            report_quarantine(*corrupt, error);
            ++run->statistics.num_quarantined;
            quarantined_partitions[*corrupt] = fmt::to_string(error);
            retry_partitions.erase(it);
            run->statistics.num_rebuilding -= num_partitions;
            finished_batch(batch_id);
            // Fire the quarantine mail and only log its outcome here: the
            // catalog's mailbox already orders this erase before any future
            // candidate query, so the run can continue immediately below
            // without waiting for the response.
            self
              ->mail(atom::erase_v, atom::extract_v, *corrupt,
                     fmt::to_string(error))
              .request(catalog, caf::infinite)
              .then(
                [this, corrupt = *corrupt](atom::done) {
                  TENZIR_DEBUG("{} quarantined partition {} in the catalog",
                               *self, corrupt);
                },
                [this, corrupt = *corrupt](const caf::error& quarantine_err) {
                  TENZIR_WARN("{} failed to quarantine partition {} in the "
                              "catalog: {}",
                              *self, corrupt, quarantine_err);
                });
            if (stopping) {
              // A stop request already decided not to touch any partition
              // beyond the ones currently rebuilding, so don't requeue the
              // survivors of this batch or pick up more work; just drop
              // them from this run's accounting and let this worker finish.
              run->statistics.num_total -= 1 + retry_partitions.size();
              rp.deliver();
              return;
            }
            run->remaining_partitions.insert(run->remaining_partitions.begin(),
                                             retry_partitions.begin(),
                                             retry_partitions.end());
            run->statistics.num_total -= 1;
            rp.delegate(static_cast<rebuilder_actor>(self), atom::internal_v,
                        atom::rebuild_v, static_cast<uint64_t>(this_run));
            return;
          }
          TENZIR_WARN("{} failed to rebuild partitions: {}", *self, error);
          run->remaining_partitions.insert(run->remaining_partitions.begin(),
                                           retry_partitions.begin(),
                                           retry_partitions.end());
          run->statistics.num_rebuilding -= num_partitions;
          finished_batch(batch_id);
          rp.deliver(std::move(error));
        });
    return rp;
  }

  /// Schedule a rebuild run.
  auto schedule() -> void {
    auto options = start_options{
      .all = false,
      .undersized = true,
      .parallel = automatic_rebuild,
      .max_partitions = std::numeric_limits<size_t>::max(),
      .expression = trivially_true_expression(),
      .detached = true,
      .automatic = true,
    };
    self->mail(atom::internal_v, atom::schedule_v)
      .delay(rebuild_interval)
      .send(self);
    self->mail(atom::start_v, std::move(options))
      .request(static_cast<rebuilder_actor>(self), caf::infinite)
      .then(
        [this] {
          TENZIR_DEBUG("{} finished automatic rebuild", *self);
        },
        [this](const caf::error& err) {
          TENZIR_WARN("{} failed during automatic rebuild: {}", *self, err);
        });
  }
};

/// Defines the behavior of the REBUILDER actor.
/// @param self A pointer to this actor.
/// @param catalog A handle to the CATALOG actor.
/// @param index A handle to the INDEX actor.
rebuilder_actor::behavior_type
rebuilder(rebuilder_actor::stateful_pointer<rebuilder_state> self,
          catalog_actor catalog, index_actor index) {
  self->state().self = self;
  self->state().catalog = std::move(catalog);
  self->state().index = std::move(index);
  self->state().max_partition_size
    = caf::get_or(content(self->system().config()), "tenzir.max-partition-size",
                  defaults::max_partition_size);
  self->state().desired_batch_size
    = caf::get_or(content(self->system().config()), "tenzir.import.batch-size",
                  defaults::import::table_slice_size);
  self->state().automatic_rebuild = caf::get_or(
    content(self->system().config()), "tenzir.automatic-rebuild", size_t{1});
  self->state().rebuild_merge_margin
    = caf::get_or(content(self->system().config()),
                  "tenzir.rebuild-merge-margin", default_merge_margin);
  if (not std::isfinite(self->state().rebuild_merge_margin)
      or self->state().rebuild_merge_margin < 0
      or self->state().rebuild_merge_margin > 1) {
    auto error
      = caf::make_error(ec::invalid_configuration,
                        "tenzir.rebuild-merge-margin must be between 0 and 1");
    TENZIR_ERROR("{}", render(error));
    self->quit(error);
    return rebuilder_actor::behavior_type::make_empty_behavior();
  }
  const auto* configured_timezone = caf::get_if<std::string>(
    &content(self->system().config()), "tenzir.rebuild-timezone");
  try {
    if (configured_timezone) {
      self->state().rebuild_timezone = date::locate_zone(*configured_timezone);
    } else {
      try {
        self->state().rebuild_timezone = date::current_zone();
      } catch (std::runtime_error const&) {
        // Minimal containers and Nix build sandboxes may not provide
        // /etc/localtime. UTC is the conventional system default in that case.
        self->state().rebuild_timezone = date::locate_zone("UTC");
      }
    }
  } catch (std::runtime_error const& ex) {
    auto error = caf::make_error(
      ec::invalid_configuration, "failed to resolve {} time zone: {}",
      configured_timezone
        ? fmt::format("tenzir.rebuild-timezone '{}'", *configured_timezone)
        : std::string{"system"},
      ex.what());
    TENZIR_ERROR("{}", render(error));
    self->quit(error);
    return rebuilder_actor::behavior_type::make_empty_behavior();
  }
  if (const auto* configured = caf::get_if<caf::config_value::integer>(
        &content(self->system().config()), "tenzir.rebuild-memory-budget")) {
    if (*configured < 0) {
      auto error
        = caf::make_error(ec::invalid_configuration,
                          "tenzir.rebuild-memory-budget must not be negative");
      TENZIR_ERROR("{}", render(error));
      self->quit(error);
      return rebuilder_actor::behavior_type::make_empty_behavior();
    }
    self->state().rebuild_memory_budget
      = detail::narrow_cast<uint64_t>(*configured);
  }
  if (self->state().automatic_rebuild > 0) {
    self->state().rebuild_interval
      = caf::get_or(content(self->system().config()), "tenzir.rebuild-interval",
                    defaults::rebuild_interval);
    TENZIR_INFO("{} runs automatic rebuilds every {} with {} thread(s), a "
                "memory budget of {}, a merge margin of {:.0f}%, and {} time "
                "zone buckets",
                *self, data{self->state().rebuild_interval},
                self->state().automatic_rebuild,
                self->state().rebuild_memory_budget
                  ? format_bytes(*self->state().rebuild_memory_budget == 0
                                   ? std::numeric_limits<uint64_t>::max()
                                   : *self->state().rebuild_memory_budget)
                  : "automatic",
                self->state().rebuild_merge_margin * 100,
                self->state().rebuild_timezone->name());
    // We delay the first run such that we do not do it during initialization
    // where there are many other things going on. For long-running processes,
    // we on average already waited for half the duration before.
    detail::weak_run_delayed(self, self->state().rebuild_interval / 2, [self] {
      self->state().schedule();
    });
  } else {
    TENZIR_INFO("{} has automatic rebuilds disabled via "
                "'tenzir.automatic-rebuild'; partitions are only merged by an "
                "explicit 'rebuild start'",
                *self);
  }
  // Runs regardless of whether metrics are wired up, since this is the only
  // signal that a wedged batch produces.
  detail::weak_run_delayed_loop(self, defaults::metrics_interval, [self] {
    self->state().check_for_stall();
  });
  if (auto importer
      = self->system().registry().get<importer_actor>("tenzir.importer")) {
    self->state().importer = importer;
    self->state().quarantine_builder = series_builder{type{
      "tenzir.metrics.rebuild_quarantine",
      record_type{
        {"timestamp", time_type{}},
        {"partition", string_type{}},
        {"error", string_type{}},
      },
      {{"internal"}},
    }};
    auto builder = series_builder{type{
      "tenzir.metrics.rebuild",
      record_type{
        {"timestamp", time_type{}},
        {"partitions", uint64_type{}},
        {"queued_partitions", uint64_type{}},
      },
      {{"internal"}},
    }};
    detail::weak_run_delayed_loop(
      self, defaults::metrics_interval,
      [self, importer = std::move(importer),
       builder = std::move(builder)]() mutable {
        const auto partitions = self->state().run
                                  ? self->state().run->statistics.num_rebuilding
                                  : 0;
        const auto queued_partitions
          = self->state().run ? self->state().run->statistics.num_total
                                  - self->state().run->statistics.num_completed
                                  - self->state().run->statistics.num_rebuilding
                              : 0;
        auto metric = builder.record();
        metric.field("timestamp", time::clock::now());
        metric.field("partitions", partitions);
        metric.field("queued_partitions", queued_partitions);
        self->mail(builder.finish_assert_one_slice()).send(importer);
      });
  }
  return {
    [self](atom::status, status_verbosity verbosity, duration) {
      return self->state().status(verbosity);
    },
    [self](atom::start, start_options& options) {
      return self->state().start(std::move(options));
    },
    [self](atom::stop, const stop_options& options) {
      return self->state().stop(options);
    },
    [self](atom::internal, atom::rebuild, uint64_t rebuild_run) {
      return self->state().rebuild(rebuild_run);
    },
    [self](atom::internal, atom::schedule) {
      return self->state().schedule();
    },
    [self](const caf::exit_msg& msg) {
      TENZIR_DEBUG("{} received EXIT from {}: {}", *self, msg.source,
                   msg.reason);
      if (not self->state().run) {
        self->quit(msg.reason);
        return;
      }
      for (auto&& rp : std::exchange(self->state().run->stop_requests, {})) {
        rp.deliver(msg.reason);
      }
      for (auto&& rp : std::exchange(self->state().run->delayed_rebuilds, {})) {
        rp.deliver(msg.reason);
      }
      self->quit(msg.reason);
    },
  };
}

/// A helper function to get a handle to the REBUILDER actor from a client
/// process.
caf::expected<rebuilder_actor> get_rebuilder(caf::actor_system& sys) {
  auto self = caf::scoped_actor{sys};
  auto node_opt = connect_to_node(self);
  if (not node_opt) {
    return std::move(node_opt.error());
  }
  auto result = caf::expected<caf::actor>{caf::error{}};
  const auto node = std::move(*node_opt);
  self->mail(atom::get_v, atom::label_v, std::vector<std::string>{"rebuilder"})
    .request(node, caf::infinite)
    .receive(
      [&](std::vector<caf::actor>& actors) {
        if (actors.empty()) {
          result = caf::make_error(ec::logic_error,
                                   "rebuilder is not in component "
                                   "registry; the server process may be "
                                   "running without the rebuilder plugin");
        } else {
          // There should always only be one MATCHER SUPERVISOR at a given time.
          // We cannot, however, assign a specific label when adding to the
          // registry, and lookup by label only works reliably for singleton
          // components, and we cannot make the MATCHER SUPERVISOR a singleton
          // component from outside libtenzir.
          TENZIR_ASSERT(actors.size() == 1);
          result = std::move(actors[0]);
        }
      },
      [&](caf::error& err) { //
        result = std::move(err);
      });
  if (not result) {
    return std::move(result.error());
  }
  return caf::actor_cast<rebuilder_actor>(std::move(*result));
}

caf::message
rebuild_start_command(const invocation& inv, caf::actor_system& sys) {
  // Create a scoped actor for interaction with the actor system and connect to
  // the node.
  auto self = caf::scoped_actor{sys};
  auto rebuilder = get_rebuilder(sys);
  if (not rebuilder) {
    return caf::make_message(std::move(rebuilder.error()));
  }
  // Parse the query expression, iff it exists.
  auto query = read_query(inv, "tenzir.rebuild.read", must_provide_query::no);
  if (not query) {
    return caf::make_message(std::move(query.error()));
  }
  auto expr = expression{};
  if (query->empty()) {
    expr = trivially_true_expression();
  } else {
    auto parsed = to<expression>(*query);
    if (not parsed) {
      return caf::make_message(std::move(parsed.error()));
    }
    expr = std::move(*parsed);
  }
  auto options = start_options{
    .all = caf::get_or(inv.options, "tenzir.rebuild.all", false),
    .undersized = caf::get_or(inv.options, "tenzir.rebuild.undersized", false),
    .parallel = caf::get_or(inv.options, "tenzir.rebuild.parallel", size_t{1}),
    .max_partitions = caf::get_or(inv.options, "tenzir.rebuild.max-partitions",
                                  std::numeric_limits<size_t>::max()),
    .expression = std::move(expr),
    .detached = caf::get_or(inv.options, "tenzir.rebuild.detached", false),
    .automatic = false,
  };
  auto result = caf::message{};
  self->mail(atom::start_v, std::move(options))
    .request(*rebuilder, caf::infinite)
    .receive(
      [] {
        // nop
      },
      [&](caf::error& err) {
        result = caf::make_message(std::move(err));
      });
  return result;
}

caf::message
rebuild_stop_command(const invocation& inv, caf::actor_system& sys) {
  // Create a scoped actor for interaction with the actor system and connect to
  // the node.
  auto self = caf::scoped_actor{sys};
  auto rebuilder = get_rebuilder(sys);
  if (not rebuilder) {
    return caf::make_message(std::move(rebuilder.error()));
  }
  auto result = caf::message{};
  auto options = stop_options{
    .detached = caf::get_or(inv.options, "tenzir.rebuild.detached", false),
  };
  self->mail(atom::stop_v, std::move(options))
    .request(*rebuilder, caf::infinite)
    .receive(
      [] {
        // nop
      },
      [&](caf::error& err) {
        result = caf::make_message(std::move(err));
      });
  return result;
}

caf::message rebuild_show_command(const invocation&, caf::actor_system& sys) {
  // Create a scoped actor for interaction with the actor system and connect to
  // the node.
  auto self = caf::scoped_actor{sys};
  auto rebuilder = get_rebuilder(sys);
  if (not rebuilder) {
    return caf::make_message(std::move(rebuilder.error()));
  }
  auto err = caf::error{};
  self->mail(atom::status_v, status_verbosity::debug, duration::max())
    .request(*rebuilder, caf::infinite)
    .receive(
      [&](const record& status) {
        auto yaml = to_yaml(status);
        if (not yaml) {
          err = std::move(yaml.error());
          return;
        }
        fmt::print("{}\n", *yaml);
      },
      [&](caf::error& error) {
        err = std::move(error);
      });
  if (err.valid()) {
    return caf::make_message(std::move(err));
  }
  return {};
}

/// An example plugin.
class plugin final : public virtual command_plugin,
                     public virtual component_plugin {
public:
  /// Loading logic.
  plugin() = default;

  /// Teardown logic.
  ~plugin() override = default;

  /// Initializes a plugin with its respective entries from the YAML config
  /// file, i.e., `plugin.<NAME>`.
  /// @param config The relevant subsection of the configuration.
  auto initialize(const record&, const record&) -> caf::error override {
    return caf::none;
  }

  /// Returns the unique name of the plugin.
  auto name() const -> std::string override {
    return "rebuild";
  }

  auto component_name() const -> std::string override {
    return "rebuilder";
  }

  /// Creates additional commands.
  auto make_command() const
    -> std::pair<std::unique_ptr<command>, command::factory> override {
    auto rebuild = std::make_unique<command>(
      "rebuild",
      "rebuilds outdated partitions matching the "
      "(optional) query expression",
      command::opts("?tenzir.rebuild")
        .add<bool>("all", "rebuild all partitions")
        .add<bool>("undersized", "consider only undersized partitions")
        .add<bool>("detached,d", "exit immediately instead of waiting for the "
                                 "rebuild to finish")
        .add<std::string>("read,r", "path for reading the (optional) query")
        .add<int64_t>("max-partitions,n", "number of partitions to rebuild at "
                                          "most (default: unlimited)")
        .add<int64_t>("parallel,j", "number of runs to start in parallel "
                                    "(default: 1)"));
    rebuild->add_subcommand("start",
                            "rebuilds outdated partitions matching the "
                            "(optional) query qexpression",
                            rebuild->options);
    rebuild->add_subcommand(
      "stop", "stop an ongoing rebuild process",
      command::opts("?tenzir.rebuild")
        .add<bool>("detached,d", "exit immediately instead of waiting for the "
                                 "rebuild to be stopped"));
    rebuild->add_subcommand("show", "shows the current rebuild status",
                            command::opts("?tenzir.rebuild"));
    auto factory = command::factory{
      {"rebuild start", rebuild_start_command},
      // Make 'tenzir rebuild' an alias for 'tenzir rebuild start'.
      {"rebuild", rebuild_start_command},
      {"rebuild stop", rebuild_stop_command},
      {"rebuild show", rebuild_show_command},
    };
    return {std::move(rebuild), std::move(factory)};
  }

  auto make_component(node_actor::stateful_pointer<node_state> node) const
    -> component_plugin_actor override {
    auto [catalog, index]
      = node->state().registry.find<catalog_actor, index_actor>();
    return node->spawn(rebuilder, std::move(catalog), std::move(index));
  }
};

} // namespace

} // namespace tenzir::plugins::rebuild
TENZIR_REGISTER_PLUGIN(tenzir::plugins::rebuild::plugin)
TENZIR_REGISTER_PLUGIN_TYPE_ID_BLOCK(tenzir_rebuild_plugin_types)

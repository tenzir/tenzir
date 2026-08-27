//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

// The catalog's storage maintenance. Every source selects its work here,
// inside the catalog's own actor context and against live state, so no
// partition set ever crosses an actor boundary to be acted on later. See
// `catalog_transform.cpp` for what running the selected work looks like.

#include "tenzir/catalog.hpp"
#include "tenzir/detail/available_memory.hpp"
#include "tenzir/detail/saturating_arithmetic.hpp"
#include "tenzir/error.hpp"
#include "tenzir/importer.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/partition_synopsis.hpp"
#include "tenzir/partition_transformer.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/plugin/storage_policy.hpp"
#include "tenzir/session.hpp"
#include "tenzir/tql2/parser.hpp"
#include "tenzir/version.hpp"

#include <caf/actor_registry.hpp>
#include <caf/event_based_actor.hpp>

#include <algorithm>
#include <numeric>
#include <ranges>

namespace tenzir {

namespace {

/// The decoded-bytes budget for one rebuild batch. The transformer holds
/// decoded output slices before persisting the new store, and the store writer
/// builds further Arrow structures during persist, so we admit substantially
/// less than the apparent free memory.
auto rebuild_byte_budget(size_t parallelism) -> uint64_t {
  const auto available
    = detail::available_memory().value_or(detail::available_memory_info{
      .bytes = uint64_t{512} * 1024 * 1024,
      .source = "fallback",
    });
  const auto divisor = std::max<uint64_t>(parallelism, 1);
  return available.bytes / 4 / divisor;
}

/// The rebatching pipeline for a schema. Suricata partitions additionally drop
/// events without a timestamp, which is how they became rebuild candidates in
/// the first place.
auto rebuild_pipeline(const type& schema, size_t desired_batch_size)
  -> ast::pipeline {
  auto source
    = schema.name().starts_with("suricata")
        ? fmt::format("where timestamp? != null | batch {}", desired_batch_size)
        : fmt::format("batch {}", desired_batch_size);
  auto dh = null_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto result = parse_pipeline_with_location_override(source, location::unknown,
                                                      provider.as_session());
  TENZIR_ASSERT(result);
  return std::move(*result);
}

} // namespace

void catalog_state::learn_size_estimate(rebuild_run& run,
                                        const partition_info& partition) {
  if (partition.events == 0 or partition.approx_bytes == 0) {
    return;
  }
  const auto bytes_per_event
    = std::max<uint64_t>(partition.approx_bytes / partition.events, 1);
  auto& estimate = run.approx_bytes_per_event[partition.schema];
  if (estimate == 0) {
    estimate = bytes_per_event;
  } else if (estimate < bytes_per_event) {
    estimate += ((bytes_per_event - estimate) / 2);
  } else {
    estimate = bytes_per_event + ((estimate - bytes_per_event) / 2);
  }
}

auto catalog_state::estimate_approx_bytes(const rebuild_run& run,
                                          const partition_info& partition,
                                          uint64_t unknown) -> uint64_t {
  if (partition.approx_bytes > 0 or partition.events == 0) {
    return partition.approx_bytes;
  }
  if (const auto it = run.approx_bytes_per_event.find(partition.schema);
      it != run.approx_bytes_per_event.end()) {
    return detail::saturating_mul(it->second,
                                  static_cast<uint64_t>(partition.events));
  }
  return unknown;
}

auto catalog_state::has_size_estimate(const rebuild_run& run,
                                      const partition_info& partition) -> bool {
  return partition.events == 0 or partition.approx_bytes > 0
         or run.approx_bytes_per_event.contains(partition.schema);
}

auto catalog_state::is_worth_rebuilding_alone(const rebuild_run& run,
                                              const partition_info& partition)
  -> bool {
  // Termination is `rebuild_run::visited`'s job, so this is purely about not
  // spending a transform that buys nothing. A lone partition is rewritten
  // into something very close to itself, which is worth it when:
  //
  //  - the user asked for it. `--all` means "rewrite everything", e.g. after a
  //    format change, and honouring that is the whole point of the flag.
  //  - it is outdated, and the rewrite brings it to the current version.
  //  - we cannot size it. A partition written before `approx_bytes` existed is
  //    assumed to fill the entire memory budget, so it never shares a batch
  //    with anything; refusing lone batches would leave every such partition
  //    unrebuilt for good, and those are exactly what a rebuild is for.
  //
  // What is left is a current, measurable, merely undersized partition, which
  // a rebuild would rewrite into an equally undersized one. That one waits for
  // a partner.
  if (run.options.all) {
    return true;
  }
  if (partition.version < version::current_partition_version) {
    return true;
  }
  return not has_size_estimate(run, partition);
}

auto catalog_state::is_rebuild_candidate(const uuid& partition,
                                         const partition_synopsis& synopsis,
                                         const rebuild_run& run) const -> bool {
  // A partition another transform already holds would be refused by `apply`
  // anyway, and selecting it would only waste a batch slot.
  if (in_transformation.contains(partition)) {
    return false;
  }
  // Selecting the same partition, or a partition this run produced, is how
  // continuous re-selection fails to terminate.
  if (run.visited.contains(partition)) {
    return false;
  }
  if (synopsis.events == 0) {
    return false;
  }
  // A manual run ignores everything ingested after it started, which is what
  // lets it terminate while data keeps arriving.
  if (run.horizon and synopsis.max_import_time >= *run.horizon) {
    return false;
  }
  if (run.options.all) {
    return true;
  }
  if (synopsis.version < version::current_partition_version) {
    return true;
  }
  return run.options.undersized
         and synopsis.events < static_cast<size_t>(
               static_cast<double>(partition_capacity) * undersized_threshold);
}

auto catalog_state::select_rebuild_batch(rebuild_run& run)
  -> std::vector<partition_info> {
  if (run.selected >= run.options.max_partitions) {
    return {};
  }
  const auto budget = rebuild_byte_budget(run.options.parallel);
  if (budget == 0) {
    TENZIR_WARN("{} has no memory budget for a rebuild batch", name);
    return {};
  }
  // Restrict the pool to the run's expression. A trivially true expression --
  // what the automatic source uses -- would select everything anyway, so we
  // skip the lookup rather than walking every synopsis for it.
  auto matching = Option<std::unordered_set<uuid>>{None{}};
  if (run.options.expression != trivially_true_expression()) {
    auto candidates = lookup(run.options.expression);
    if (not candidates) {
      TENZIR_WARN("{} failed to resolve the rebuild expression {}: {}", name,
                  run.options.expression, candidates.error());
      return {};
    }
    matching.emplace();
    for (const auto& [schema, info] : candidates->candidate_infos) {
      for (const auto& partition : info.partition_infos) {
        matching->insert(partition.uuid);
      }
    }
  }
  // Group the eligible partitions by schema. Schemas are tried in name order
  // and partitions within one oldest first, so that selection is deterministic
  // and a batch merges data that arrived close together.
  auto eligible = std::map<std::string, std::vector<partition_info>>{};
  for (const auto& [schema, partitions] : synopses_per_type) {
    for (const auto& [id, synopsis] : partitions) {
      if (matching and not matching->contains(id)) {
        continue;
      }
      if (not is_rebuild_candidate(id, *synopsis, run)) {
        continue;
      }
      auto& partition
        = eligible[std::string{schema.name()}].emplace_back(id, *synopsis);
      // Seed the per-schema estimate from every candidate, not just the ones
      // that end up in this batch: one measurable partition is what lets its
      // unmeasurable siblings be sized, and without that they are each assumed
      // to fill the whole budget and never share a batch.
      learn_size_estimate(run, partition);
    }
  }
  const auto remaining = run.options.max_partitions - run.selected;
  for (auto& [schema_name, partitions] : eligible) {
    std::ranges::sort(partitions, {}, &partition_info::max_import_time);
    auto batch = std::vector<partition_info>{};
    auto events = size_t{0};
    auto bytes = uint64_t{0};
    for (const auto& partition : partitions) {
      if (events >= partition_capacity or batch.size() >= remaining) {
        break;
      }
      const auto partition_bytes
        = estimate_approx_bytes(run, partition, budget);
      if (not batch.empty()
          and detail::saturating_add(bytes, partition_bytes) > budget) {
        break;
      }
      bytes = detail::saturating_add(bytes, partition_bytes);
      events += partition.events;
      batch.push_back(partition);
    }
    if (batch.empty()) {
      continue;
    }
    if (batch.size() == 1
        and not is_worth_rebuilding_alone(run, batch.front())) {
      continue;
    }
    TENZIR_VERBOSE("{} selected {} partition(s) of schema {} for rebuild with "
                   "{} events and {} estimated bytes (budget: {})",
                   catalog_state::name, batch.size(), schema_name, events,
                   bytes, budget);
    return batch;
  }
  return {};
}

auto catalog_state::begin_rebuild(rebuild_options options) -> caf::error {
  if (options.parallel == 0) {
    return caf::make_error(ec::invalid_configuration,
                           "rebuild requires a non-zero parallel level");
  }
  // The automatic source never displaces a run that is already going; it just
  // tries again on its next pass.
  if (options.automatic and rebuild) {
    return {};
  }
  if (rebuild and not rebuild->options.automatic) {
    return caf::make_error(
      ec::invalid_argument,
      fmt::format("{} refuses to start a rebuild while one is still ongoing "
                  "({} partitions transformed); consider 'tenzir-ctl rebuild "
                  "stop'",
                  *self, rebuild->transformed));
  }
  // A manual run supersedes the automatic one.
  if (rebuild) {
    TENZIR_VERBOSE("{} stops the automatic rebuild for a manual one", *self);
    finish_rebuild();
  }
  // Number every run, so a superseded run's continuations can tell that the
  // accounting they are about to touch is no longer theirs.
  static auto generation = uint64_t{0};
  rebuild.emplace();
  rebuild->generation = ++generation;
  rebuild->options = std::move(options);
  // A manual run only considers what already exists, so that it terminates
  // while data keeps arriving. The automatic source deliberately does not.
  if (not rebuild->options.automatic) {
    rebuild->horizon = time::clock::now();
  }
  TENZIR_DEBUG("{} starts a{} rebuild of {}{} partitions with {} thread(s)",
               *self, rebuild->options.automatic ? "n automatic" : " manual",
               rebuild->options.all ? "all" : "outdated",
               rebuild->options.undersized ? " and undersized" : "",
               rebuild->options.parallel);
  schedule_rebuild();
  return {};
}

auto catalog_state::start_rebuild(rebuild_options options)
  -> caf::result<void> {
  const auto detached = options.detached;
  if (auto error = begin_rebuild(std::move(options)); error.valid()) {
    return error;
  }
  // `schedule_rebuild` may have finished the run already, and a detached
  // caller does not want to wait for one that has not.
  if (detached or not rebuild) {
    return {};
  }
  auto rp = self->make_response_promise<void>();
  rebuild->stop_requests.push_back(rp);
  return rp;
}

auto catalog_state::stop_rebuild(const rebuild_stop_options& options)
  -> caf::result<void> {
  if (not rebuild) {
    return {};
  }
  rebuild->stopping = true;
  TENZIR_VERBOSE("{} stops the rebuild after its {} in-flight batch(es)", *self,
                 rebuild->running);
  if (rebuild->running == 0) {
    finish_rebuild();
    return {};
  }
  if (options.detached) {
    return {};
  }
  auto rp = self->make_response_promise<void>();
  rebuild->stop_requests.push_back(rp);
  return rp;
}

void catalog_state::finish_rebuild() {
  TENZIR_ASSERT(rebuild);
  auto run = *std::exchange(rebuild, None{});
  if (run.transformed == 0) {
    TENZIR_VERBOSE("{} had nothing to rebuild", *self);
  } else {
    TENZIR_INFO("{} rebuilt {} into {} partitions", *self, run.transformed,
                run.results);
  }
  auto stop_requests = std::exchange(run.stop_requests, {});
  // Only the statistics and options are ever read back; the run's bookkeeping
  // grows with the number of partitions it touched, so it does not stay
  // resident for the lifetime of the node.
  run.visited = {};
  run.approx_bytes_per_event = {};
  last_rebuild = std::move(run);
  for (auto& rp : stop_requests) {
    rp.deliver();
  }
}

void catalog_state::schedule_rebuild() {
  TENZIR_ASSERT(rebuild);
  while (not rebuild->stopping
         and rebuild->running < rebuild->options.parallel) {
    auto batch = select_rebuild_batch(*rebuild);
    if (batch.empty()) {
      break;
    }
    for (const auto& partition : batch) {
      rebuild->visited.insert(partition.uuid);
    }
    const auto size = batch.size();
    rebuild->selected += size;
    ++rebuild->running;
    auto pipeline = rebuild_pipeline(batch.front().schema, desired_batch_size);
    // Go through the handler rather than calling `apply` directly: it returns
    // a promise, and this is the one place that needs a continuation on it.
    self
      ->mail(atom::apply_v, std::move(pipeline), std::move(batch),
             keep_original_partition::no, std::string{"rebuild"})
      .request(caf::actor_cast<catalog_actor>(self), caf::infinite)
      .then(
        [this, generation = rebuild->generation](
          partition_apply_result& result) {
          if (not rebuild or rebuild->generation != generation) {
            // A later run replaced ours. Its accounting is not ours to settle:
            // decrementing its `running` would wrap the counter and wedge it.
            return;
          }
          --rebuild->running;
          for (const auto& partition : result.output_partitions) {
            learn_size_estimate(*rebuild, partition);
            // The output is a fresh partition that may well match the run's
            // filter again. Marking it visited is what bounds the run.
            rebuild->visited.insert(partition.uuid);
          }
          rebuild->transformed += result.input_partitions.size();
          rebuild->results += result.output_partitions.size();
          // A mismatch means another transform took part of the batch out from
          // under this one. `apply` refuses overlaps, so the batch is simply
          // smaller than selected; there is nothing to repair.
          const auto events = [](const auto& partitions) {
            return std::transform_reduce(partitions.begin(), partitions.end(),
                                         size_t{}, std::plus<>{},
                                         [](const partition_info& partition) {
                                           return partition.events;
                                         });
          };
          if (events(result.input_partitions)
              != events(result.output_partitions)) {
            TENZIR_WARN("{} rebuilt {} events from {} partitions into {} "
                        "events in {} partitions",
                        *self, events(result.input_partitions),
                        result.input_partitions.size(),
                        events(result.output_partitions),
                        result.output_partitions.size());
          }
          schedule_rebuild_or_finish();
        },
        [this, size, generation = rebuild->generation](caf::error& error) {
          if (not rebuild or rebuild->generation != generation) {
            return;
          }
          --rebuild->running;
          // The transformer blames a store decode failure on the partition it
          // came from, so we can quarantine exactly the culprit instead of
          // treating the whole batch as suspect. Without the quarantine the
          // next pass would select it again and fail the same way forever.
          if (const auto corrupt = store_error_partition(error)) {
            TENZIR_WARN("{} quarantines partition {} after a rebuild failure: "
                        "{}",
                        *self, *corrupt, error);
            ++rebuild->quarantined;
            quarantined_partitions[*corrupt] = fmt::to_string(error);
            report_quarantine(*corrupt, error);
            std::ignore = erase_and_extract(*corrupt, fmt::to_string(error));
          } else {
            TENZIR_WARN("{} failed to rebuild {} partitions: {}", *self, size,
                        error);
          }
          schedule_rebuild_or_finish();
        });
  }
  if (rebuild->running == 0) {
    finish_rebuild();
  }
}

void catalog_state::schedule_rebuild_or_finish() {
  TENZIR_ASSERT(rebuild);
  if (rebuild->stopping and rebuild->running == 0) {
    finish_rebuild();
    return;
  }
  schedule_rebuild();
}

void catalog_state::report_quarantine(const uuid& partition,
                                      const caf::error& error) {
  if (not quarantine_metric) {
    return;
  }
  const auto importer
    = self->system().registry().get<importer_actor>("tenzir.importer");
  if (not importer) {
    return;
  }
  auto event = quarantine_metric->record();
  event.field("timestamp", time::clock::now());
  event.field("partition", fmt::to_string(partition));
  event.field("error", fmt::to_string(error));
  self->mail(quarantine_metric->finish_assert_one_slice()).send(importer);
}

auto catalog_state::describe_rebuild_run(const rebuild_run& run) -> record {
  return record{
    {"partitions",
     record{
       // Selection is incremental, so unlike a run over a snapshot this has
       // no total to count down from.
       {"selected", run.selected},
       {"batches", run.running},
       {"transformed", run.transformed},
       {"results", run.results},
       {"quarantined", run.quarantined},
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
     }},
    {"stopping", run.stopping},
  };
}

auto catalog_state::rebuild_status() const -> record {
  auto result = record{};
  if (rebuild) {
    result["current-run"] = describe_rebuild_run(*rebuild);
  }
  if (last_rebuild) {
    result["last-run"] = describe_rebuild_run(*last_rebuild);
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

// -- the disk budget loop -----------------------------------------------------

auto catalog_state::parked_bytes() const -> uint64_t {
  auto result = uint64_t{0};
  for (const auto& [partition, entry] : deferred) {
    if (not entry.synopsis) {
      continue;
    }
    // The files, not `approx_bytes`: that is a decoded-size estimate the
    // rebuild batcher weighs against available memory, and it runs several
    // times the compressed on-disk footprint. Subtracting it from a
    // `compute_dbdir_size` measurement would make the loop stop evicting while
    // still over budget.
    result += entry.synopsis->store_file.size
              + entry.synopsis->indexes_file.size
              + entry.synopsis->sketches_file.size;
  }
  return result;
}

auto catalog_state::eviction_weight_of(const uuid& partition,
                                       const partition_synopsis& synopsis) const
  -> double {
  const auto age = std::chrono::duration<double>{time::clock::now()
                                                 - synopsis.max_import_time}
                     .count();
  if (policy) {
    if (const auto weighted = policy->eviction_weight(partition, synopsis)) {
      return *weighted;
    }
  }
  // No policy, or none for this partition: age alone. A weight is a multiplier
  // on age rather than a scale of its own, so this mixes with weighted answers
  // -- and it is the same as an unweighted schema gets from compaction, whose
  // default weight is 1. Ordering descending is oldest-first, the built-in
  // behavior.
  return age;
}

auto catalog_state::select_eviction_batch(size_t limit) const
  -> std::vector<uuid> {
  if (limit == 0) {
    return {};
  }
  // Heaviest goes first. Without a policy the weight is the age, so this is
  // oldest-first. The disk monitor ordered by the partition file's mtime,
  // which a rebuild resets: rewriting an old partition made it look young and
  // moved it to the back of the queue. `max_import_time` is a property of the
  // events, so it survives a rewrite.
  auto candidates = std::vector<std::pair<double, uuid>>{};
  for (const auto& [schema, partitions] : synopses_per_type) {
    for (const auto& [partition, synopsis] : partitions) {
      if (in_transformation.contains(partition)) {
        // Erasing an input would let its data resurrect through the
        // transform's output. The next pass picks it up once it is free.
        continue;
      }
      candidates.emplace_back(eviction_weight_of(partition, *synopsis),
                              partition);
    }
  }
  const auto count = std::min(limit, candidates.size());
  std::partial_sort(candidates.begin(),
                    candidates.begin() + static_cast<ptrdiff_t>(count),
                    candidates.end(), [](const auto& lhs, const auto& rhs) {
                      return lhs.first > rhs.first;
                    });
  auto result = std::vector<uuid>{};
  result.reserve(count);
  for (auto i = size_t{0}; i < count; ++i) {
    result.push_back(candidates[i].second);
  }
  return result;
}

void catalog_state::measure_space() {
  TENZIR_ASSERT(not measuring_space);
  measuring_space = true;
  // `compute_dbdir_size` walks the whole database, or shells out to an
  // external binary. The disk monitor could block on that because nothing
  // else went through it; the catalog answers candidate lookups, so the scan
  // runs on a throwaway detached actor rather than on this thread.
  auto worker = self->spawn<caf::detached>(
    [dir = paths.database_dir,
     config = maintenance.space](caf::event_based_actor*) -> caf::behavior {
      return {
        [dir, config](atom::get) -> caf::result<uint64_t> {
          auto size = compute_dbdir_size(dir, config);
          if (not size) {
            return std::move(size.error());
          }
          return static_cast<uint64_t>(*size);
        },
      };
    });
  self->mail(atom::get_v)
    .request(worker, caf::infinite)
    .then(
      [this, worker](uint64_t size) {
        self->send_exit(worker, caf::exit_reason::user_shutdown);
        measuring_space = false;
        on_space_measured(size);
      },
      [this, worker](caf::error& error) {
        self->send_exit(worker, caf::exit_reason::user_shutdown);
        measuring_space = false;
        // Abandon the pass rather than keep deleting blind: without a
        // measurement there is no way to tell when to stop.
        evicting = false;
        TENZIR_WARN("{} failed to measure the size of {}: {}", *self,
                    paths.database_dir, error);
      });
}

void catalog_state::on_space_measured(uint64_t size) {
  dbdir_size = size;
  // Partitions that are erased but still pinned keep their files until the
  // last reader drops. They are already on their way out, so counting them
  // against the budget would make one pressure episode erase far more than it
  // needs to while freeing nothing until the pins clear.
  const auto parked = parked_bytes();
  const auto effective = size - std::min(size, parked);
  // A pass that has started runs down to the low water mark; otherwise the
  // node would sit just under the high mark and re-trigger constantly.
  const auto threshold = evicting ? maintenance.space.low_water_mark
                                  : maintenance.space.high_water_mark;
  if (effective <= threshold) {
    if (evicting) {
      TENZIR_VERBOSE("{} is back under its disk budget at {} bytes", *self,
                     effective);
      evicting = false;
    }
    return;
  }
  const auto batch = select_eviction_batch(maintenance.space.step_size);
  if (batch.empty()) {
    TENZIR_WARN("{} is over its disk budget at {} bytes but has no partition "
                "it may evict",
                *self, effective);
    evicting = false;
    return;
  }
  evicting = true;
  TENZIR_VERBOSE("{} evicts {} partition(s) to get from {} bytes under {}",
                 *self, batch.size(), effective, threshold);
  for (const auto& partition : batch) {
    if (run_eviction_action(partition)) {
      continue;
    }
    // The selection already skipped everything `retire` would refuse, and the
    // budget loop has nobody to report an error to; a failure is logged there.
    std::ignore = retire(partition, None{});
    ++evicted;
  }
  // Re-measure rather than subtract an estimate: the erasures may be deferred
  // behind pins, in which case nothing was actually freed yet.
  measure_space();
}

void catalog_state::release_compaction_slot() {
  TENZIR_ASSERT(compacting > 0);
  // The single place a slot is given back. Anything that waits on a free slot
  // hooks in here, so that no release path can be added later that forgets to
  // wake it.
  --compacting;
}

void catalog_state::maintenance_pass() {
  if (not policy) {
    return;
  }
  // Walk the partitions and ask about each one. Selection is per partition and
  // against live state, so a partition that becomes ineligible between two
  // passes is simply not asked about again.
  for (const auto& [schema, partitions] : synopses_per_type) {
    for (const auto& [partition, synopsis] : partitions) {
      if (compacting >= maintenance.compaction_slots) {
        // The pool is full. What is left waits for the next pass rather than
        // queueing, so that the work is re-decided against fresh state.
        return;
      }
      if (in_transformation.contains(partition)) {
        continue;
      }
      auto action = policy->maintenance_action(partition, *synopsis);
      if (not action) {
        continue;
      }
      run_maintenance_action(partition, std::move(*action));
    }
  }
}

void catalog_state::run_maintenance_action(const uuid& partition,
                                           storage_action action) {
  auto synopsis = find_synopsis(partition);
  if (not synopsis) {
    return;
  }
  auto batch = std::vector<partition_info>{};
  batch.emplace_back(partition, *synopsis);
  ++compacting;
  self
    ->mail(atom::apply_v, std::move(action.pipeline), std::move(batch),
           action.keep, action.origin)
    .request(caf::actor_cast<catalog_actor>(self), caf::infinite)
    .then(
      [this, partition,
       recorded = action.token](partition_apply_result& result) {
        ++compacted;
        if (policy) {
          policy->on_committed(recorded, partition, result.output_partitions);
        }
        release_compaction_slot();
      },
      [this, partition, recorded = action.token](caf::error& error) {
        TENZIR_WARN("{} failed to run a maintenance action on partition {}: "
                    "{}",
                    *self, partition, error);
        if (policy) {
          policy->on_failed(recorded, partition, error);
        }
        release_compaction_slot();
      });
}

auto catalog_state::run_eviction_action(const uuid& partition) -> bool {
  if (not policy) {
    return false;
  }
  auto synopsis = find_synopsis(partition);
  if (not synopsis) {
    return false;
  }
  auto action = policy->eviction_action(partition, *synopsis);
  if (not action) {
    // No action means erase outright, which is what an unconfigured eviction
    // has always done.
    return false;
  }
  // The policy owns this partition either way: erasing it because the pool is
  // busy would throw away the rewrite it asked for. Leaving it for the next
  // measurement round is the only correct answer.
  if (compacting >= maintenance.compaction_slots) {
    TENZIR_DEBUG("{} defers the eviction action for partition {} because the "
                 "compaction pool is full",
                 *self, partition);
    return true;
  }
  auto batch = std::vector<partition_info>{};
  batch.emplace_back(partition, *synopsis);
  ++compacting;
  self
    ->mail(atom::apply_v, std::move(action->pipeline), std::move(batch),
           action->keep, action->origin)
    .request(caf::actor_cast<catalog_actor>(self), caf::infinite)
    .then(
      [this, partition,
       recorded = action->token](partition_apply_result& result) {
        ++evicted;
        if (policy) {
          policy->on_committed(recorded, partition, result.output_partitions);
        }
        release_compaction_slot();
      },
      [this, partition, recorded = action->token](caf::error& error) {
        TENZIR_WARN("{} failed to run the eviction action on partition {}: {}",
                    *self, partition, error);
        if (policy) {
          policy->on_failed(recorded, partition, error);
        }
        release_compaction_slot();
      });
  return true;
}

auto catalog_state::space_status() const -> record {
  if (maintenance.space.high_water_mark == 0) {
    return {};
  }
  auto result = record{
    {"compacting", compacting},
    {"compacted", compacted},
    {"evicting", evicting},
    {"evicted", evicted},
    {"parked-bytes", parked_bytes()},
    {"high-water-mark", uint64_t{maintenance.space.high_water_mark}},
    {"low-water-mark", uint64_t{maintenance.space.low_water_mark}},
  };
  if (dbdir_size) {
    result["dbdir-size"] = *dbdir_size;
  }
  return result;
}

} // namespace tenzir

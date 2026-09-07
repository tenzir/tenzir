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
#include "tenzir/defaults.hpp"
#include "tenzir/detail/available_memory.hpp"
#include "tenzir/detail/saturating_arithmetic.hpp"
#include "tenzir/detail/weak_run_delayed.hpp"
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
auto rebuild_byte_budget(Option<uint64_t> configured, size_t parallelism)
  -> uint64_t {
  if (configured) {
    return *configured == 0 ? std::numeric_limits<uint64_t>::max()
                            : *configured / std::max<size_t>(parallelism, 1);
  }
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

auto catalog_state::rebuild_day(time imported) const -> int64_t {
  auto const seconds = std::chrono::floor<std::chrono::seconds>(imported);
  auto const local = rebuild_zone
                       ? rebuild_zone->to_local(seconds).time_since_epoch()
                       : seconds.time_since_epoch();
  return std::chrono::floor<std::chrono::days>(local).count();
}

auto catalog_state::next_rebuild_hour(time now) const -> time {
  TENZIR_ASSERT(rebuild_zone);
  auto const info = rebuild_zone->get_info(now);
  auto const local = rebuild_zone->to_local(now);
  auto const boundary
    = std::chrono::floor<std::chrono::hours>(local) + std::chrono::hours{1};
  auto const candidate = time{boundary.time_since_epoch() - info.offset};
  // The offset transition itself closes the current window, including a
  // repeated hour. Never resolve an ambiguous local label back to system time.
  if (info.end <= std::chrono::floor<std::chrono::seconds>(candidate)) {
    return time{info.end.time_since_epoch()};
  }
  return candidate;
}

auto catalog_state::initialize_maintenance(time now) -> caf::error {
  try {
    if (maintenance.rebuild_timezone.empty()) {
      try {
        rebuild_zone = arrow_vendored::date::current_zone();
      } catch (std::runtime_error const&) {
        // Minimal containers may have no /etc/localtime. UTC is the system
        // default there; an explicit invalid timezone must still fail.
        rebuild_zone = arrow_vendored::date::locate_zone("UTC");
      }
    } else {
      rebuild_zone
        = arrow_vendored::date::locate_zone(maintenance.rebuild_timezone);
    }
  } catch (std::exception const& error) {
    return caf::make_error(
      ec::invalid_configuration,
      fmt::format("failed to resolve {} time zone: {}",
                  maintenance.rebuild_timezone.empty()
                    ? std::string{"system"}
                    : fmt::format("tenzir.rebuild-timezone '{}'",
                                  maintenance.rebuild_timezone),
                  error.what()));
  }
  next_collection = next_rebuild_hour(now);
  next_policy_check = now;
  next_space_scan = now;
  next_disposal_check = now;
  for (auto const& [schema, partitions] : *synopses_per_type) {
    for (auto const& [id, synopsis] : *partitions) {
      admissions[id] = ++admission_sequence;
      open_rebuild_groups.emplace(schema,
                                  rebuild_day(synopsis->max_import_time));
      catalog_bytes += synopsis->store_file.size + synopsis->indexes_file.size
                       + synopsis->sketches_file.size;
      policy_dirty.insert(id);
    }
  }
  return {};
}

auto catalog_state::blocks_rebuild(uuid const& id,
                                   partition_synopsis const& synopsis,
                                   time now) const -> bool {
  if (policy_pending.contains(id) or eviction_pending.contains(id)
      or (policy and policy->blocks_rebuild(id, synopsis, now))) {
    return true;
  }
  if (named_run and policy
      and policy->named_action(named_run->rule, named_run->older_than,
                               named_run->newer_than, id, synopsis, now)) {
    return true;
  }
  return false;
}

void catalog_state::close_rebuild_collection(time now) {
  if (maintenance.automatic_rebuild == 0
      or maintenance.rebuild_interval <= duration::zero()) {
    open_rebuild_groups.clear();
    closed_rebuild_groups.clear();
    return;
  }
  if (maintenance_ready and now >= next_collection) {
    closed_admission = admission_sequence;
    closed_rebuild_groups.merge(open_rebuild_groups);
    open_rebuild_groups.clear();
    next_collection = next_rebuild_hour(now);
  }
}

void catalog_state::advance_maintenance(time now) {
  if (not maintenance_ready or deciding_maintenance) {
    return;
  }
  deciding_maintenance = true;
  close_rebuild_collection(now);
  auto const policy_interval
    = policy ? policy->maintenance_interval() : duration::zero();
  if (policy_interval > duration::zero() and now >= next_policy_check) {
    for (auto const& [schema, partitions] : *synopses_per_type) {
      for (auto const& [id, synopsis] : *partitions) {
        policy_dirty.insert(id);
      }
    }
    next_policy_check = time::max();
  }
  auto const space_enabled
    = maintenance.space.high_water_mark > 0
      and maintenance.space.scan_interval > std::chrono::seconds::zero();
  if (space_enabled and now >= next_space_scan) {
    next_space_scan = now + maintenance.space.scan_interval;
    if (not measuring_space) {
      measure_space();
    }
  }
  if (now >= next_disposal_check) {
    sweep_deferred();
  }
  // Selection and claims happen in this order, before asynchronous execution.
  enforce_disk_budget(now);
  drain_named_rule(now);
  schedule_compaction(now);
  auto const automatic_enabled
    = maintenance.automatic_rebuild > 0
      and maintenance.rebuild_interval > duration::zero();
  if (automatic_enabled and not closed_rebuild_groups.empty() and rebuild
      and rebuild->options.automatic and rebuild->running == 0) {
    // A closed hour can extend the pending work without letting an old
    // policy conflict hold up unrelated groups indefinitely.
    if (rebuild->groups) {
      for (auto const& group : *rebuild->groups) {
        for (auto const& input : group) {
          auto synopsis = find_synopsis(input.uuid);
          if (synopsis
              and is_rebuild_candidate(input.uuid, *synopsis, *rebuild)) {
            closed_rebuild_groups.emplace(input.schema,
                                          rebuild_day(input.max_import_time));
          }
        }
      }
    }
    finish_rebuild();
  }
  if (automatic_enabled and not closed_rebuild_groups.empty() and not rebuild
      and not pending_rebuild) {
    auto error = begin_rebuild(rebuild_options{
      .undersized = true,
      .parallel = maintenance.automatic_rebuild,
      .expression = trivially_true_expression(),
      .automatic = true,
    });
    if (error) {
      TENZIR_WARN("{} failed to start automatic rebuild: {}", *self, error);
    }
  }
  while (rebuild) {
    auto const generation = rebuild->generation;
    schedule_rebuild(now);
    if (not rebuild or rebuild->generation == generation) {
      break;
    }
  }
  arm_maintenance_wakeup(now);
  deciding_maintenance = false;
}

void catalog_state::arm_maintenance_wakeup(time now) {
  auto const automatic_enabled
    = maintenance.automatic_rebuild > 0
      and maintenance.rebuild_interval > duration::zero();
  auto const policy_interval
    = policy ? policy->maintenance_interval() : duration::zero();
  auto const space_enabled
    = maintenance.space.high_water_mark > 0
      and maintenance.space.scan_interval > std::chrono::seconds::zero();
  // One wakeup covers collection, policy eligibility, reconciliation, and
  // deferred disposal. State transitions otherwise call us inline.
  next_disposal_check = time::max();
  for (auto const& [id, entry] : deferred) {
    if (entry.retry_at) {
      next_disposal_check = std::min(next_disposal_check, *entry.retry_at);
    } else if (entry.deadline) {
      next_disposal_check = std::min(next_disposal_check, *entry.deadline);
    }
  }
  auto deadline = next_disposal_check;
  for (auto& [id, transform] : active_transformations) {
    if (transform.stall_reported) {
      continue;
    }
    auto const warn_at = transform.started_at + std::chrono::minutes{1};
    if (now < warn_at) {
      deadline = std::min(deadline, warn_at);
      continue;
    }
    transform.stall_reported = true;
    auto const phase
      = transform.progress->phase.load(std::memory_order_relaxed);
    TENZIR_WARN("{} transform {} has been in flight for {}: {}",
                transform.origin, id, data{now - transform.started_at},
                data{active_transformations_status()});
    transform.progress->report_next_phase_transition_from(phase);
  }
  if (automatic_enabled) {
    deadline = std::min(deadline, next_collection);
  }
  if (policy_interval > duration::zero()) {
    deadline = std::min(deadline, next_policy_check);
  }
  if (space_enabled) {
    deadline = std::min(deadline, next_space_scan);
  }
  if (space_enabled and eviction_retry_at > now) {
    deadline = std::min(deadline, eviction_retry_at);
  }
  if (deadline == time::max()) {
    maintenance_wakeup.dispose();
    wakeup_at = None{};
    return;
  }
  if (not wakeup_at or *wakeup_at != deadline) {
    maintenance_wakeup.dispose();
    wakeup_at = deadline;
    maintenance_wakeup = detail::weak_run_delayed(
      self, std::max(duration::zero(), deadline - now), [this, deadline] {
        if (not wakeup_at or *wakeup_at != deadline) {
          return;
        }
        wakeup_at = None{};
        advance_maintenance(time::clock::now());
      });
  }
}

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
  if (auto it = admissions.find(partition);
      it != admissions.end() and it->second > run.horizon) {
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

auto catalog_state::select_rebuild_batch(rebuild_run& run, time now)
  -> std::vector<partition_info> {
  run.deferred = 0;
  if (run.selected >= run.options.max_partitions) {
    return {};
  }
  const auto budget = rebuild_byte_budget(maintenance.rebuild_memory_budget,
                                          run.options.parallel);
  run.batch_byte_budget = budget;
  if (budget == 0) {
    TENZIR_WARN("{} has no memory budget for a rebuild batch", name);
    return {};
  }
  // Restrict the pool to the run's expression. A trivially true expression --
  // what the automatic source uses -- would select everything anyway, so we
  // skip the lookup rather than walking every synopsis for it.
  auto matching = Option<std::unordered_set<uuid>>{None{}};
  if (not run.groups
      and run.options.expression != trivially_true_expression()) {
    auto candidates = lookup(run.options.expression);
    if (not candidates) {
      // An invalid filter must fail the run, not masquerade as "matched
      // nothing": the caller typed it and can fix it.
      TENZIR_WARN("{} failed to resolve the rebuild expression {}: {}", name,
                  run.options.expression, candidates.error());
      run.stopping = true;
      if (not run.failure) {
        run.failure = candidates.error();
      }
      return {};
    }
    matching.emplace();
    for (const auto& [schema, info] : candidates->candidate_infos) {
      for (const auto& partition : info.partition_infos) {
        matching->insert(partition.uuid);
      }
    }
  }
  if (not run.groups) {
    auto eligible
      = std::unordered_map<type,
                           std::map<int64_t, std::vector<partition_info>>>{};
    for (auto const& [schema, partitions] : *synopses_per_type) {
      for (auto const& [id, synopsis] : *partitions) {
        if (run.collected_groups
            and not run.collected_groups->contains(
              {schema, rebuild_day(synopsis->max_import_time)})) {
          continue;
        }
        if ((matching and not matching->contains(id))
            or not is_rebuild_candidate(id, *synopsis, run)) {
          continue;
        }
        auto& info = eligible[schema][rebuild_day(synopsis->max_import_time)]
                       .emplace_back(id, *synopsis);
        learn_size_estimate(run, info);
      }
    }
    auto schemas = std::vector<type>{};
    for (auto const& [schema, days] : eligible) {
      schemas.push_back(schema);
    }
    std::ranges::sort(schemas, [](type const& lhs, type const& rhs) {
      if (auto order = lhs.name() <=> rhs.name(); order != 0) {
        return order < 0;
      }
      return lhs.make_fingerprint() < rhs.make_fingerprint();
    });
    run.groups.emplace();
    for (auto const& schema : schemas) {
      for (auto& [day, partitions] : eligible[schema]) {
        std::ranges::sort(partitions, {}, &partition_info::max_import_time);
        run.groups->emplace_back(std::make_move_iterator(partitions.begin()),
                                 std::make_move_iterator(partitions.end()));
      }
    }
  }
  auto const remaining = run.options.max_partitions - run.selected;
  for (auto& group : *run.groups) {
    auto batch = std::vector<partition_info>{};
    auto events = size_t{0};
    auto bytes = uint64_t{0};
    auto closed_day = false;
    for (auto it = group.begin(); it != group.end();) {
      auto current = it++;
      auto const& partition = *current;
      auto synopsis = find_synopsis(partition.uuid);
      if (not synopsis
          or not is_rebuild_candidate(partition.uuid, *synopsis, run)) {
        group.erase(current);
        continue;
      }
      if (in_transformation.contains(partition.uuid)
          or retiring.contains(partition.uuid)
          or blocks_rebuild(partition.uuid, *synopsis, now)) {
        ++run.deferred;
        continue;
      }
      if (quarantined_partitions.contains(partition.uuid)) {
        group.erase(current);
        continue;
      }
      auto const estimate = estimate_approx_bytes(run, partition, budget);
      closed_day
        = rebuild_day(partition.max_import_time) < rebuild_day(run.started_at);
      if (not batch.empty()
          and (detail::saturating_add(bytes, estimate) > budget
               or (not closed_day
                   and detail::saturating_add(events, partition.events)
                         > partition_capacity))) {
        // ponytail: Greedy packing can miss fitting pairs. Use a Pareto
        // frontier only if measured fragmentation warrants the complexity.
        if (batch.size() == 1
            and not is_worth_rebuilding_alone(run, batch.front())
            and estimate <= bytes and partition.events <= events) {
          batch.front() = partition;
          bytes = estimate;
          events = partition.events;
        }
        continue;
      }
      batch.push_back(partition);
      bytes = detail::saturating_add(bytes, estimate);
      events = detail::saturating_add(events, partition.events);
      if ((not closed_day and events >= partition_capacity)
          or batch.size() >= remaining) {
        break;
      }
    }
    auto const required
      = std::ranges::any_of(batch, [&](auto const& partition) {
          return is_worth_rebuilding_alone(run, partition);
        });
    auto const outputs
      = events / partition_capacity
        + static_cast<size_t>(events % partition_capacity != 0);
    if (required
        or satisfies_partition_reduction(
          batch.size(), outputs, 1,
          closed_day ? maintenance.rebuild_merge_margin : 0.0)) {
      return batch;
    }
  }
  return {};
}

auto catalog_state::begin_rebuild(rebuild_options options) -> caf::error {
  if (options.parallel == 0) {
    return caf::make_error(ec::invalid_configuration,
                           "rebuild requires a non-zero parallel level");
  }
  // The automatic source never displaces a run that is already going or
  // queued; it just tries again on its next pass.
  if (options.automatic and (rebuild or pending_rebuild)) {
    return {};
  }
  if ((rebuild and not rebuild->options.automatic) or pending_rebuild) {
    return caf::make_error(
      ec::invalid_argument,
      fmt::format("{} refuses to start a rebuild while one is still ongoing "
                  "({} partitions transformed); consider 'tenzir-ctl rebuild "
                  "stop'",
                  *self, rebuild ? rebuild->transformed : 0));
  }
  // A manual run supersedes the automatic one -- but only once its in-flight
  // batches land. Discarding them would leave their inputs claimed by
  // transforms this run cannot select, so a `--all` could report success
  // without having covered them; and their landing must settle the automatic
  // run's accounting, not this one's.
  if (rebuild) {
    if (rebuild->running > 0) {
      TENZIR_VERBOSE("{} queues a manual rebuild behind the automatic run's "
                     "{} in-flight batch(es)",
                     *self, rebuild->running);
      rebuild->stopping = true;
      pending_rebuild.emplace(std::move(options));
      return {};
    }
    TENZIR_VERBOSE("{} stops the automatic rebuild for a manual one", *self);
    finish_rebuild();
  }
  // Number every run, so a superseded run's continuations can tell that the
  // accounting they are about to touch is no longer theirs.
  rebuild.emplace();
  rebuild->generation = last_rebuild ? last_rebuild->generation + 1 : 1;
  rebuild->options = std::move(options);
  // Automatic runs use the closed hour; manual runs include the open hour.
  rebuild->horizon
    = rebuild->options.automatic ? closed_admission : admission_sequence;
  if (rebuild->options.automatic) {
    rebuild->collected_groups = std::exchange(closed_rebuild_groups, {});
  }
  TENZIR_DEBUG("{} starts a{} rebuild of {}{} partitions with {} thread(s)",
               *self, rebuild->options.automatic ? "n automatic" : " manual",
               rebuild->options.all ? "all" : "outdated",
               rebuild->options.undersized ? " and undersized" : "",
               rebuild->options.parallel);
  return {};
}

auto catalog_state::start_rebuild(rebuild_options options)
  -> caf::result<void> {
  const auto detached = options.detached;
  if (auto error = begin_rebuild(std::move(options)); error.valid()) {
    return error;
  }
  if (detached) {
    advance_maintenance(time::clock::now());
    return {};
  }
  // The run may be queued behind the automatic run's in-flight batches; the
  // caller then waits for the queued run, which adopts these promises when it
  // starts.
  if (pending_rebuild) {
    auto rp = self->make_response_promise<void>();
    pending_rebuild_waiters.push_back(rp);
    return rp;
  }
  TENZIR_ASSERT(rebuild);
  auto rp = self->make_response_promise<void>();
  rebuild->stop_requests.push_back(rp);
  advance_maintenance(time::clock::now());
  return rp;
}

auto catalog_state::stop_rebuild(const rebuild_stop_options& options)
  -> caf::result<void> {
  // A queued manual run is stopped before it starts; its waiters get the same
  // plain completion a stopped running rebuild's waiters get.
  if (pending_rebuild) {
    pending_rebuild = None{};
    for (auto& rp : std::exchange(pending_rebuild_waiters, {})) {
      rp.deliver();
    }
  }
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
  const auto failure = run.failure;
  // Only the statistics and options are ever read back; the run's bookkeeping
  // grows with the number of partitions it touched, so it does not stay
  // resident for the lifetime of the node.
  run.visited = {};
  run.groups = None{};
  run.collected_groups = None{};
  run.approx_bytes_per_event = {};
  last_rebuild = std::move(run);
  for (auto& rp : stop_requests) {
    // A run that aborted reports why, so that `tenzir-ctl rebuild` and
    // anything scripted on top of it can tell a skipped rebuild from a done
    // one.
    if (failure) {
      rp.deliver(*failure);
    } else {
      rp.deliver();
    }
  }
  // A manual run queued behind this one starts now, and its waiters become
  // its stop requests.
  if (pending_rebuild) {
    auto options = *std::exchange(pending_rebuild, None{});
    auto waiters = std::exchange(pending_rebuild_waiters, {});
    if (auto error = begin_rebuild(std::move(options)); error.valid()) {
      for (auto& rp : waiters) {
        rp.deliver(error);
      }
      return;
    }
    TENZIR_ASSERT(rebuild);
    for (auto& rp : waiters) {
      rebuild->stop_requests.push_back(std::move(rp));
    }
  }
}

void catalog_state::schedule_rebuild(time now) {
  TENZIR_ASSERT(rebuild);
  while (not rebuild->stopping
         and rebuild->running < rebuild->options.parallel) {
    auto batch = select_rebuild_batch(*rebuild, now);
    if (batch.empty()) {
      break;
    }
    auto batch_ids = std::vector<uuid>{};
    batch_ids.reserve(batch.size());
    for (const auto& partition : batch) {
      rebuild->visited.insert(partition.uuid);
      batch_ids.push_back(partition.uuid);
    }
    const auto size = batch.size();
    rebuild->selected += size;
    ++rebuild->running;
    rebuild->running_partitions += size;
    auto pipeline = rebuild_pipeline(batch.front().schema, desired_batch_size);
    auto options = TransformOptions{
      .minimum_partition_reduction = 1,
      .minimum_reduction_ratio = rebuild_day(batch.front().max_import_time)
                                     < rebuild_day(rebuild->started_at)
                                   ? maintenance.rebuild_merge_margin
                                   : 0.0,
      .input_byte_budget = rebuild->batch_byte_budget,
      .rebuild_batch_size = desired_batch_size,
    };
    for (auto const& partition : batch) {
      if (is_worth_rebuilding_alone(*rebuild, partition)) {
        options.required_inputs.push_back(partition.uuid);
      }
    }
    // Claim and dispatch in the same actor turn as selection.
    transform(
      std::move(pipeline), std::move(batch), keep_original_partition::no,
      std::string{"rebuild"}, std::string{},
      [this, size, batch_ids,
       generation = rebuild->generation](partition_apply_result& result) {
        if (not rebuild or rebuild->generation != generation) {
          // A later run replaced ours. Its accounting is not ours to settle:
          // decrementing its `running` would wrap the counter and wedge it.
          return;
        }
        --rebuild->running;
        rebuild->running_partitions -= size;
        if (result.skipped) {
          // The loader may accept too few inputs to meet the reduction
          // constraint. Keep them visited for this run instead of retrying
          // the same insufficient batch on every completion.
          return;
        }
        // A transformer may stop at its memory budget. Return untouched
        // inputs and their allowance to this run.
        if (result.input_partitions.size() < batch_ids.size()) {
          rebuild->groups = None{};
          auto consumed = std::unordered_set<uuid>{};
          for (const auto& partition : result.input_partitions) {
            consumed.insert(partition.uuid);
          }
          for (const auto& id : batch_ids) {
            if (not consumed.contains(id)) {
              rebuild->visited.erase(id);
              --rebuild->selected;
            }
          }
        }
        for (const auto& partition : result.output_partitions) {
          learn_size_estimate(*rebuild, partition);
          // The output is a fresh partition that may well match the run's
          // filter again. Marking it visited is what bounds the run.
          rebuild->visited.insert(partition.uuid);
        }
        rebuild->transformed += result.input_partitions.size();
        rebuild->results += result.output_partitions.size();
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
        advance_maintenance(time::clock::now());
      },
      [this, size, batch_ids,
       generation = rebuild->generation](caf::error& error) {
        if (not rebuild or rebuild->generation != generation) {
          return;
        }
        --rebuild->running;
        rebuild->running_partitions -= size;
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
          // The quarantine itself can fail -- a refused retirement, a
          // failed marker write. The partition then stays active and a
          // later selection may hit it again, which retries the quarantine;
          // what must not happen is counting it as handled in silence. The
          // request counts as running work: finishing the run before the
          // marker is durable could hand the caller a success whose
          // quarantined tally the still-pending operation later disproves.
          ++rebuild->running;
          self
            ->mail(atom::erase_v, atom::extract_v, *corrupt,
                   fmt::to_string(error))
            .request(caf::actor_cast<catalog_actor>(self), caf::infinite)
            .then(
              [this, generation](atom::done) {
                // The marker is durable and the file operations are on
                // their way; nothing further to track here.
                if (rebuild and rebuild->generation == generation) {
                  --rebuild->running;
                  advance_maintenance(time::clock::now());
                }
              },
              [this, corrupt = *corrupt,
               generation](caf::error& quarantine_error) {
                TENZIR_WARN("{} failed to quarantine partition {}: {}", *self,
                            corrupt, quarantine_error);
                quarantined_partitions.erase(corrupt);
                if (rebuild and rebuild->generation == generation) {
                  --rebuild->quarantined;
                  --rebuild->running;
                  // Retrying would loop: selection would pick the corrupt
                  // partition again, fail the same way, and land back
                  // here. Wind the run down and report why -- the
                  // partition stays active and the next run retries the
                  // quarantine.
                  rebuild->stopping = true;
                  if (not rebuild->failure) {
                    rebuild->failure = quarantine_error;
                  }
                  advance_maintenance(time::clock::now());
                }
              });
          // The rest of the batch is unchanged in the catalog and only
          // failed by association. Un-visiting it -- allowance included, or
          // a batch that exhausted `max_partitions` could never retry --
          // hands it back to selection, so one corrupt store does not
          // exempt its batch mates from the run.
          rebuild->groups = None{};
          for (const auto& id : batch_ids) {
            if (id != *corrupt) {
              rebuild->visited.erase(id);
              --rebuild->selected;
            }
          }
        } else {
          // Anything else -- a failed write, a failed rename -- is not
          // progress, and retrying it blind would likely fail the same way.
          // Wind the run down and report the failure to whoever waits on
          // it, rather than delivering a success that silently skipped
          // these partitions.
          TENZIR_WARN("{} stops the rebuild after failing to rebuild {} "
                      "partitions: {}",
                      *self, size, error);
          rebuild->stopping = true;
          if (not rebuild->failure) {
            rebuild->failure = error;
          }
        }
        advance_maintenance(time::clock::now());
      },
      std::move(options));
  }
  if (rebuild->running == 0) {
    if (rebuild->options.automatic and rebuild->deferred > 0
        and not rebuild->stopping) {
      return;
    }
    if (rebuild->deferred > 0 and not rebuild->options.automatic
        and not rebuild->failure) {
      rebuild->failure = caf::make_error(
        ec::busy,
        fmt::format("rebuild deferred {} partition(s) to storage "
                    "policy or existing maintenance; retry after it finishes",
                    rebuild->deferred));
    }
    finish_rebuild();
  }
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
       {"deferred", run.deferred},
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
  if (rebuild_zone) {
    result["timezone"] = std::string{rebuild_zone->name()};
  }
  if (maintenance_ready) {
    result["next-collection"] = next_collection;
  }
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

// -- disk budget decisions ----------------------------------------------------

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

auto catalog_state::deleting_bytes() const -> uint64_t {
  auto result = uint64_t{0};
  for (const auto& [partition, footprint] : deleting) {
    result += footprint;
  }
  return result;
}

auto catalog_state::eviction_weight_of(const uuid& partition,
                                       const partition_synopsis& synopsis,
                                       time now) const -> double {
  const auto age
    = std::chrono::duration<double>{now - synopsis.max_import_time}.count();
  if (policy) {
    if (const auto weighted
        = policy->eviction_weight(partition, synopsis, now)) {
      return *weighted;
    }
  }
  // No policy, or none for this partition: age alone. A policy expresses its
  // weights as scaled age rather than on a scale of its own, so this mixes
  // with weighted answers -- and it is what an unweighted schema gets from
  // compaction, whose default weight of 1 leaves the age untouched. Ordering
  // descending is oldest-first, the built-in behavior.
  return age;
}

auto catalog_state::select_eviction_batch(
  size_t limit, const std::unordered_set<uuid>& excluded, time now) const
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
  for (const auto& [schema, partitions] : *synopses_per_type) {
    for (const auto& [partition, synopsis] : *partitions) {
      if (in_transformation.contains(partition)) {
        // Erasing an input would let its data resurrect through the
        // transform's output. The next pass picks it up once it is free.
        continue;
      }
      if (retiring.contains(partition)) {
        // Already on its way out; its retirement has not settled yet.
        continue;
      }
      if (excluded.contains(partition)) {
        continue;
      }
      candidates.emplace_back(eviction_weight_of(partition, *synopsis, now),
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
  auto const generation = storage_generation;
  auto const stable
    = active_transformers.empty() and deleting.empty() and retiring.empty();
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
      [this, worker, generation, stable](uint64_t size) {
        self->send_exit(worker, caf::exit_reason::user_shutdown);
        measuring_space = false;
        if (stable and generation == storage_generation
            and active_transformers.empty() and deleting.empty()
            and retiring.empty()) {
          on_space_measured(size);
        } else {
          // A directory walk is not a snapshot. Retain the last reconciled
          // overhead instead of interpreting partially moved files as growth.
          advance_maintenance(time::clock::now());
        }
      },
      [this, worker](caf::error& error) {
        self->send_exit(worker, caf::exit_reason::user_shutdown);
        measuring_space = false;
        // Keep the last reconciled overhead; live resource accounting still
        // advances independently of this failed directory walk.
        TENZIR_WARN("{} failed to measure the size of {}: {}", *self,
                    paths.database_dir, error);
        advance_maintenance(time::clock::now());
      });
}

void catalog_state::on_space_measured(uint64_t size) {
  auto const known = catalog_bytes + parked_bytes() + deleting_bytes();
  external_bytes = size - std::min(size, known);
  dbdir_size = size;
  space_reconciled = true;
  advance_maintenance(time::clock::now());
}

void catalog_state::enforce_disk_budget(time now) {
  eviction_pending.clear();
  if (maintenance.space.high_water_mark == 0
      or maintenance.space.scan_interval <= std::chrono::seconds::zero()
      or not space_reconciled or now < eviction_retry_at) {
    return;
  }
  // Only live data and non-partition bytes need further reclamation. Parked
  // and deleting files are still physical usage, but already spoken for.
  auto const live = detail::saturating_add(catalog_bytes, external_bytes);
  dbdir_size = detail::saturating_add(
    live, detail::saturating_add(parked_bytes(), deleting_bytes()));
  auto promised = uint64_t{0};
  for (auto const& id : retiring) {
    if (auto synopsis = find_synopsis(id)) {
      promised += synopsis->store_file.size + synopsis->indexes_file.size
                  + synopsis->sketches_file.size;
    }
  }
  auto const effective = live - std::min(live, promised);
  auto const threshold = evicting ? maintenance.space.low_water_mark
                                  : maintenance.space.high_water_mark;
  if (effective <= threshold) {
    evicting = false;
    return;
  }
  evicting = true;
  auto const outstanding = retiring.size() + deleting.size() + eviction_running;
  if (outstanding >= maintenance.space.step_size) {
    return;
  }
  auto planned = uint64_t{0};
  auto slots = maintenance.space.step_size - outstanding;
  // Order once per decision, including protected victims. Repeated top-k
  // scans would turn a long protected prefix into quadratic work.
  for (auto const& id :
       select_eviction_batch(std::numeric_limits<size_t>::max(), {}, now)) {
    if (slots == 0
        or effective - std::min(effective, planned)
             <= maintenance.space.low_water_mark) {
      break;
    }
    auto const synopsis = find_synopsis(id);
    if (not synopsis) {
      continue;
    }
    auto const outcome = run_eviction_action(id);
    if (outcome != eviction_outcome::none) {
      eviction_pending.insert(id);
      if (outcome == eviction_outcome::started) {
        --slots;
      }
      // A rewrite's reclamation is unknown. Do not credit its entire input.
      continue;
    }
    planned += synopsis->store_file.size + synopsis->indexes_file.size
               + synopsis->sketches_file.size;
    retiring.insert(id);
    --slots;
    self->mail(atom::erase_v, id)
      .request(caf::actor_cast<catalog_actor>(self), caf::infinite)
      .then(
        [this, id](atom::done) {
          retiring.erase(id);
          ++evicted;
          advance_maintenance(time::clock::now());
        },
        [this, id](caf::error& error) {
          retiring.erase(id);
          eviction_retry_at
            = time::clock::now() + maintenance.space.scan_interval;
          TENZIR_WARN("{} failed to evict partition {}: {}", *self, id, error);
          advance_maintenance(time::clock::now());
        });
  }
}

void catalog_state::release_compaction_slot() {
  TENZIR_ASSERT(compacting > 0);
  // The shared selector, not the completing source, assigns the freed slot.
  --compacting;
  advance_maintenance(time::clock::now());
}

void catalog_state::schedule_compaction(time now) {
  if (not policy or policy->maintenance_interval() <= duration::zero()) {
    return;
  }
  // Classify every changed input before considering rebuild, even with no
  // compaction capacity. A busy pool never turns a due policy into no policy.
  for (auto const& id : std::exchange(policy_dirty, {})) {
    auto synopsis = find_synopsis(id);
    if (synopsis) {
      next_policy_check = std::min(
        next_policy_check, policy->maintenance_deadline(id, *synopsis, now));
    }
    if (synopsis and policy->blocks_rebuild(id, *synopsis, now)) {
      policy_pending.insert(id);
    } else {
      policy_pending.erase(id);
    }
  }
  if (compacting >= maintenance.compaction_slots) {
    return;
  }
  auto candidates
    = std::vector<uuid>{policy_pending.begin(), policy_pending.end()};
  for (auto const& id : candidates) {
    if (compacting >= maintenance.compaction_slots) {
      break;
    }
    if (in_transformation.contains(id) or retiring.contains(id)) {
      continue;
    }
    auto synopsis = find_synopsis(id);
    auto action = synopsis ? policy->maintenance_action(id, *synopsis, now)
                           : Option<storage_action>{None{}};
    if (not action) {
      policy_pending.erase(id);
      continue;
    }
    run_policy_action(id, std::move(*action));
  }
}

void catalog_state::run_policy_action(const uuid& partition,
                                      storage_action action, bool eviction) {
  auto synopsis = find_synopsis(partition);
  if (not synopsis) {
    return;
  }
  auto batch = std::vector<partition_info>{};
  batch.emplace_back(partition, *synopsis);
  auto const input_bytes = synopsis->store_file.size
                           + synopsis->indexes_file.size
                           + synopsis->sketches_file.size;
  ++compacting;
  if (eviction) {
    ++eviction_running;
  }
  transform(
    std::move(action.pipeline), std::move(batch), action.keep, action.origin,
    policy ? policy->serialize_token(action.token) : std::string{},
    [this, partition, eviction, input_bytes, keep = action.keep,
     recorded = action.token](partition_apply_result& result) {
      if (eviction) {
        --eviction_running;
      }
      if (result.input_partitions.empty()) {
        // Nothing was consumed, so there is no policy result to commit.
        release_compaction_slot();
        return;
      }
      if (eviction) {
        ++evicted;
        record_eviction_result(partition, input_bytes, keep, result);
      } else {
        ++compacted;
      }
      if (policy) {
        policy->on_committed(recorded, partition, result.output_partitions);
      }
      if (policy and not result.marker.empty()) {
        // The commit above lives only in policy memory plus this marker;
        // the marker may go only once the flush *succeeds*. Per-commit
        // flushes coalesce -- a flush during an in-flight write joins it.
        release_marker_after_flush(std::filesystem::path{result.marker});
      }
      // A committed action can expose follow-up work on the same data: the
      // replacement may be eligible for the next rule in line, and waiting
      // for the next interval would delay every subsequent rule by a full
      // period. Have the released slot walk again; a walk that starts
      // nothing ends the cycle.
      release_compaction_slot();
    },
    [this, partition, eviction, recorded = action.token](caf::error& error) {
      if (eviction) {
        --eviction_running;
      }
      TENZIR_WARN("{} failed to run a maintenance action on partition {}: "
                  "{}",
                  *self, partition, error);
      if (policy) {
        policy->on_failed(recorded, partition, error);
      }
      release_compaction_slot();
    });
}

auto catalog_state::run_named_rule(std::string rule,
                                   Option<duration> older_than,
                                   Option<duration> newer_than)
  -> caf::result<atom::done> {
  if (not policy) {
    return caf::make_error(ec::invalid_configuration,
                           "no storage policy is configured");
  }
  if (named_run) {
    return caf::make_error(ec::busy, "a compaction rule is already running");
  }
  // A rule with nothing to do and a misspelled rule both find no work, so ask
  // the policy which of the two this is rather than reporting success for a
  // typo. A policy that does not name its rules gets the benefit of the doubt.
  if (const auto known = policy->rule_names(); not known.empty()) {
    if (std::ranges::find(known, rule) == known.end()) {
      return caf::make_error(ec::invalid_argument,
                             fmt::format("no storage policy rule named '{}'; "
                                         "known rules: {}",
                                         rule, fmt::join(known, ", ")));
    }
  }
  // Only the policy can validate the effective window: the overrides merge
  // with the rule's configured bounds, which the catalog never sees. An
  // impossible window would otherwise rewrite matching partitions through an
  // always-false predicate, advance their watermarks, and report success.
  if (auto error = policy->check_named_run(rule, older_than, newer_than);
      error.valid()) {
    return error;
  }
  // Collect the partitions first: starting transforms while walking would
  // mutate the very maps the walk is iterating.
  auto pending = std::vector<uuid>{};
  auto skipped = size_t{0};
  auto const now = time::clock::now();
  for (const auto& [schema, partitions] : *synopses_per_type) {
    for (const auto& [partition, synopsis] : *partitions) {
      if (not policy->named_action(rule, older_than, newer_than, partition,
                                   *synopsis, now)) {
        continue;
      }
      if (in_transformation.contains(partition)) {
        // Another transform holds this partition, and its replacement gets an
        // id this run will never see. Count it so the run can say that it did
        // not cover everything, instead of a `done` that quietly excludes it.
        ++skipped;
        continue;
      }
      pending.push_back(partition);
    }
  }
  if (pending.empty() and skipped == 0) {
    // Nothing matched, which is a legitimate outcome rather than an error: the
    // rule may simply have nothing left to do.
    TENZIR_VERBOSE("{} found no partition for compaction rule {}", *self, rule);
    return atom::done_v;
  }
  TENZIR_INFO("{} runs compaction rule {} over {} partition(s)", *self, rule,
              pending.size());
  auto rp = self->make_response_promise<atom::done>();
  named_run.emplace(named_rule_run{
    .rule = std::move(rule),
    .older_than = older_than,
    .newer_than = newer_than,
    .pending = std::move(pending),
    .skipped = skipped,
    .promise = rp,
  });
  advance_maintenance(now);
  return rp;
}

void catalog_state::drain_named_rule(time now) {
  if (not named_run) {
    return;
  }
  // One partition per free slot. A rule can match every partition in the
  // database, and firing them all at once would put a transformer on each --
  // every one of them budgeting a quarter of available memory for itself.
  while (not named_run->pending.empty()
         and compacting < maintenance.compaction_slots) {
    const auto partition = named_run->pending.back();
    named_run->pending.pop_back();
    auto synopsis = find_synopsis(partition);
    if (not synopsis) {
      // Erased while the run was queued; the data is gone either way.
      continue;
    }
    if (in_transformation.contains(partition) or retiring.contains(partition)) {
      // Claimed while the run was queued; its replacement carries an id this
      // run never saw, so it counts as not covered.
      ++named_run->skipped;
      continue;
    }
    // Ask again rather than replaying a decision made when the run started:
    // the history may have moved on in the meantime.
    auto action
      = policy->named_action(named_run->rule, named_run->older_than,
                             named_run->newer_than, partition, *synopsis, now);
    if (not action) {
      continue;
    }
    auto batch = std::vector<partition_info>{};
    batch.emplace_back(partition, *synopsis);
    ++compacting;
    ++named_run->running;
    transform(
      std::move(action->pipeline), std::move(batch), action->keep,
      action->origin,
      policy ? policy->serialize_token(action->token) : std::string{},
      [this, partition,
       recorded = action->token](partition_apply_result& result) {
        if (result.input_partitions.empty()) {
          // Do not record untouched input as processed.
          ++named_run->skipped;
          finish_named_rule_batch({});
          return;
        }
        if (policy) {
          policy->on_committed(recorded, partition, result.output_partitions);
          if (not result.marker.empty()) {
            release_marker_after_flush(std::filesystem::path{result.marker});
          }
        }
        finish_named_rule_batch({});
      },
      [this, partition, recorded = action->token](caf::error& error) {
        TENZIR_WARN("{} failed to run a compaction rule on partition {}: {}",
                    *self, partition, error);
        if (policy) {
          policy->on_failed(recorded, partition, error);
        }
        finish_named_rule_batch(std::move(error));
      });
  }
  if (named_run->running == 0 and named_run->pending.empty()) {
    // Every candidate went away before its turn came.
    finish_named_run();
  }
}

void catalog_state::finish_named_run() {
  auto run = *std::exchange(named_run, None{});
  auto deliver = [](named_rule_run run, caf::error flush_error) {
    if (run.failure) {
      run.promise.deliver(std::move(run.failure));
      return;
    }
    if (flush_error) {
      // The transforms landed, but the record of them did not: answering
      // `done` would promise durability the disk refused. The write is
      // retried in the background, so a re-run after recovery is safe.
      run.promise.deliver(caf::make_error(
        ec::filesystem_error,
        fmt::format("compaction rule {} ran, but persisting its history "
                    "failed and is being retried: {}",
                    run.rule, flush_error)));
      return;
    }
    if (run.skipped > 0) {
      // Partitions the run could not reach were replaced under new ids, so a
      // plain `done` would overstate what the rule covered. Saying so makes
      // the command retryable: the next run sees the replacements.
      run.promise.deliver(caf::make_error(
        ec::busy, fmt::format("compaction rule {} skipped {} partition(s) "
                              "that another transform held; run it again to "
                              "cover their replacements",
                              run.rule, run.skipped)));
      return;
    }
    run.promise.deliver(atom::done_v);
  };
  if (not policy) {
    deliver(std::move(run), {});
    return;
  }
  // The caller is about to hear that the run happened; the state backing that
  // answer -- the watermarks its transforms recorded -- must be durable
  // first, not sitting in a debounce window that a shutdown would discard.
  deliver(std::move(run), policy->flush());
}

void catalog_state::finish_named_rule_batch(caf::error error) {
  if (not named_run) {
    release_compaction_slot();
    return;
  }
  --named_run->running;
  // Report the first failure, but let the rest of the run finish rather than
  // abandoning partitions the operator asked to process.
  if (error and not named_run->failure) {
    named_run->failure = std::move(error);
  }
  if (named_run->pending.empty() and named_run->running == 0) {
    finish_named_run();
  }
  // Last, so that the run's own accounting is settled before the freed slot
  // is offered to whatever is queued.
  release_compaction_slot();
}

void catalog_state::record_eviction_result(
  uuid const& input, uint64_t input_bytes, keep_original_partition keep,
  partition_apply_result const& result) {
  auto output_bytes = uint64_t{0};
  for (auto const& output : result.output_partitions) {
    if (auto synopsis = find_synopsis(output.uuid)) {
      output_bytes += synopsis->store_file.size + synopsis->indexes_file.size
                      + synopsis->sketches_file.size;
    }
  }
  if (keep == keep_original_partition::no and output_bytes < input_bytes) {
    return;
  }
  if (keep == keep_original_partition::yes) {
    eviction_suppressed.insert(input);
  }
  for (auto const& output : result.output_partitions) {
    eviction_suppressed.insert(output.uuid);
  }
}

auto catalog_state::run_eviction_action(const uuid& partition)
  -> eviction_outcome {
  if (eviction_suppressed.contains(partition)) {
    return eviction_outcome::deferred;
  }
  if (not policy) {
    return eviction_outcome::none;
  }
  auto synopsis = find_synopsis(partition);
  if (not synopsis) {
    return eviction_outcome::none;
  }
  auto answer = policy->eviction_action(partition, *synopsis);
  if (const auto* fallback
      = std::get_if<storage_policy::eviction_fallback>(&answer)) {
    switch (*fallback) {
      case storage_policy::eviction_fallback::erase:
        // Erase outright, which is what an unconfigured eviction has always
        // done.
        return eviction_outcome::none;
      case storage_policy::eviction_fallback::keep:
        // The policy owns this partition but cannot act on it right now, for
        // example because its configured pipeline does not build. Erasing in
        // its stead would delete data the operator asked to transform; the
        // next measurement round asks again.
        return eviction_outcome::deferred;
    }
    TENZIR_UNREACHABLE();
  }
  auto& action = std::get<storage_action>(answer);
  // The policy owns this partition either way: erasing it because the pool is
  // busy would throw away the rewrite it asked for. Leaving it for the next
  // measurement round is the only correct answer.
  if (compacting >= maintenance.compaction_slots) {
    TENZIR_DEBUG("{} defers the eviction action for partition {} because the "
                 "compaction pool is full",
                 *self, partition);
    return eviction_outcome::deferred;
  }
  run_policy_action(partition, std::move(action), true);
  return eviction_outcome::started;
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
    {"deleting-bytes", deleting_bytes()},
    {"no-progress-partitions", eviction_suppressed.size()},
    {"high-water-mark", uint64_t{maintenance.space.high_water_mark}},
    {"low-water-mark", uint64_t{maintenance.space.low_water_mark}},
  };
  if (dbdir_size) {
    result["dbdir-size"] = *dbdir_size;
  }
  return result;
}

} // namespace tenzir

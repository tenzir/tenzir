//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/arrow_table_slice.hpp>
#include <vast/concept/parseable/to.hpp>
#include <vast/concept/parseable/vast/expression.hpp>
#include <vast/data.hpp>
#include <vast/detail/inspection_common.hpp>
#include <vast/detail/narrow.hpp>
#include <vast/fwd.hpp>
#include <vast/partition_synopsis.hpp>
#include <vast/pipeline.hpp>
#include <vast/plugin.hpp>
#include <vast/query_context.hpp>
#include <vast/system/catalog.hpp>
#include <vast/system/index.hpp>
#include <vast/system/node.hpp>
#include <vast/system/node_control.hpp>
#include <vast/system/query_cursor.hpp>
#include <vast/system/read_query.hpp>
#include <vast/system/report.hpp>
#include <vast/system/spawn_or_connect_to_node.hpp>
#include <vast/system/status.hpp>
#include <vast/table_slice.hpp>
#include <vast/table_slice_builder.hpp>
#include <vast/uuid.hpp>

#include <arrow/table.h>
#include <caf/expected.hpp>
#include <caf/policy/select_all.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/type_id.hpp>
#include <caf/typed_event_based_actor.hpp>
#include <fmt/format.h>

namespace vast::plugins::rebuild {

namespace {

/// The rebatch pipeline operator takes homogeneous table slices and if and only
/// if necessary recreates the slices with a given desired batch size, with only
/// the last slice potentially being undersized. This operator is intentionally
/// not exposed to the user, as that allows for it to make stricter assumptions
/// about its input, namely that an instance of the operator only takes input
/// of a single schema. Rebatching is guaranteed not to change the order of
/// events, just how they're grouped together.
class rebatch_operator final : public crtp_operator<rebatch_operator> {
public:
  /// Constructs a rebatch pipeline operator with a given schema and desired
  /// batch size.
  /// @param schema The schema of the input and output table slices.
  /// @param desired_batch_size The desired output batch size; guaranteed to be
  /// exactly matched for all but the last output batch.
  rebatch_operator(type schema, size_t desired_batch_size)
    : schema_{std::move(schema)},
      desired_batch_size_{desired_batch_size != 0
                            ? desired_batch_size
                            : defaults::import::table_slice_size} {
    // nop
  }

  auto operator()(generator<table_slice> input) const
    -> generator<table_slice> {
    auto buffer = std::vector<table_slice>{};
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        continue;
      }
      const auto buffered_rows = rows(buffer);
      VAST_ASSERT(buffered_rows < desired_batch_size_);
      // We don't have enough yet.
      if (buffered_rows + slice.rows() < desired_batch_size_) {
        buffer.push_back(std::move(slice));
        co_yield {};
        continue;
      }
      // We've got enough, so we can now concatenate and yield.
      const auto remainder = desired_batch_size_ - buffered_rows;
      VAST_ASSERT(remainder <= slice.rows());
      auto [head, tail] = split(slice, slice.rows() - remainder);
      buffer.push_back(head);
      co_yield concatenate(std::exchange(buffer, {}));
      // In case the input slice was oversized, we may have to yield additional
      // resized batches.
      while (tail.rows() >= desired_batch_size_) {
        std::tie(head, tail) = split(tail, desired_batch_size_);
        co_yield std::move(head);
      }
      // Lastly, keep the undersized remainder for the next input or the end.
      buffer.push_back(std::move(tail));
    }
    if (!buffer.empty()) {
      co_yield concatenate(std::move(buffer));
    }
  }

  auto to_string() const -> std::string override {
    return "<rebatch>";
  }

private:
  type schema_ = {};
  size_t desired_batch_size_ = {};
};

/// The threshold at which to consider a partition undersized, relative to the
/// configured 'vast.max-partition-size'.
inline constexpr auto undersized_threshold = 0.8;

/// The parsed options of the `vast rebuild start` command.
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

/// The parsed options of the `vast rebuild stop` command.
struct stop_options {
  bool detached = false;

  friend auto inspect(auto& f, stop_options& x) {
    return f.apply(x.detached);
  }
};

/// Statistics for an ongoing rebuild. Numbers are partitions.
struct statistics {
  size_t num_total = {};
  size_t num_rebuilding = {};
  size_t num_completed = {};
  size_t num_results = {};
};

/// The state of an in-progress rebuild.
struct run {
  std::vector<partition_info> remaining_partitions = {};
  struct statistics statistics = {};
  start_options options = {};
  std::vector<caf::typed_response_promise<void>> stop_requests = {};
  std::vector<caf::typed_response_promise<void>> delayed_rebuilds = {};
};

/// The interface of the REBUILDER actor.
using rebuilder_actor = system::typed_actor_fwd<
  // Start a rebuild.
  auto(atom::start, start_options)->caf::result<void>,
  // Stop a rebuild.
  auto(atom::stop, stop_options)->caf::result<void>,
  // INTERNAL: Continue working on the currently in-progress rebuild.
  auto(atom::internal, atom::rebuild)->caf::result<void>,
  // INTERNAL: Continue working on the currently in-progress rebuild.
  auto(atom::internal, atom::schedule)->caf::result<void>>
  // Conform to the protocol of the STATUS CLIENT actor.
  ::extend_with<system::component_plugin_actor>::unwrap;

/// The state of the REBUILDER actor.
struct rebuilder_state {
  /// The actor name as shown in logs.
  [[maybe_unused]] static constexpr const char* name = "rebuilder";

  /// The constructor of the state.
  /// NOTE: gcc-11 requires that we have this explicitly defaulted. :shrug: -- DL
  rebuilder_state() = default;

  /// Actor handles required for the rebuilder.
  rebuilder_actor::pointer self = {};
  system::catalog_actor catalog = {};
  system::index_actor index = {};
  system::accountant_actor accountant = {};

  /// Constants read once from the system configuration.
  size_t max_partition_size = 0u;
  size_t desired_batch_size = 0u;
  size_t automatic_rebuild = 0u;
  duration rebuild_interval = {};

  /// The state of the ongoing rebuild.
  std::optional<struct run> run = {};
  bool stopping = false;

  /// Shows the status of a currently ongoing rebuild.
  auto status(system::status_verbosity) -> record {
    if (!run)
      return {};
    return {
      {"partitions",
       record{
         {"total", run->statistics.num_total},
         {"transforming", run->statistics.num_rebuilding},
         {"transformed", run->statistics.num_completed},
         {"remaining",
          run->statistics.num_total - run->statistics.num_completed},
         {"results", run->statistics.num_results},
       }},
      {"options",
       record{
         {"all", run->options.all},
         {"undersized", run->options.undersized},
         {"parallel", run->options.parallel},
         {"max-partitions", run->options.max_partitions},
         {"expression", fmt::to_string(run->options.expression)},
         {"detached", run->options.detached},
         {"automatic", run->options.automatic},
       }},
    };
  }

  /// Start a new rebuild.
  auto start(start_options options) -> caf::result<void> {
    if (options.parallel == 0)
      return caf::make_error(ec::invalid_configuration,
                             "rebuild requires a non-zero parallel level");
    if (options.automatic && run)
      return {};
    if (run && !run->options.automatic)
      return caf::make_error(
        ec::invalid_argument,
        fmt::format("{} refuses to start rebuild while a rebuild is still "
                    "ongoing ({}/{} done); consider running 'vast rebuild "
                    "stop'",
                    *self, run->statistics.num_completed,
                    run->statistics.num_total));
    if (!options.automatic && run && run->options.automatic) {
      auto rp = self->make_response_promise<void>();
      self
        ->request(static_cast<rebuilder_actor>(self), caf::infinite,
                  atom::stop_v, stop_options{.detached = false})
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
    run->options = std::move(options);
    VAST_DEBUG("{} requests {}{} partitions matching the expression {}", *self,
               run->options.all ? "all" : "outdated",
               run->options.undersized ? " undersized" : "",
               run->options.expression);
    auto rp = self->make_response_promise<void>();
    auto finish = [this, rp](caf::error err, bool silent = false) mutable {
      if (!silent) {
        // Only print to INFO when work was actually done, or when the run
        // was manually requested.
        if (run->statistics.num_completed == 0)
          if (run->options.automatic)
            VAST_VERBOSE("{} had nothing to do", *self);
          else
            VAST_INFO("{} had nothing to do", *self);
        else
          VAST_INFO("{} rebuilt {} into {} partitions", *self,
                    run->statistics.num_completed, run->statistics.num_results);
      }
      for (auto&& rp : std::exchange(run->stop_requests, {}))
        rp.deliver();
      run.reset();
      if (run->options.detached)
        return;
      if (err) {
        rp.deliver(std::move(err));
        return;
      }
      rp.deliver();
    };
    if (run->options.detached)
      rp.deliver();
    auto query_context
      = query_context::make_extract("rebuild", self, run->options.expression);
    query_context.id = uuid::random();
    self
      ->request(catalog, caf::infinite, atom::candidates_v,
                std::move(query_context))
      .then(
        [this, finish](system::catalog_lookup_result& lookup_result) mutable {
          VAST_ASSERT(run->statistics.num_total == 0);
          for (auto& [type, result] : lookup_result.candidate_infos) {
            if (not run->options.all) {
              std::erase_if(
                result.partition_infos, [&](const partition_info& partition) {
                  if (partition.version < version::current_partition_version)
                    return false;
                  if (run->options.undersized
                      && partition.events < detail::narrow_cast<size_t>(
                           detail::narrow_cast<double>(max_partition_size)
                           * undersized_threshold))
                    return false;
                  return true;
                });
            }
            if (run->options.max_partitions < result.partition_infos.size()) {
              std::stable_sort(result.partition_infos.begin(),
                               result.partition_infos.end(),
                               [](const auto& lhs, const auto& rhs) {
                                 return lhs.schema < rhs.schema;
                               });
              result.partition_infos.erase(
                result.partition_infos.begin()
                  + detail::narrow_cast<ptrdiff_t>(run->options.max_partitions),
                result.partition_infos.end());
              if (result.partition_infos.size() == 1
                  && result.partition_infos.front().version
                       < version::current_partition_version) {
                // Edge case: we can't do anything if we have a single
                // undersized partition for a given schema.
                result.partition_infos.clear();
              }
            }
            run->statistics.num_total += result.partition_infos.size();
            run->remaining_partitions.insert(run->remaining_partitions.end(),
                                             result.partition_infos.begin(),
                                             result.partition_infos.end());
          }
          if (run->statistics.num_total == 0) {
            VAST_DEBUG("{} ignores rebuild request for 0 partitions", *self);
            return finish({}, true);
          }
          if (run->options.automatic)
            VAST_VERBOSE("{} triggered an automatic run for {} candidate "
                         "partitions with {} threads",
                         *self, run->statistics.num_total,
                         run->options.parallel);
          else
            VAST_INFO("{} triggered a run for {} candidate partitions with {} "
                      "threads",
                      *self, run->statistics.num_total, run->options.parallel);
          self
            ->fan_out_request<caf::policy::select_all>(
              std::vector<rebuilder_actor>(run->options.parallel, self),
              caf::infinite, atom::internal_v, atom::rebuild_v)
            .then(
              [finish]() mutable {
                finish({});
              },
              [finish](caf::error& error) mutable {
                finish(std::move(error));
              });
        },
        [finish](caf::error& error) mutable {
          finish(std::move(error));
        });
    return rp;
  }

  /// Stop a rebuild.
  auto stop(const stop_options& options) -> caf::result<void> {
    if (!run) {
      if (!stopping)
        VAST_DEBUG("{} got request to stop rebuild but no rebuild is running",
                   *self);
      else
        VAST_INFO("{} stopped ongoing rebuild", *self);
      stopping = false;
      return {};
    }
    stopping = true;
    if (!run->remaining_partitions.empty()) {
      VAST_ASSERT(run->remaining_partitions.size()
                  == run->statistics.num_total - run->statistics.num_rebuilding
                       - run->statistics.num_completed);
      VAST_INFO("{} schedules stop after rebuild of {} partitions currently "
                "in rebuilding, and will not touch remaining {} partitions",
                *self, run->statistics.num_rebuilding,
                run->remaining_partitions.size());
      run->statistics.num_total -= run->remaining_partitions.size();
      run->remaining_partitions.clear();
      emit_telemetry();
    }
    if (options.detached)
      return {};
    auto rp = self->make_response_promise<void>();
    return run->stop_requests.emplace_back(std::move(rp));
  }

  /// Make progress on the ongoing rebuild.
  auto rebuild() -> caf::result<void> {
    if (run->remaining_partitions.empty())
      return {}; // We're done!
    auto current_run_partitions = std::vector<partition_info>{};
    auto current_run_events = size_t{0};
    // Take the first partition and collect as many of the same
    // type as possible to create new paritions. The approach used may
    // collects too many partitions if there is no exact match, but that is
    // usually better than conservatively undersizing the number of
    // partitions for the current run. For oversized runs we move the last
    // transformed partition back to the list of remaining partitions if it
    // is less than some percentage of the desired size.
    const auto schema = run->remaining_partitions[0].schema;
    const auto first_removed = std::remove_if(
      run->remaining_partitions.begin(), run->remaining_partitions.end(),
      [&](const partition_info& partition) {
        if (schema == partition.schema
            && current_run_events < max_partition_size) {
          current_run_events += partition.events;
          current_run_partitions.push_back(partition);
          VAST_TRACE("{} selects partition {} (v{}, {}) with "
                     "{} events (total: {})",
                     *self, partition.uuid, partition.version, partition.schema,
                     partition.events, current_run_events);
          return true;
        }
        return false;
      });
    run->remaining_partitions.erase(first_removed,
                                    run->remaining_partitions.end());
    auto is_oversized = current_run_events > max_partition_size;
    run->statistics.num_rebuilding += current_run_partitions.size();
    // If we have just a single partition then we shouldn't rebuild if our
    // intent was to merge undersized partitions, unless the partition is
    // oversized or not of the latest partition version.
    const auto skip_rebuild
      = run->options.undersized && current_run_partitions.size() == 1
        && current_run_partitions[0].version
             == version::current_partition_version
        && current_run_partitions[0].events <= max_partition_size;
    if (skip_rebuild) {
      VAST_DEBUG("{} skips rebuilding of undersized partition {} because no "
                 "other partition of schema {} exists",
                 *self, current_run_partitions[0].uuid,
                 current_run_partitions[0].schema);
      run->statistics.num_rebuilding -= 1;
      run->statistics.num_total -= 1;
      // Pick up new work until we run out of remainig partitions.
      emit_telemetry();
      return self->delegate(static_cast<rebuilder_actor>(self),
                            atom::internal_v, atom::rebuild_v);
    }
    // Ask the index to rebuild the partitions we selected.
    auto rp = self->make_response_promise<void>();
    auto ops = std::vector<operator_ptr>{};
    ops.push_back(std::make_unique<rebatch_operator>(
      current_run_partitions[0].schema,
      detail::narrow_cast<int64_t>(desired_batch_size)));
    emit_telemetry();
    // We sort the selected partitions from old to new so the rebuild transform
    // sees the batches (and events) in the order they arrived. This prevents
    // the rebatching from shuffling events, and rebatching of already correctly
    // sized batches just for the right alignment.
    std::sort(current_run_partitions.begin(), current_run_partitions.end(),
              [](const partition_info& lhs, const partition_info& rhs) {
                return lhs.max_import_time < rhs.max_import_time;
              });
    const auto num_partitions = current_run_partitions.size();
    self
      ->request(index, caf::infinite, atom::apply_v, pipeline{std::move(ops)},
                std::move(current_run_partitions),
                system::keep_original_partition::no)
      .then(
        [this, rp, current_run_events, num_partitions,
         is_oversized](std::vector<partition_info>& result) mutable {
          if (result.empty()) {
            VAST_DEBUG("{} skipped {} partitions as they are already being "
                       "transformed by another actor",
                       *self, num_partitions);
            run->statistics.num_total -= num_partitions;
            run->statistics.num_rebuilding -= num_partitions;
            // Pick up new work until we run out of remaining partitions.
            emit_telemetry();
            rp.delegate(static_cast<rebuilder_actor>(self), atom::internal_v,
                        atom::rebuild_v);
            return;
          }
          VAST_DEBUG("{} rebuilt {} into {} partitions", *self, num_partitions,
                     result.size());
          // Determines whether we moved partitions back.
          bool needs_second_stage = false;
          // If the number of events in the resulting partitions does not
          // match the number of events in the partitions that went in we ran
          // into a conflict with other partition transformations on an
          // overlapping set.
          const auto result_events
            = std::transform_reduce(result.begin(), result.end(), size_t{},
                                    std::plus<>{},
                                    [](const partition_info& partition) {
                                      return partition.events;
                                    });
          if (current_run_events != result_events)
            VAST_WARN("{} detected a mismatch: rebuilt {} events from {} "
                      "partitions into {} events in {} partitions",
                      *self, current_run_events, num_partitions, result_events,
                      result.size());
          // Adjust the counters, update the indicator, and move back
          // undersized transformed partitions to the list of remainig
          // partitions as desired.
          VAST_ASSERT(!result.empty());
          run->statistics.num_completed += num_partitions;
          run->statistics.num_results += result.size();
          if (is_oversized) {
            VAST_ASSERT(result.size() > 1);
            if (result.back().events <= detail::narrow_cast<size_t>(
                  detail::narrow_cast<double>(max_partition_size)
                  * undersized_threshold)) {
              needs_second_stage = true;
              run->remaining_partitions.push_back(std::move(result.back()));
              run->statistics.num_completed -= 1;
              run->statistics.num_results -= 1;
              run->statistics.num_total += 1;
            }
          }
          if (needs_second_stage)
            std::sort(run->remaining_partitions.begin(),
                      run->remaining_partitions.end(),
                      [](const partition_info& lhs, const partition_info& rhs) {
                        return lhs.max_import_time > rhs.max_import_time;
                      });
          run->statistics.num_rebuilding -= num_partitions;
          // Pick up new work until we run out of remainig partitions.
          emit_telemetry();
          rp.delegate(static_cast<rebuilder_actor>(self), atom::internal_v,
                      atom::rebuild_v);
        },
        [this, num_partitions = current_run_partitions.size(),
         rp](caf::error& error) mutable {
          VAST_WARN("{} failed to rebuild partititons: {}", *self, error);
          run->statistics.num_rebuilding -= num_partitions;
          // Pick up new work until we run out of remainig partitions.
          emit_telemetry();
          rp.delegate(static_cast<rebuilder_actor>(self), atom::internal_v,
                      atom::rebuild_v);
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
    self->delayed_send(self, rebuild_interval, atom::internal_v,
                       atom::schedule_v);
    self
      ->request(static_cast<rebuilder_actor>(self), caf::infinite,
                atom::start_v, std::move(options))
      .then(
        [this] {
          VAST_DEBUG("{} finished automatic rebuild", *self);
        },
        [this](const caf::error& err) {
          VAST_WARN("{} failed during automatic rebuild: {}", *self, err);
        });
  }

private:
  /// Send metrics to the accountant for live monitoring.
  void emit_telemetry() {
    if (!accountant)
      return;
    auto report = system::report {
      .data = {
        {"rebuilder.partitions.remaining", run ? run->statistics.num_total - run->statistics.num_completed : 0u},
        {"rebuilder.partitions.rebuilding", run ? run->statistics.num_rebuilding : 0u},
        {"rebuilder.partitions.completed", run ? run->statistics.num_completed : 0u},
      },
      .metadata = {
      },
    };
    self->send(accountant, atom::metrics_v, std::move(report));
  }
};

/// Defines the behavior of the REBUILDER actor.
/// @param self A pointer to this actor.
/// @param catalog A handle to the CATALOG actor.
/// @param index A handle to the INDEX actor.
/// @param accountant A handle to the ACCOUNTANT actor.
rebuilder_actor::behavior_type
rebuilder(rebuilder_actor::stateful_pointer<rebuilder_state> self,
          system::catalog_actor catalog, system::index_actor index,
          system::accountant_actor accountant) {
  self->state.self = self;
  self->state.catalog = std::move(catalog);
  self->state.index = std::move(index);
  self->state.accountant = std::move(accountant);
  self->state.max_partition_size
    = caf::get_or(self->system().config(), "vast.max-partition-size",
                  defaults::system::max_partition_size);
  self->state.desired_batch_size
    = caf::get_or(self->system().config(), "vast.import.batch-size",
                  defaults::import::table_slice_size);
  self->state.automatic_rebuild
    = caf::get_or(self->system().config(), "vast.automatic-rebuild", size_t{1});
  if (self->state.automatic_rebuild > 0) {
    self->state.rebuild_interval
      = caf::get_or(self->system().config(), "vast.active-partition-timeout",
                    defaults::system::active_partition_timeout);
    self->state.schedule();
  }
  self->set_exit_handler([self](const caf::exit_msg& msg) {
    VAST_DEBUG("{} received EXIT from {}: {}", *self, msg.source, msg.reason);
    if (!self->state.run) {
      self->quit(msg.reason);
      return;
    }
    for (auto&& rp : std::exchange(self->state.run->stop_requests, {}))
      rp.deliver(msg.reason);
    for (auto&& rp : std::exchange(self->state.run->delayed_rebuilds, {}))
      rp.deliver(msg.reason);
    self->quit(msg.reason);
  });
  return {
    [self](atom::status, system::status_verbosity verbosity) {
      return self->state.status(verbosity);
    },
    [self](atom::start, start_options& options) {
      return self->state.start(std::move(options));
    },
    [self](atom::stop, const stop_options& options) {
      return self->state.stop(options);
    },
    [self](atom::internal, atom::rebuild) {
      return self->state.rebuild();
    },
    [self](atom::internal, atom::schedule) {
      return self->state.schedule();
    },
  };
}

/// A helper function to get a handle to the REBUILDER actor from a client
/// process.
caf::expected<rebuilder_actor>
get_rebuilder(caf::actor_system& sys, const caf::settings& config) {
  auto self = caf::scoped_actor{sys};
  if (caf::get_or(config, "vast.node", false)
      && caf::get_or(config, "vast.rebuild.detached", false))
    return caf::make_error(ec::invalid_configuration,
                           "the options 'vast.node' and "
                           "'vast.rebuild.detached' "
                           "are incompatible");
  auto node_opt
    = system::spawn_or_connect_to_node(self, config, content(sys.config()));
  if (auto* err = std::get_if<caf::error>(&node_opt))
    return std::move(*err);
  const auto& node
    = std::holds_alternative<system::node_actor>(node_opt)
        ? std::get<system::node_actor>(node_opt)
        : std::get<scope_linked<system::node_actor>>(node_opt).get();
  const auto timeout = system::node_connection_timeout(config);
  auto result = caf::expected<caf::actor>{caf::error{}};
  self->request(node, timeout, atom::get_v, atom::type_v, "rebuild")
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
          // component from outside libvast.
          VAST_ASSERT(actors.size() == 1);
          result = std::move(actors[0]);
        }
      },
      [&](caf::error& err) { //
        result = std::move(err);
      });
  if (!result)
    return std::move(result.error());
  return caf::actor_cast<rebuilder_actor>(std::move(*result));
}

caf::message
rebuild_start_command(const invocation& inv, caf::actor_system& sys) {
  // Create a scoped actor for interaction with the actor system and connect to
  // the node.
  auto self = caf::scoped_actor{sys};
  auto rebuilder = get_rebuilder(sys, inv.options);
  if (!rebuilder)
    return caf::make_message(std::move(rebuilder.error()));
  // Parse the query expression, iff it exists.
  auto query = system::read_query(inv, "vast.rebuild.read",
                                  system::must_provide_query::no);
  if (!query)
    return caf::make_message(std::move(query.error()));
  auto expr = expression{};
  if (query->empty()) {
    expr = trivially_true_expression();
  } else {
    auto parsed = to<expression>(*query);
    if (!parsed)
      return caf::make_message(std::move(parsed.error()));
    expr = std::move(*parsed);
  }
  auto options = start_options{
    .all = caf::get_or(inv.options, "vast.rebuild.all", false),
    .undersized = caf::get_or(inv.options, "vast.rebuild.undersized", false),
    .parallel = caf::get_or(inv.options, "vast.rebuild.parallel", size_t{1}),
    .max_partitions = caf::get_or(inv.options, "vast.rebuild.max-partitions",
                                  std::numeric_limits<size_t>::max()),
    .expression = std::move(expr),
    .detached = caf::get_or(inv.options, "vast.rebuild.detached", false),
    .automatic = false,
  };
  auto result = caf::message{};
  self->request(*rebuilder, caf::infinite, atom::start_v, std::move(options))
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
  auto rebuilder = get_rebuilder(sys, inv.options);
  if (!rebuilder)
    return caf::make_message(std::move(rebuilder.error()));
  auto result = caf::message{};
  auto options = stop_options{
    .detached = caf::get_or(inv.options, "vast.rebuild.detached", false),
  };
  self->request(*rebuilder, caf::infinite, atom::stop_v, std::move(options))
    .receive(
      [] {
        // nop
      },
      [&](caf::error& err) {
        result = caf::make_message(std::move(err));
      });
  return result;
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
  caf::error initialize([[maybe_unused]] const record& plugin_config,
                        [[maybe_unused]] const record& global_config) override {
    return caf::none;
  }

  /// Returns the unique name of the plugin.
  [[nodiscard]] std::string name() const override {
    return "rebuild";
  }

  /// Creates additional commands.
  [[nodiscard]] std::pair<std::unique_ptr<command>, command::factory>
  make_command() const override {
    auto rebuild = std::make_unique<command>(
      "rebuild",
      "rebuilds outdated partitions matching the "
      "(optional) query expression",
      command::opts("?vast.rebuild")
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
      command::opts("?vast.rebuild")
        .add<bool>("detached,d", "exit immediately instead of waiting for the "
                                 "rebuild to be stopped"));
    auto factory = command::factory{
      {"rebuild start", rebuild_start_command},
      // Make 'vast rebuild' an alias for 'vast rebuild start'.
      {"rebuild", rebuild_start_command},
      {"rebuild stop", rebuild_stop_command},
    };
    return {std::move(rebuild), std::move(factory)};
  }

  system::component_plugin_actor
  make_component(system::node_actor::stateful_pointer<system::node_state> node)
    const override {
    auto [catalog, index, accountant]
      = node->state.registry.find<system::catalog_actor, system::index_actor,
                                  system::accountant_actor>();
    return node->spawn(rebuilder, std::move(catalog), std::move(index),
                       std::move(accountant));
  }
};

} // namespace

} // namespace vast::plugins::rebuild

CAF_BEGIN_TYPE_ID_BLOCK(vast_rebuild_plugin_types, 1400)
  CAF_ADD_TYPE_ID(vast_rebuild_plugin_types,
                  (vast::plugins::rebuild::start_options))
  CAF_ADD_TYPE_ID(vast_rebuild_plugin_types,
                  (vast::plugins::rebuild::stop_options))
CAF_END_TYPE_ID_BLOCK(vast_rebuild_plugin_types)

VAST_REGISTER_PLUGIN(vast::plugins::rebuild::plugin)
VAST_REGISTER_PLUGIN_TYPE_ID_BLOCK(vast_rebuild_plugin_types)

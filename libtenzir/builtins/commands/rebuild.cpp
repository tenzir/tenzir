//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/actors.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/catalog.hpp>
#include <tenzir/concept/parseable/tenzir/expression.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/connect_to_node.hpp>
#include <tenzir/data.hpp>
#include <tenzir/defaults.hpp>
#include <tenzir/detail/inspection_common.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/detail/weak_run_delayed.hpp>
#include <tenzir/fwd.hpp>
#include <tenzir/index.hpp>
#include <tenzir/node.hpp>
#include <tenzir/node_control.hpp>
#include <tenzir/partition_synopsis.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/query_context.hpp>
#include <tenzir/query_cursor.hpp>
#include <tenzir/read_query.hpp>
#include <tenzir/status.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/table_slice_builder.hpp>
#include <tenzir/uuid.hpp>

#include <arrow/table.h>
#include <caf/expected.hpp>
#include <caf/policy/select_all.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/type_id.hpp>
#include <caf/typed_event_based_actor.hpp>
#include <fmt/format.h>

namespace tenzir::plugins::rebuild {

namespace {

/// The threshold at which to consider a partition undersized, relative to the
/// configured 'tenzir.max-partition-size'.
inline constexpr auto undersized_threshold = 0.8;

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
using rebuilder_actor = typed_actor_fwd<
  // Start a rebuild.
  auto(atom::start, start_options)->caf::result<void>,
  // Stop a rebuild.
  auto(atom::stop, stop_options)->caf::result<void>,
  // INTERNAL: Continue working on the currently in-progress rebuild.
  auto(atom::internal, atom::rebuild)->caf::result<void>,
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

  /// Constants read once from the system configuration.
  size_t max_partition_size = 0u;
  size_t desired_batch_size = 0u;
  size_t automatic_rebuild = 0u;
  duration rebuild_interval = {};

  /// The state of the ongoing rebuild.
  std::optional<struct run> run = {};
  bool stopping = false;

  /// Shows the status of a currently ongoing rebuild.
  auto status(status_verbosity) -> record {
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
        fmt::format(
          "{} refuses to start rebuild while a rebuild is still "
          "ongoing ({}/{} done); consider running 'tenzir-ctl rebuild "
          "stop'",
          *self, run->statistics.num_completed, run->statistics.num_total));
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
    TENZIR_DEBUG("{} requests {}{} partitions matching the expression {}",
                 *self, run->options.all ? "all" : "outdated",
                 run->options.undersized ? " undersized" : "",
                 run->options.expression);
    auto rp = self->make_response_promise<void>();
    auto finish = [this, rp](caf::error err, bool silent = false) mutable {
      if (!silent) {
        // Only print to INFO when work was actually done, or when the run
        // was manually requested.
        if (run->statistics.num_completed == 0)
          if (run->options.automatic)
            TENZIR_VERBOSE("{} had nothing to do", *self);
          else
            TENZIR_INFO("{} had nothing to do", *self);
        else
          TENZIR_INFO("{} rebuilt {} into {} partitions", *self,
                      run->statistics.num_completed,
                      run->statistics.num_results);
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
        [this, finish](catalog_lookup_result& lookup_result) mutable {
          TENZIR_ASSERT(run->statistics.num_total == 0);
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
            TENZIR_DEBUG("{} ignores rebuild request for 0 partitions", *self);
            return finish({}, true);
          }
          if (run->options.automatic)
            TENZIR_VERBOSE("{} triggered an automatic run for {} candidate "
                           "partitions with {} threads",
                           *self, run->statistics.num_total,
                           run->options.parallel);
          else
            TENZIR_INFO(
              "{} triggered a run for {} candidate partitions with {} "
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
        TENZIR_DEBUG("{} got request to stop rebuild but no rebuild is running",
                     *self);
      else
        TENZIR_INFO("{} stopped ongoing rebuild", *self);
      stopping = false;
      return {};
    }
    stopping = true;
    if (!run->remaining_partitions.empty()) {
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
          TENZIR_TRACE("{} selects partition {} (v{}, {}) with "
                       "{} events (total: {})",
                       *self, partition.uuid, partition.version,
                       partition.schema, partition.events, current_run_events);
          return true;
        }
        return false;
      });
    run->remaining_partitions.erase(first_removed,
                                    run->remaining_partitions.end());
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
      TENZIR_DEBUG("{} skips rebuilding of undersized partition {} because no "
                   "other partition of schema {} exists",
                   *self, current_run_partitions[0].uuid,
                   current_run_partitions[0].schema);
      run->statistics.num_rebuilding -= 1;
      run->statistics.num_total -= 1;
      // Pick up new work until we run out of remainig partitions.
      return self->delegate(static_cast<rebuilder_actor>(self),
                            atom::internal_v, atom::rebuild_v);
    }
    // Ask the index to rebuild the partitions we selected.
    auto rp = self->make_response_promise<void>();
    auto rebatch
      = pipeline::internal_parse(fmt::format("batch {}", desired_batch_size));
    TENZIR_ASSERT(rebatch);
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
      ->request(index, caf::infinite, atom::apply_v, std::move(*rebatch),
                std::move(current_run_partitions), keep_original_partition::no)
      .then(
        [this, rp, current_run_events,
         num_partitions](std::vector<partition_info>& result) mutable {
          if (result.empty()) {
            TENZIR_DEBUG("{} skipped {} partitions as they are already being "
                         "transformed by another actor",
                         *self, num_partitions);
            run->statistics.num_total -= num_partitions;
            run->statistics.num_rebuilding -= num_partitions;
            // Pick up new work until we run out of remaining partitions.
            rp.delegate(static_cast<rebuilder_actor>(self), atom::internal_v,
                        atom::rebuild_v);
            return;
          }
          TENZIR_DEBUG("{} rebuilt {} into {} partitions", *self,
                       num_partitions, result.size());
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
            TENZIR_WARN("{} detected a mismatch: rebuilt {} events from {} "
                        "partitions into {} events in {} partitions",
                        *self, current_run_events, num_partitions,
                        result_events, result.size());
          // Adjust the counters, update the indicator, and move back
          // undersized transformed partitions to the list of remainig
          // partitions as desired.
          run->statistics.num_completed += num_partitions;
          run->statistics.num_results += result.size();
          run->statistics.num_rebuilding -= num_partitions;
          // Pick up new work until we run out of remainig partitions.
          rp.delegate(static_cast<rebuilder_actor>(self), atom::internal_v,
                      atom::rebuild_v);
        },
        [this, num_partitions = current_run_partitions.size(),
         rp](caf::error& error) mutable {
          TENZIR_WARN("{} failed to rebuild partititons: {}", *self, error);
          run->statistics.num_rebuilding -= num_partitions;
          // Pick up new work until we run out of remainig partitions.
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
  self->state.self = self;
  self->state.catalog = std::move(catalog);
  self->state.index = std::move(index);
  self->state.max_partition_size
    = caf::get_or(self->system().config(), "tenzir.max-partition-size",
                  defaults::max_partition_size);
  self->state.desired_batch_size
    = caf::get_or(self->system().config(), "tenzir.import.batch-size",
                  defaults::import::table_slice_size);
  self->state.automatic_rebuild = caf::get_or(
    self->system().config(), "tenzir.automatic-rebuild", size_t{1});
  if (self->state.automatic_rebuild > 0) {
    self->state.rebuild_interval
      = caf::get_or(self->system().config(), "tenzir.rebuild-interval",
                    defaults::rebuild_interval);
    self->state.schedule();
  }
  self->set_exit_handler([self](const caf::exit_msg& msg) {
    TENZIR_DEBUG("{} received EXIT from {}: {}", *self, msg.source, msg.reason);
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
  if (auto importer
      = self->system().registry().get<importer_actor>("tenzir.importer")) {
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
        const auto partitions
          = self->state.run ? self->state.run->statistics.num_rebuilding : 0;
        const auto queued_partitions
          = self->state.run ? self->state.run->statistics.num_total
                                - self->state.run->statistics.num_completed
                                - self->state.run->statistics.num_rebuilding
                            : 0;
        auto metric = builder.record();
        metric.field("timestamp", time::clock::now());
        metric.field("partitions", partitions);
        metric.field("queued_partitions", queued_partitions);
        self->send(importer, builder.finish_assert_one_slice());
      });
  }
  return {
    [self](atom::status, status_verbosity verbosity, duration) {
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
caf::expected<rebuilder_actor> get_rebuilder(caf::actor_system& sys) {
  auto self = caf::scoped_actor{sys};
  auto node_opt = connect_to_node(self);
  if (not node_opt) {
    return std::move(node_opt.error());
  }
  auto result = caf::expected<caf::actor>{caf::error{}};
  const auto node = std::move(*node_opt);
  self
    ->request(node, caf::infinite, atom::get_v, atom::label_v,
              std::vector<std::string>{"rebuilder"})
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
  if (!result)
    return std::move(result.error());
  return caf::actor_cast<rebuilder_actor>(std::move(*result));
}

caf::message
rebuild_start_command(const invocation& inv, caf::actor_system& sys) {
  // Create a scoped actor for interaction with the actor system and connect to
  // the node.
  auto self = caf::scoped_actor{sys};
  auto rebuilder = get_rebuilder(sys);
  if (!rebuilder)
    return caf::make_message(std::move(rebuilder.error()));
  // Parse the query expression, iff it exists.
  auto query = read_query(inv, "tenzir.rebuild.read", must_provide_query::no);
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
  auto rebuilder = get_rebuilder(sys);
  if (!rebuilder)
    return caf::make_message(std::move(rebuilder.error()));
  auto result = caf::message{};
  auto options = stop_options{
    .detached = caf::get_or(inv.options, "tenzir.rebuild.detached", false),
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
    auto factory = command::factory{
      {"rebuild start", rebuild_start_command},
      // Make 'tenzir rebuild' an alias for 'tenzir rebuild start'.
      {"rebuild", rebuild_start_command},
      {"rebuild stop", rebuild_stop_command},
    };
    return {std::move(rebuild), std::move(factory)};
  }

  auto make_component(node_actor::stateful_pointer<node_state> node) const
    -> component_plugin_actor override {
    auto [catalog, index]
      = node->state.registry.find<catalog_actor, index_actor>();
    return node->spawn(rebuilder, std::move(catalog), std::move(index));
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

TENZIR_REGISTER_PLUGIN(tenzir::plugins::rebuild::plugin)
TENZIR_REGISTER_PLUGIN_TYPE_ID_BLOCK(tenzir_rebuild_plugin_types)

//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/concept/parseable/to.hpp>
#include <vast/concept/parseable/vast/expression.hpp>
#include <vast/data.hpp>
#include <vast/detail/fanout_counter.hpp>
#include <vast/fwd.hpp>
#include <vast/partition_synopsis.hpp>
#include <vast/pipeline.hpp>
#include <vast/plugin.hpp>
#include <vast/query_context.hpp>
#include <vast/system/catalog.hpp>
#include <vast/system/connect_to_node.hpp>
#include <vast/system/index.hpp>
#include <vast/system/node.hpp>
#include <vast/system/query_cursor.hpp>
#include <vast/system/read_query.hpp>
#include <vast/system/status.hpp>
#include <vast/table_slice.hpp>
#include <vast/uuid.hpp>

#include <caf/expected.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/type_id.hpp>
#include <caf/typed_event_based_actor.hpp>
#include <fmt/format.h>

namespace vast::plugins::rebuild {

struct rebuild_request {
  bool undersized{false};
  bool all{false};
  std::size_t parallel{1u};
  expression expr;

  friend auto inspect(auto& f, rebuild_request& x) {
    return f(x.undersized, x.all, x.parallel, x.expr);
  }
};
} // namespace vast::plugins::rebuild

CAF_BEGIN_TYPE_ID_BLOCK(vast_rebuild_plugin_types, 1400)
  CAF_ADD_TYPE_ID(vast_rebuild_plugin_types,
                  (vast::plugins::rebuild::rebuild_request))

CAF_END_TYPE_ID_BLOCK(vast_rebuild_plugin_types)

namespace vast::plugins::rebuild {

namespace {

using rebuilder_actor
  = system::typed_actor_fwd<caf::reacts_to<atom::rebuild, rebuild_request>,
                            caf::reacts_to<atom::internal, atom::rebuild>>
  // Conform to the protocol of the STATUS CLIENT actor.
  ::extend_with<system::component_plugin_actor>::unwrap;

struct rebuilder_state {
  static constexpr const char* name = "rebuilder";

  system::catalog_actor catalog;
  system::index_actor index;
  std::size_t max_partition_size{0u};

  std::vector<partition_info> remaining_partitions = {};

  /// Various counters required to show a live progress bar of the potentially
  /// long-running rebuilding process.
  size_t num_total = {};
  size_t num_transforming = {};
  size_t num_transformed = {};
  size_t num_results = {};
  size_t num_heterogeneous = {};
  bool inaccurate_statistics = false;

  bool is_running = false;

  rebuilder_state() = default;
};

rebuilder_actor::behavior_type
rebuilder(rebuilder_actor::stateful_pointer<rebuilder_state> self,
          system::catalog_actor catalog, system::index_actor index) {
  self->state.catalog = std::move(catalog);
  self->state.index = std::move(index);

  self->state.max_partition_size
    = caf::get_or(self->system().config(), "vast.max-partition-size",
                  defaults::system::max_partition_size);
  VAST_INFO("Created actor: {}", *self);
  return {
    [self](atom::status, system::status_verbosity) -> record {
      return {
        {"num-total", self->state.num_total},
        {"num-transforming", self->state.num_transforming},
        {"num-transformed", self->state.num_transformed},
        {"num-results", self->state.num_results},
        {"num-heterogeneous", self->state.num_heterogeneous},
      };
    },
    [self](atom::rebuild, const rebuild_request& request) -> caf::result<void> {
      if (self->state.is_running)
        return caf::make_error(ec::invalid_argument, "Rebuild already running");
      self->state.is_running = true;

      const auto lookup_id = uuid::random();
      const auto max_partition_version = request.all
                                           ? version::partition_version
                                           : version::partition_version - 1;
      VAST_INFO("{} requests {}{} partitions matching the expression {}", *self,
                request.all ? "all" : "outdated",
                request.undersized ? " undersized" : "", request.expr);

      auto rp = self->make_response_promise<void>();
      auto finish = [self, rp](caf::error err) mutable {
        self->state.is_running = false;
        if (err) {
          rp.deliver(std::move(err));
          return;
        }
        static_cast<caf::response_promise&>(rp).deliver(caf::unit);
      };

      self
        ->request(self->state.catalog, caf::infinite, atom::candidates_v,
                  lookup_id, request.expr, max_partition_version)
        .then(
          [self, lookup_id, request,
           finish](system::catalog_result& result) mutable {
            if (request.undersized)
              std::erase_if(result.partitions,
                            [=](const partition_info& partition) {
                              return static_cast<bool>(partition.schema)
                                     && partition.events
                                          > self->state.max_partition_size / 2;
                            });
            if (result.partitions.empty()) {
              VAST_INFO("{} had nothing to do for request {}", *self,
                        lookup_id);
              return finish({});
            }
            self->state.num_total = result.partitions.size();
            self->state.num_heterogeneous
              = std::count_if(result.partitions.begin(),
                              result.partitions.end(),
                              [](const partition_info& partition) {
                                return !partition.schema;
                              });
            self->state.remaining_partitions = std::move(result.partitions);
            auto counter = detail::make_fanout_counter(
              request.parallel,
              [finish]() mutable {
                finish({});
              },
              [finish](caf::error error) mutable {
                finish(std::move(error));
              });
            VAST_DEBUG("{} triggers a rebuild for {} partitions with {} "
                       "parallel "
                       "runs",
                       *self, self->state.num_total, request.parallel);
            for (size_t i = 0; i < request.parallel; ++i) {
              self
                ->request(static_cast<rebuilder_actor>(self), caf::infinite,
                          atom::internal_v, atom::rebuild_v)
                .then(
                  [counter]() {
                    counter->receive_success();
                  },
                  [counter](caf::error& error) {
                    counter->receive_error(std::move(error));
                  });
            }
          },
          [self, finish](caf::error& error) mutable {
            finish(std::move(error));
          });
      return rp;
    },
    [self](atom::internal, atom::rebuild) -> caf::result<void> {
      if (self->state.remaining_partitions.empty())
        return {}; // We're done!
      auto current_run_partitions = std::vector<uuid>{};
      auto current_run_events = size_t{0};
      bool is_heterogeneous = false;
      bool is_oversized = false;
      if (self->state.num_heterogeneous > 0) {
        // If there's any partition that has no homogenous schema we want to
        // take it first and split it up into heterogeneous partitions.
        const auto heterogenenous_partition
          = std::find_if(self->state.remaining_partitions.begin(),
                         self->state.remaining_partitions.end(),
                         [](const partition_info& partition) {
                           return !partition.schema;
                         });
        if (heterogenenous_partition
            != self->state.remaining_partitions.end()) {
          is_heterogeneous = true;
          current_run_partitions.push_back(heterogenenous_partition->uuid);
          self->state.remaining_partitions.erase(heterogenenous_partition);
        } else {
          // Wait until we have all heterogeneous partitions transformed into
          // homogenous partitions before starting to work on the homogenous
          // parittions. In practice, this leads to less undeful partitions.
          return caf::skip;
        }
      } else {
        // Take the first homogenous partition and collect as many of the same
        // type as possible to create new paritions. The approach used may
        // collects too many partitions if there is no exact match, but that is
        // usually better than conservatively undersizing the number of
        // partitions for the current run. For oversized runs we move the last
        // transformed partition back to the list of remaining partitions if it
        // is less than 50% of the desired size.
        const auto schema = self->state.remaining_partitions[0].schema;
        const auto first_removed = std::remove_if(
          self->state.remaining_partitions.begin(),
          self->state.remaining_partitions.end(),
          [&](const partition_info& partition) {
            if (schema == partition.schema
                && current_run_events < self->state.max_partition_size) {
              current_run_events += partition.events;
              current_run_partitions.push_back(partition.uuid);
              return true;
            }
            return false;
          });
        self->state.remaining_partitions.erase(
          first_removed, self->state.remaining_partitions.end());
        is_oversized = current_run_events > self->state.max_partition_size;
      }
      // Ask the index to rebuild the partitions we selected.
      auto rp = self->make_response_promise<void>();
      const auto num_partitions = current_run_partitions.size();
      self->state.num_transforming += num_partitions;
      self
        ->request(self->state.index, caf::infinite, atom::rebuild_v,
                  std::move(current_run_partitions))
        .then(
          [self, rp, current_run_events, num_partitions, is_heterogeneous,
           is_oversized](std::vector<partition_info>& result) mutable {
            // Determines whether we moved partitions back.
            bool needs_second_stage = false;
            // If the number of events in the resulting partitions does not
            // match the number of events in the partitions that went in we ran
            // into a conflict with other partition transformations on an
            // overlapping set.
            const auto detected_conflict
              = current_run_events
                != std::transform_reduce(result.begin(), result.end(), size_t{},
                                         std::plus<>{},
                                         [](const partition_info& partition) {
                                           return partition.events;
                                         });
            if (detected_conflict) {
              self->state.inaccurate_statistics = true;
              self->state.num_transformed += num_partitions;
              self->state.num_total += result.size();
              if (is_heterogeneous)
                self->state.num_heterogeneous -= 1;
              needs_second_stage = true;
              std::copy(result.begin(), result.end(),
                        std::back_inserter(self->state.remaining_partitions));
            } else {
              // Adjust the counters, update the indicator, and move back
              // undersized transformed partitions to the list of remainig
              // partitions as desired.
              VAST_ASSERT(!result.empty());
              if (is_heterogeneous) {
                VAST_ASSERT(num_partitions == 1);
                self->state.num_heterogeneous -= 1;
                if (result.size() > 1
                    || result[0].events <= self->state.max_partition_size / 2) {
                  self->state.num_total += result.size();
                  std::copy(
                    result.begin(), result.end(),
                    std::back_inserter(self->state.remaining_partitions));
                  needs_second_stage = true;
                } else {
                  self->state.num_transformed += 1;
                }
              } else {
                self->state.num_transformed += num_partitions;
                self->state.num_results += result.size();
              }
              if (is_oversized) {
                VAST_ASSERT(result.size() > 1);
                if (result.back().events
                    <= self->state.max_partition_size / 2) {
                  self->state.remaining_partitions.push_back(
                    std::move(result.back()));
                  needs_second_stage = true;
                  self->state.num_transformed -= 1;
                  self->state.num_results -= 1;
                  self->state.num_total += 1;
                }
              }
            }
            if (needs_second_stage)
              std::sort(self->state.remaining_partitions.begin(),
                        self->state.remaining_partitions.end(),
                        [](const partition_info& lhs,
                           const partition_info& rhs) {
                          return lhs.max_import_time > rhs.max_import_time;
                        });
            self->state.num_transforming -= num_partitions;
            // Pick up new work until we run out of remainig partitions.
            rp.delegate(static_cast<rebuilder_actor>(self), atom::internal_v,
                        atom::rebuild_v);
          },
          [self, num_partitions, rp](caf::error& error) mutable {
            self->state.num_transforming -= num_partitions;
            VAST_WARN("{} failed to rebuild partititons: {}", *self, error);
            // Pick up new work until we run out of remainig partitions.
            rp.delegate(static_cast<rebuilder_actor>(self), atom::internal_v,
                        atom::rebuild_v);
          });
      return rp;
    },
  };
}

caf::expected<rebuilder_actor>
get_rebuilder(caf::actor_system& sys, const caf::settings& config) {
  auto self = caf::scoped_actor{sys};
  auto node = system::connect_to_node(self, config);
  if (!node)
    return caf::make_error(ec::missing_component,
                           fmt::format("failed to connect to node: {}",
                                       node.error()));
  auto result = caf::expected<caf::actor>{caf::no_error};
  self
    ->request(*node, defaults::system::initial_request_timeout, atom::get_v,
              atom::type_v, "rebuild")
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

caf::message rebuild_command(const invocation& inv, caf::actor_system& sys) {
  // Read options.
  auto request = rebuild_request{};
  request.all = caf::get_or(inv.options, "vast.rebuild.all", false);
  request.undersized
    = caf::get_or(inv.options, "vast.rebuild.undersized", false);
  request.parallel
    = caf::get_or(inv.options, "vast.rebuild.parallel", size_t{1});
  if (request.parallel == 0)
    return caf::make_message(caf::make_error(
      ec::invalid_configuration, "rebuild requires a non-zero parallel level"));
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
  auto expr = to<expression>(*query);
  if (!expr)
    return caf::make_message(std::move(expr.error()));
  request.expr = std::move(*expr);

  auto result = caf::message{};
  self->request(*rebuilder, caf::infinite, atom::rebuild_v, std::move(request))
    .receive(
      [] {

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
  caf::error initialize(data) override {
    return caf::none;
  }

  /// Returns the unique name of the plugin.
  [[nodiscard]] const char* name() const override {
    return "rebuild";
  }

  /// Creates additional commands.
  [[nodiscard]] std::pair<std::unique_ptr<command>, command::factory>
  make_command() const override {
    auto rebuild = std::make_unique<command>(
      "rebuild",
      "rebuilds outdated partitions matching the (optional) query expression",
      command::opts("?vast.rebuild")
        .add<bool>("all", "consider all (rather than outdated) partitions")
        .add<bool>("undersized", "consider only undersized partitions")
        .add<std::string>("read,r", "path for reading the (optional) query")
        .add<size_t>("parallel,j", "number of runs to start in parallel "
                                   "(default: 1)"));
    auto factory = command::factory{
      {"rebuild", rebuild_command},
    };
    return {std::move(rebuild), std::move(factory)};
  }

  system::component_plugin_actor
  make_component(system::node_actor::stateful_pointer<system::node_state> node)
    const override {
    auto [catalog, index]
      = node->state.registry.find<system::catalog_actor, system::index_actor>();
    return node->spawn(rebuilder, catalog, index);
  }
};

} // namespace

} // namespace vast::plugins::rebuild

VAST_REGISTER_PLUGIN(vast::plugins::rebuild::plugin)

VAST_REGISTER_PLUGIN_TYPE_ID_BLOCK(vast_rebuild_plugin_types)

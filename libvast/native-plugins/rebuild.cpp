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
#include <vast/defaults.hpp>
#include <vast/detail/fanout_counter.hpp>
#include <vast/detail/narrow.hpp>
#include <vast/error.hpp>
#include <vast/logger.hpp>
#include <vast/plugin.hpp>
#include <vast/system/catalog.hpp>
#include <vast/system/index.hpp>
#include <vast/system/node_control.hpp>
#include <vast/system/read_query.hpp>
#include <vast/system/spawn_or_connect_to_node.hpp>

#include <caf/after.hpp>
#include <caf/scoped_actor.hpp>
#include <indicators/cursor_control.hpp>
#include <indicators/progress_spinner.hpp>
#include <indicators/setting.hpp>

namespace vast::plugins::rebuild {

namespace {

using client_actor = caf::typed_actor<caf::reacts_to<atom::rebuild>>;

struct client_state {
  static constexpr auto name = "rebuild-client";

  client_actor::pointer self = {};
  system::index_actor index = {};
  std::vector<partition_info> remaining_partitions = {};
  std::unique_ptr<indicators::ProgressSpinner> indicator = {};

  size_t num_total = {};
  size_t num_transforming = {};
  size_t num_transformed = {};
  size_t num_results = {};
  size_t num_heterogeneous = {};

  void create() {
    VAST_ASSERT(!indicator);
    indicators::show_console_cursor(false);
    self->attach_functor([] {
      indicators::show_console_cursor(true);
    });
    indicator = std::make_unique<indicators::ProgressSpinner>(
      indicators::option::ShowPercentage{false},
      indicators::option::ForegroundColor{indicators::Color::yellow},
      indicators::option::SpinnerStates{
        std::vector<std::string>{"⠈", "⠐", "⠠", "⢀", "⡀", "⠄", "⠂", "⠁"}},
      indicators::option::MaxProgress{0});
  }

  void tick() const {
    if (!indicator)
      return;
    indicator->set_option(indicators::option::MaxProgress{num_total});
    indicator->set_progress(num_transformed);
    indicator->set_option(indicators::option::PostfixText{
      fmt::format("[{}/{}] Transforming {}/{} partitions...", num_transformed,
                  num_total, num_transforming, num_total - num_transformed)});
    indicator->tick();
  }

  void finish() const {
    VAST_ASSERT(indicator);
    VAST_ASSERT(indicator->is_completed());
    VAST_ASSERT(num_transformed == num_total);
    indicator->set_option(
      indicators::option::ForegroundColor{indicators::Color::green});
    indicator->set_option(indicators::option::PrefixText{"✔"});
    indicator->set_option(indicators::option::ShowSpinner{false});
    indicator->set_option(indicators::option::ShowPercentage{false});
    indicator->set_option(indicators::option::ShowRemainingTime{false});
    indicator->set_option(indicators::option::FontStyles{
      std::vector<indicators::FontStyle>{indicators::FontStyle::bold}});
    indicator->set_option(indicators::option::PostfixText{
      fmt::format("Done! Transformed {} into {} partitions.", num_transformed,
                  num_results)});
    indicator->mark_as_completed();
  }
};

client_actor::behavior_type
client(client_actor::stateful_pointer<client_state> self,
       const system::catalog_actor& catalog, system::index_actor index,
       expression expr, size_t parallel, bool all) {
  VAST_ASSERT(parallel != 0);
  self->state.self = self;
  self->state.index = std::move(index);
  // Get the current max partition size and batch size; this is used internally
  // to smartly configure the number of partitions to run on in parallel.
  const auto max_partition_size
    = caf::get_or(catalog->home_system().config(), "vast.max-partition-size",
                  defaults::system::max_partition_size);
  const auto batch_size
    = caf::get_or(catalog->home_system().config(), "vast.import.batch-size",
                  defaults::import::table_slice_size);
  // Get the partition IDs from the catalog.
  const auto lookup_id = uuid::random();
  const auto max_partition_version = all
                                       ? defaults::latest_partition_version
                                       : defaults::latest_partition_version - 1;
  VAST_INFO("{} requests {} partitions matching the expression {}", *self,
            all ? "all" : "outdated", expr);
  self
    ->request(catalog, caf::infinite, atom::candidates_v, lookup_id, expr,
              max_partition_version)
    .then(
      [self, parallel](system::catalog_result& result) {
        if (result.partitions.empty()) {
          fmt::print("no partitions need to be rebuilt\n", *self);
          self->quit();
          return;
        }
        for (const auto& part : result.partitions)
          VAST_WARN("{}: {} {} {}", part.uuid, part.events,
                    data{part.max_import_time}, part.schema);
        self->state.num_total = result.partitions.size();
        self->state.remaining_partitions = std::move(result.partitions);
        auto counter = detail::make_fanout_counter(
          parallel,
          [self]() {
            self->state.finish();
            self->quit();
          },
          [self](caf::error error) {
            self->quit(std::move(error));
          });
        VAST_INFO("{} triggers a rebuild for {} partitions with {} parallel "
                  "runs",
                  *self, self->state.num_total, parallel);
        // Create the indicator spinner.
        self->state.create();
        self->state.tick();
        for (size_t i = 0; i < parallel; ++i) {
          self
            ->request(static_cast<client_actor>(self), caf::infinite,
                      atom::rebuild_v)
            .then(
              [counter]() {
                counter->receive_success();
              },
              [counter](caf::error& error) {
                counter->receive_error(std::move(error));
              });
        }
      },
      [self](caf::error& error) {
        self->quit(std::move(error));
      });
  return {
    [self, max_partition_size, batch_size](atom::rebuild) -> caf::result<void> {
      if (self->state.remaining_partitions.empty())
        return {}; // We're done!
      auto current_run_partitions = std::vector<uuid>{};
      // If there's any partition that has no homogenous schema we want to take
      // it first.
      bool is_heterogeneous = false;
      bool is_oversized = false;
      const auto heterogenenous_partition
        = std::find_if(self->state.remaining_partitions.begin(),
                       self->state.remaining_partitions.end(),
                       [](const partition_info& partition) {
                         return !partition.schema;
                       });
      if (heterogenenous_partition != self->state.remaining_partitions.end()) {
        is_heterogeneous = true;
        self->state.num_heterogeneous += 1;
        current_run_partitions.push_back(heterogenenous_partition->uuid);
        self->state.remaining_partitions.erase(heterogenenous_partition);
      } else if (self->state.num_heterogeneous > 0) {
        VAST_WARN("???");
        return caf::skip;
      } else {
        const auto schema = self->state.remaining_partitions[0].schema;
        auto num_events = size_t{0};
        const auto first_removed = std::remove_if(
          self->state.remaining_partitions.begin(),
          self->state.remaining_partitions.end(),
          [&](const partition_info& partition) {
            if (schema == partition.schema && num_events < max_partition_size) {
              num_events += partition.events;
              current_run_partitions.push_back(partition.uuid);
              return true;
            }
            return false;
          });
        self->state.remaining_partitions.erase(
          first_removed, self->state.remaining_partitions.end());
        is_oversized = num_events > max_partition_size;
      }
      // Ask the index to rebuild the partitions.
      auto rp = self->make_response_promise<void>();
      const auto num_partitions = current_run_partitions.size();
      self->state.num_transforming += num_partitions;
      self->state.tick(); self
        ->request(self->state.index, caf::infinite, atom::rebuild_v,
                  std::move(current_run_partitions))
        .then(
          [self, rp, num_partitions, max_partition_size, batch_size,
           is_heterogeneous,
           is_oversized](std::vector<partition_info>& result) mutable {
            if (is_heterogeneous) {
              self->state.num_heterogeneous -= 1;
              std::copy(result.begin(), result.end(),
                        std::back_inserter(self->state.remaining_partitions));
            } else {
              self->state.num_transformed += num_partitions;
              self->state.num_results += result.size();
            }
            if (is_oversized) {
              std::copy_if(result.begin(), result.end(),
                           std::back_inserter(self->state.remaining_partitions),
                           [&](const partition_info& partition) {
                             if (partition.events + batch_size < max_partition_size) {
                               self->state.num_transformed -= 1;
                               self->state.num_results -= 1;
                               return true;
                             }
                             return false;
                           });
            }
            if (is_heterogeneous || is_oversized)
              std::sort(self->state.remaining_partitions.begin(),
                        self->state.remaining_partitions.end(),
                        [](const partition_info& lhs,
                           const partition_info& rhs) {
                          return lhs.max_import_time > rhs.max_import_time;
                        });
            self->state.num_transforming -= num_partitions;
            self->state.tick();
            rp.delegate(static_cast<client_actor>(self), atom::rebuild_v);
          },
          [self, num_partitions](caf::error& error) {
            self->state.num_transforming -= num_partitions;
            self->state.tick();
            self->quit(std::move(error));
          });
      return rp;
    },
    caf::after(std::chrono::milliseconds{125}) >>
      [self]() noexcept {
        self->state.tick();
      },
  };
}

caf::message rebuild_command(const invocation& inv, caf::actor_system& sys) {
  // Read options.
  const auto all = caf::get_or(inv.options, "vast.rebuild.all", false);
  const auto parallel
    = caf::get_or(inv.options, "vast.rebuild.parallel", size_t{1});
  if (parallel == 0)
    return caf::make_message(caf::make_error(ec::invalid_configuration,
                                             "rebuild requires a non-zero step "
                                             "size and parallel level"));
  // Create a scoped actor for interaction with the actor system.
  auto self = caf::scoped_actor{sys};
  // Connect to the node.
  auto node_opt = system::spawn_or_connect_to_node(self, inv.options,
                                                   content(sys.config()));
  if (auto* err = std::get_if<caf::error>(&node_opt))
    return caf::make_message(std::move(*err));
  const auto& node
    = std::holds_alternative<system::node_actor>(node_opt)
        ? std::get<system::node_actor>(node_opt)
        : std::get<scope_linked<system::node_actor>>(node_opt).get();
  // Get catalog and index actors.
  auto components
    = system::get_node_components<system::catalog_actor, system::index_actor>(
      self, node);
  if (!components)
    return caf::make_message(std::move(components.error()));
  auto [catalog, index] = std::move(*components);
  // Parse the query expression, iff it exists.
  auto query = system::read_query(inv, "vast.rebuild.read",
                                  system::must_provide_query::no);
  if (!query)
    return caf::make_message(std::move(query.error()));
  auto expr = to<expression>(*query);
  if (!expr)
    return caf::make_message(std::move(expr.error()));
  auto handle
    = self->spawn<caf::monitored>(client, std::move(catalog), std::move(index),
                                  std::move(*expr), parallel, all);
  auto result = caf::error{};
  auto done = false;
  self
    ->do_receive([&](caf::down_msg& msg) {
      VAST_ASSERT(msg.source == handle.address());
      result = std::move(msg.reason);
      done = true;
    })
    .until(done);
  if (result)
    return caf::make_message(std::move(result));
  return caf::none;
}

/// An example plugin.
class plugin final : public virtual command_plugin {
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
        .add<bool>("all", "consider all partitions")
        .add<std::string>("read,r", "path for reading the (optional) query")
        .add<size_t>("parallel,j", "number of runs to start in parallel "
                                   "(default: 1)"));
    auto factory = command::factory{
      {"rebuild", rebuild_command},
    };
    return {std::move(rebuild), std::move(factory)};
  };
};

} // namespace

} // namespace vast::plugins::rebuild

VAST_REGISTER_PLUGIN(vast::plugins::rebuild::plugin)

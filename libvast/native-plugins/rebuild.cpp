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

using client_actor = caf::typed_actor<caf::reacts_to<atom::rebuild, uint64_t>>;

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
       expression expr, size_t step_size, size_t parallel, bool all) {
  VAST_ASSERT(parallel != 0);
  VAST_ASSERT(step_size != 0);
  self->state.self = self;
  self->state.index = std::move(index);
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
      [self, parallel, step_size](system::catalog_result& result) {
        if (result.partitions.empty()) {
          fmt::print("no partitions need to be rebuilt\n", *self);
          self->quit();
          return;
        }
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
        VAST_INFO("{} triggers a rebuild for up to {} out of {} partitions "
                  "per run with {} parallel runs",
                  *self, step_size, self->state.num_total, parallel);
        // Create the indicator spinner.
        self->state.create();
        self->state.tick();
        for (size_t i = 0; i < parallel; ++i) {
          self
            ->request(static_cast<client_actor>(self), caf::infinite,
                      atom::rebuild_v, step_size)
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
    [self](atom::rebuild, uint64_t step_size) -> caf::result<void> {
      const uint64_t num_partitions
        = std::min(step_size, detail::narrow_cast<uint64_t>(
                                self->state.remaining_partitions.size()));
      if (num_partitions == 0)
        return {};
      // Take the appropriate amount of partitions from the remaining
      // partitions.
      auto partitions = std::vector<uuid>{};
      partitions.reserve(num_partitions);
      const auto first_remaining_partition
        = std::prev(self->state.remaining_partitions.end(),
                    detail::narrow_cast<ptrdiff_t>(num_partitions));
      std::transform(first_remaining_partition,
                     self->state.remaining_partitions.end(),
                     std::back_inserter(partitions),
                     [](const partition_info& partition) {
                       return partition.uuid;
                     });
      self->state.remaining_partitions.erase(
        first_remaining_partition, self->state.remaining_partitions.end());
      // Ask the index to rebuild the partitions.
      auto rp = self->make_response_promise<void>();
      self->state.num_transforming += num_partitions;
      self->state.tick();
      self
        ->request(self->state.index, caf::infinite, atom::rebuild_v,
                  std::move(partitions))
        .then(
          [self, rp, num_partitions,
           step_size](std::vector<partition_info>& result) mutable {
            self->state.num_transforming -= num_partitions;
            self->state.num_transformed += num_partitions;
            self->state.num_results += result.size();
            self->state.tick();
            rp.delegate(static_cast<client_actor>(self), atom::rebuild_v,
                        step_size);
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
  const auto step_size
    = caf::get_or(inv.options, "vast.rebuild.step-size", size_t{4});
  if (parallel == 0 || step_size == 0)
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
                                  std::move(*expr), step_size, parallel, all);
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
        .add<size_t>("step-size,n", "number of partitions to transform per run "
                                    "(default: 4)")
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

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

using rebuilder_actor = caf::typed_actor<caf::reacts_to<atom::rebuild>>;

struct rebuilder_state {
  /// The name of the rebuilder actor in logs.
  [[maybe_unused]] static constexpr auto name = "rebuilder";

  /// The state of the rebuilder actor.
  rebuilder_actor::pointer self = {};
  system::index_actor index = {};
  size_t parallel = {};
  size_t max_partition_size = {};
  std::vector<partition_info> remaining_partitions = {};
  std::unique_ptr<indicators::ProgressSpinner> indicator = {};

  /// Various counters required to show a live progress bar of the potentially
  /// long-running rebuilding process.
  size_t num_total = {};
  size_t num_transforming = {};
  size_t num_transformed = {};
  size_t num_results = {};
  size_t num_heterogeneous = {};

  /// Creates the indicator bar that shows the current progress.
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

  /// Update the indicator bar to show the current progress.
  void tick() const {
    if (!indicator)
      return;
    indicator->set_option(indicators::option::MaxProgress{num_total});
    indicator->set_progress(num_transformed);
    if (num_heterogeneous > 0)
      indicator->set_option(indicators::option::PostfixText{fmt::format(
        "[{}/{}] Phase 1/2: Splitting {}/{} heterogeneous partitions...",
        num_transformed, num_total, num_heterogeneous,
        num_transforming + num_heterogeneous)});
    else
      indicator->set_option(indicators::option::PostfixText{
        fmt::format("[{}/{}] Phase 2/2: Merging {}/{} homogenous partitions...",
                    num_transformed, num_total, num_transforming,
                    num_total - num_transformed)});
    indicator->tick();
  }

  /// Finish the indicator bar.
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

rebuilder_actor::behavior_type
rebuilder(rebuilder_actor::stateful_pointer<rebuilder_state> self,
          const system::catalog_actor& catalog, system::index_actor index,
          expression expr, size_t parallel, bool all,
          size_t max_partition_size) {
  VAST_ASSERT(parallel != 0);
  self->state.self = self;
  self->state.index = std::move(index);
  self->state.parallel = parallel;
  self->state.max_partition_size = max_partition_size;
  // Get the current max partition size and batch size; this is used internally
  // to smartly configure the number of partitions to run on in parallel.
  // Get the partition IDs from the catalog.
  const auto lookup_id = uuid::random();
  const auto max_partition_version
    = all ? version::partition_version : version::partition_version - 1;
  VAST_INFO("{} requests {} partitions matching the expression {}", *self,
            all ? "all" : "outdated", expr);
  self
    ->request(catalog, caf::infinite, atom::candidates_v, lookup_id, expr,
              max_partition_version)
    .then(
      [self](system::catalog_result& result) {
        if (result.partitions.empty()) {
          fmt::print("no partitions need to be rebuilt\n", *self);
          self->quit();
          return;
        }
        self->state.num_total = result.partitions.size();
        self->state.num_heterogeneous
          = std::count_if(result.partitions.begin(), result.partitions.end(),
                          [](const partition_info& partition) {
                            return !partition.schema;
                          });
        self->state.remaining_partitions = std::move(result.partitions);
        auto counter = detail::make_fanout_counter(
          self->state.parallel,
          [self]() {
            self->state.finish();
            self->quit();
          },
          [self](caf::error error) {
            self->quit(std::move(error));
          });
        VAST_DEBUG("{} triggers a rebuild for {} partitions with {} parallel "
                   "runs",
                   *self, self->state.num_total, self->state.parallel);
        // Create the indicator spinner.
        self->state.create();
        self->state.tick();
        for (size_t i = 0; i < self->state.parallel; ++i) {
          self
            ->request(static_cast<rebuilder_actor>(self), caf::infinite,
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
    [self](atom::rebuild) -> caf::result<void> {
      if (self->state.remaining_partitions.empty())
        return {}; // We're done!
      auto current_run_partitions = std::vector<uuid>{};
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
        auto num_events = size_t{0};
        const auto first_removed = std::remove_if(
          self->state.remaining_partitions.begin(),
          self->state.remaining_partitions.end(),
          [&](const partition_info& partition) {
            if (schema == partition.schema
                && num_events < self->state.max_partition_size) {
              num_events += partition.events;
              current_run_partitions.push_back(partition.uuid);
              return true;
            }
            return false;
          });
        self->state.remaining_partitions.erase(
          first_removed, self->state.remaining_partitions.end());
        is_oversized = num_events > self->state.max_partition_size;
      }
      // Ask the index to rebuild the partitions we selected.
      auto rp = self->make_response_promise<void>();
      const auto num_partitions = current_run_partitions.size();
      self->state.num_transforming += num_partitions;
      self->state.tick();
      self
        ->request(self->state.index, caf::infinite, atom::rebuild_v,
                  std::move(current_run_partitions))
        .then(
          [self, rp, num_partitions, is_heterogeneous,
           is_oversized](std::vector<partition_info>& result) mutable {
            // Adjust the counters, update the indicator, and move back
            // undersized transformed partitions to the list of remainig
            // partitions as desired.
            VAST_ASSERT(!result.empty());
            bool needs_second_stage = false;
            if (is_heterogeneous) {
              self->state.num_heterogeneous -= 1;
              if (result.size() > 1
                  || result[0].events <= self->state.max_partition_size / 2) {
                std::copy(result.begin(), result.end(),
                          std::back_inserter(self->state.remaining_partitions));
                needs_second_stage = true;
              }
            } else {
              self->state.num_transformed += num_partitions;
              self->state.num_results += result.size();
            }
            if (is_oversized) {
              VAST_ASSERT(result.size() > 1);
              if (result.back().events <= self->state.max_partition_size / 2) {
                self->state.remaining_partitions.push_back(
                  std::move(result.back()));
                needs_second_stage = true;
                self->state.num_transformed -= 1;
                self->state.num_results -= 1;
                self->state.num_total += 1;
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
            self->state.tick();
            // Pick up new work until we run out of remainig partitions.
            rp.delegate(static_cast<rebuilder_actor>(self), atom::rebuild_v);
          },
          [self, num_partitions](caf::error& error) {
            self->state.num_transforming -= num_partitions;
            self->state.tick();
            self->quit(std::move(error));
          });
      return rp;
    },
    // While the rebuilding is in progress, update the spinner 8x per second.
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
    return caf::make_message(caf::make_error(
      ec::invalid_configuration, "rebuild requires a non-zero parallel level"));
  // Create a scoped actor for interaction with the actor system and connect to
  // the node.
  auto self = caf::scoped_actor{sys};
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
  // Get the server's max partition size.
  auto max_partition_size
    = caf::expected<size_t>{defaults::system::max_partition_size};
  self->request(node, caf::infinite, atom::config_v)
    .receive(
      [&](record& config) {
        const auto vast_it = config.find("vast");
        if (vast_it == config.end()
            || !caf::holds_alternative<record>(vast_it->second))
          return;
        const auto& vast = caf::get<record>(vast_it->second);
        const auto max_partition_size_it = vast.find("max-partition-size");
        if (max_partition_size_it == vast.end()
            || !caf::holds_alternative<count>(max_partition_size_it->second))
          return;
        max_partition_size = caf::get<count>(max_partition_size_it->second);
      },
      [&](caf::error& error) {
        max_partition_size = std::move(error);
      });
  if (!max_partition_size)
    return caf::make_message(std::move(max_partition_size.error()));
  // Spawn a rebuilder, and wait for it to finish.
  auto handle = self->spawn<caf::monitored>(rebuilder, std::move(catalog),
                                            std::move(index), std::move(*expr),
                                            parallel, all, *max_partition_size);
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

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
#include <vast/fwd.hpp>
#include <vast/partition_synopsis.hpp>
#include <vast/plugin.hpp>
#include <vast/system/catalog.hpp>
#include <vast/system/connect_to_node.hpp>
#include <vast/system/index.hpp>
#include <vast/system/node.hpp>
#include <vast/system/read_query.hpp>
#include <vast/system/status.hpp>
#include <vast/uuid.hpp>

#include <caf/expected.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/type_id.hpp>
#include <caf/typed_event_based_actor.hpp>
#include <fmt/format.h>

namespace vast::plugins::rebuild {

namespace {

struct rebuild_request {
  bool undersized{false};
  bool all{false};
  std::size_t parallel{1u};
  expression expr;

  friend auto inspect(auto& f, rebuild_request& x) {
    return f(x.undersized, x.all, x.parallel, x.expr);
  }
};

using rebuilder_actor
  = system::typed_actor_fwd<caf::reacts_to<atom::rebuild, rebuild_request>>
  // Conform to the protocol of the STATUS CLIENT actor.
  ::extend_with<system::component_plugin_actor>::unwrap;

struct rebuilder_state {
  static constexpr const char* name = "rebuilder";

  system::catalog_actor catalog;
  system::index_actor index;
};

rebuilder_actor::behavior_type
rebuilder(rebuilder_actor::stateful_pointer<rebuilder_state> self,
          system::catalog_actor catalog, system::index_actor index) {
  self->state.catalog = std::move(catalog);
  self->state.index = std::move(index);
  VAST_INFO("Created actor: {}", *self);
  return {
    [](atom::status, system::status_verbosity) -> record {
      return {};
    },
    [](atom::rebuild, const rebuild_request&) -> caf::result<void> {
      return {};
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
    return node->spawn(rebuilder, std::move(catalog), std::move(index));
  }
};

} // namespace

} // namespace vast::plugins::rebuild

VAST_REGISTER_PLUGIN(vast::plugins::rebuild::plugin)

CAF_BEGIN_TYPE_ID_BLOCK(vast_rebuild_plugin_types, 1400)
  CAF_ADD_TYPE_ID(vast_rebuild_plugin_types,
                  (vast::plugins::rebuild::rebuild_request))

CAF_END_TYPE_ID_BLOCK(vast_rebuild_plugin_types)

VAST_REGISTER_PLUGIN_TYPE_ID_BLOCK(vast_rebuild_plugin_types)

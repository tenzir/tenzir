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
#include <vast/error.hpp>
#include <vast/logger.hpp>
#include <vast/plugin.hpp>
#include <vast/system/catalog.hpp>
#include <vast/system/index.hpp>
#include <vast/system/node_control.hpp>
#include <vast/system/read_query.hpp>
#include <vast/system/spawn_or_connect_to_node.hpp>

#include <caf/function_view.hpp>
#include <caf/scoped_actor.hpp>

namespace vast::plugins::rebuild {

namespace {

caf::message rebuild_command(const invocation& inv, caf::actor_system& sys) {
  // Read options.
  const auto all = caf::get_or(inv.options, "vast.rebuild.all", false);
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
  const auto& [catalog, index] = std::move(*components);
  // Parse the query expression, iff it exists.
  auto query = system::read_query(inv, "vast.rebuild.read",
                                  system::must_provide_query::no);
  if (!query)
    return caf::make_message(std::move(query.error()));
  auto expr = to<expression>(*query);
  if (!expr)
    return caf::make_message(std::move(expr.error()));
  // Get the partition IDs from the catalog.
  const auto lookup_id = uuid::random();
  const auto max_partition_version = all
                                       ? defaults::latest_partition_version
                                       : defaults::latest_partition_version - 1;
  fmt::print("requesting {} partitions from the catalog...\n",
             all ? "all" : "outdated");
  auto catalog_result = caf::expected<system::catalog_result>{caf::no_error};
  self
    ->request(catalog, caf::infinite, atom::candidates_v, lookup_id, *expr,
              max_partition_version)
    .receive(
      [&](system::catalog_result& value) {
        catalog_result = std::move(value);
      },
      [&](caf::error& err) {
        catalog_result = std::move(err);
      });
  if (!catalog_result)
    return caf::make_message(std::move(catalog_result.error()));
  if (catalog_result->partitions.empty()) {
    fmt::print("nothing to do\n");
    return caf::none;
  }
  fmt::print("starting transformation of {} partitions...\n",
             catalog_result->partitions.size());
  // Run identity transform on all partitions for the index.
  auto partition_info
    = caf::expected<std::vector<vast::partition_info>>{caf::no_error};
  self
    ->request(index, caf::infinite, atom::rebuild_v, catalog_result->partitions)
    .receive(
      [&](std::vector<vast::partition_info>& value) {
        partition_info = std::move(value);
      },
      [&](caf::error& err) {
        partition_info = std::move(err);
      });
  if (!partition_info)
    return caf::make_message(std::move(partition_info.error()));
  // Print some statistics for the user.
  fmt::print("successfully transformed {} -> {} partitions\n",
             catalog_result->partitions.size(), partition_info->size());
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
      "rebuild", "TODO",
      command::opts("?vast.rebuild")
        .add<bool>("all", "TODO")
        .add<std::string>("read,r", "path for reading the query"));
    auto factory = command::factory{
      {"rebuild", rebuild_command},
    };
    return {std::move(rebuild), std::move(factory)};
  };
};

} // namespace

} // namespace vast::plugins::rebuild

VAST_REGISTER_PLUGIN(vast::plugins::rebuild::plugin)

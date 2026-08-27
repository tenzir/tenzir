//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/option.hpp"

#include <tenzir/actors.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/catalog.hpp>
#include <tenzir/concept/parseable/tenzir/expression.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/connect_to_node.hpp>
#include <tenzir/data.hpp>
#include <tenzir/defaults.hpp>
#include <tenzir/detail/available_memory.hpp>
#include <tenzir/detail/inspection_common.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/detail/saturating_arithmetic.hpp>
#include <tenzir/detail/weak_run_delayed.hpp>
#include <tenzir/fwd.hpp>
#include <tenzir/node.hpp>
#include <tenzir/partition_synopsis.hpp>
#include <tenzir/partition_transformer.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/query_context.hpp>
#include <tenzir/read_query.hpp>
#include <tenzir/session.hpp>
#include <tenzir/status.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/ast.hpp>
#include <tenzir/tql2/parser.hpp>
#include <tenzir/uuid.hpp>

#include <arrow/table.h>
#include <arrow/vendored/datetime.h>
#include <caf/actor_registry.hpp>
#include <caf/expected.hpp>
#include <caf/policy/select_all.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/settings.hpp>
#include <caf/type_id.hpp>
#include <caf/typed_event_based_actor.hpp>
#include <fmt/format.h>
namespace date = arrow_vendored::date;

#include <cmath>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace tenzir::plugins::rebuild {

namespace {

auto get_component(caf::scoped_actor& self, const node_actor& node,
                   std::string label) -> caf::expected<caf::actor> {
  auto result = caf::expected<caf::actor>{caf::error{}};
  self->mail(atom::get_v, atom::label_v, std::vector<std::string>{label})
    .request(node, caf::infinite)
    .receive(
      [&](std::vector<caf::actor>& actors) {
        if (actors.empty()) {
          result = caf::make_error(
            ec::logic_error,
            fmt::format("{} is not in the component registry", label));
        } else {
          TENZIR_ASSERT(actors.size() == 1);
          result = std::move(actors[0]);
        }
      },
      [&](caf::error& err) {
        result = std::move(err);
      });
  return result;
}

auto get_catalog(caf::actor_system& sys) -> caf::expected<catalog_actor> {
  auto self = caf::scoped_actor{sys};
  auto node_opt = connect_to_node(self);
  if (not node_opt) {
    return std::move(node_opt.error());
  }
  auto catalog = get_component(self, *node_opt, "catalog");
  if (not catalog) {
    return std::move(catalog.error());
  }
  return caf::actor_cast<catalog_actor>(std::move(*catalog));
}

caf::message
rebuild_start_command(const invocation& inv, caf::actor_system& sys) {
  // Create a scoped actor for interaction with the actor system and connect to
  // the node.
  auto self = caf::scoped_actor{sys};
  auto catalog = get_catalog(sys);
  if (not catalog) {
    return caf::make_message(std::move(catalog.error()));
  }
  // Parse the query expression, iff it exists.
  auto query = read_query(inv, "tenzir.rebuild.read", must_provide_query::no);
  if (not query) {
    return caf::make_message(std::move(query.error()));
  }
  auto expr = expression{};
  if (query->empty()) {
    expr = trivially_true_expression();
  } else {
    auto parsed = to<expression>(*query);
    if (not parsed) {
      return caf::make_message(std::move(parsed.error()));
    }
    expr = std::move(*parsed);
  }
  auto result = caf::message{};
  auto on_error = [&](caf::error& err) {
    result = caf::make_message(std::move(err));
  };
  self
    ->mail(atom::start_v, atom::rebuild_v,
           rebuild_options{
             .all = caf::get_or(inv.options, "tenzir.rebuild.all", false),
             .undersized
             = caf::get_or(inv.options, "tenzir.rebuild.undersized", false),
             .parallel
             = caf::get_or(inv.options, "tenzir.rebuild.parallel", size_t{1}),
             .max_partitions
             = caf::get_or(inv.options, "tenzir.rebuild.max-partitions",
                           std::numeric_limits<size_t>::max()),
             .expression = std::move(expr),
             .detached
             = caf::get_or(inv.options, "tenzir.rebuild.detached", false),
             .automatic = false,
           })
    .request(*catalog, caf::infinite)
    .receive([] { /* nop */ }, on_error);
  return result;
}

caf::message
rebuild_stop_command(const invocation& inv, caf::actor_system& sys) {
  // Create a scoped actor for interaction with the actor system and connect to
  // the node.
  auto self = caf::scoped_actor{sys};
  auto catalog = get_catalog(sys);
  if (not catalog) {
    return caf::make_message(std::move(catalog.error()));
  }
  auto result = caf::message{};
  auto on_error = [&](caf::error& err) {
    result = caf::make_message(std::move(err));
  };
  self
    ->mail(atom::stop_v, atom::rebuild_v,
           rebuild_stop_options{
             .detached
             = caf::get_or(inv.options, "tenzir.rebuild.detached", false),
           })
    .request(*catalog, caf::infinite)
    .receive([] { /* nop */ }, on_error);
  return result;
}

caf::message rebuild_show_command(const invocation&, caf::actor_system& sys) {
  // Create a scoped actor for interaction with the actor system and connect to
  // the node.
  auto self = caf::scoped_actor{sys};
  auto catalog = get_catalog(sys);
  if (not catalog) {
    return caf::make_message(std::move(catalog.error()));
  }
  auto err = caf::error{};
  self->mail(atom::status_v, status_verbosity::debug, duration::max())
    .request(caf::actor_cast<status_client_actor>(*catalog), caf::infinite)
    .receive(
      [&](const record& status) {
        auto yaml = to_yaml(status);
        if (not yaml) {
          err = std::move(yaml.error());
          return;
        }
        fmt::print("{}\n", *yaml);
      },
      [&](caf::error& error) {
        err = std::move(error);
      });
  if (err.valid()) {
    return caf::make_message(std::move(err));
  }
  return {};
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
  auto initialize(const record&, const record&) -> caf::error override {
    return caf::none;
  }

  /// Returns the unique name of the plugin.
  auto name() const -> std::string override {
    return "rebuild";
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
    rebuild->add_subcommand("show", "shows the current rebuild status",
                            command::opts("?tenzir.rebuild"));
    auto factory = command::factory{
      {"rebuild start", rebuild_start_command},
      // Make 'tenzir rebuild' an alias for 'tenzir rebuild start'.
      {"rebuild", rebuild_start_command},
      {"rebuild stop", rebuild_stop_command},
      {"rebuild show", rebuild_show_command},
    };
    return {std::move(rebuild), std::move(factory)};
  }
};

} // namespace

} // namespace tenzir::plugins::rebuild
TENZIR_REGISTER_PLUGIN(tenzir::plugins::rebuild::plugin)

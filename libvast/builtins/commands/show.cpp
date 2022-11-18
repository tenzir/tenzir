//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/concept/printable/to_string.hpp>
#include <vast/concept/printable/vast/json.hpp>
#include <vast/data.hpp>
#include <vast/logger.hpp>
#include <vast/plugin.hpp>
#include <vast/system/node_control.hpp>
#include <vast/system/spawn_or_connect_to_node.hpp>
#include <vast/taxonomies.hpp>

namespace vast::plugins::show {

namespace {

caf::message show_command(const invocation& inv, caf::actor_system& sys) {
  const auto as_yaml = caf::get_or(inv.options, "vast.show.yaml", false);
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
  // Get the type-registry actor.
  auto components
    = system::get_node_components<system::type_registry_actor>(self, node);
  if (!components)
    return caf::make_message(std::move(components.error()));
  auto [type_registry] = std::move(*components);
  // show!
  auto command_result = caf::message{caf::none};
  self->request(type_registry, caf::infinite, atom::get_v, atom::taxonomies_v)
    .receive(
      [&](taxonomies& taxonomies) mutable {
        auto result = list{};
        result.reserve(taxonomies.concepts.size());
        if (inv.full_name == "show" || inv.full_name == "show concepts") {
          for (auto& [name, concept_] : taxonomies.concepts) {
            auto fields = list{};
            fields.reserve(concept_.fields.size());
            for (auto& field : concept_.fields)
              fields.push_back(std::move(field));
            auto concepts = list{};
            concepts.reserve(concept_.concepts.size());
            for (auto& concept_ : concept_.concepts)
              concepts.push_back(std::move(concept_));
            auto entry = record{
              {"concept",
               record{
                 {"name", std::move(name)},
                 {"description", std::move(concept_.description)},
                 {"fields", std::move(fields)},
                 {"concepts", std::move(concepts)},
               }},
            };
            result.push_back(std::move(entry));
          }
        }
        if (inv.full_name == "show" || inv.full_name == "show models") {
          for (auto& [name, model] : taxonomies.models) {
            auto definition = list{};
            definition.reserve(model.definition.size());
            for (auto& definition_entry : model.definition)
              definition.push_back(std::move(definition_entry));
            auto entry = record{
              {"model",
               record{
                 {"name", std::move(name)},
                 {"description", std::move(model.description)},
                 {"definition", std::move(definition)},
               }},
            };
            result.push_back(std::move(entry));
          }
        }
        if (as_yaml) {
          if (auto yaml = to_yaml(data{std::move(result)}))
            fmt::print("{}\n", *yaml);
          else
            command_result = caf::make_message(std::move(yaml.error()));
        } else {
          if (auto json = to_json(data{std::move(result)}))
            fmt::print("{}\n", *json);
          else
            command_result = caf::make_message(std::move(json.error()));
        }
      },
      [&](caf::error& err) mutable {
        command_result = caf::make_message(
          caf::make_error(ec::unspecified, fmt::format("'show' failed to get "
                                                       "taxonomies from "
                                                       "type-registry: {}",
                                                       std::move(err))));
      });
  return command_result;
}

class plugin final : public virtual command_plugin {
public:
  plugin() = default;
  ~plugin() override = default;

  caf::error initialize(data) override {
    return caf::none;
  }

  [[nodiscard]] const char* name() const override {
    return "show";
  }

  [[nodiscard]] std::pair<std::unique_ptr<command>, command::factory>
  make_command() const override {
    auto show = std::make_unique<command>(
      "show", "print configuration objects as JSON",
      command::opts("?vast.show").add<bool>("yaml", "format output as YAML"));
    show->add_subcommand("concepts", "print all registered concept definitions",
                         command::opts("?vast.show.concepts"));
    show->add_subcommand("models", "print all registered model definitions",
                         command::opts("?vast.show.models"));
    auto factory = command::factory{
      {"show", show_command},
      {"show concepts", show_command},
      {"show models", show_command},
    };
    return {std::move(show), std::move(factory)};
  };
};

} // namespace

} // namespace vast::plugins::show

VAST_REGISTER_PLUGIN(vast::plugins::show::plugin)

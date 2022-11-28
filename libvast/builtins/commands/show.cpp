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
#include <vast/query_context.hpp>
#include <vast/system/catalog.hpp>
#include <vast/system/node_control.hpp>
#include <vast/system/spawn_or_connect_to_node.hpp>
#include <vast/taxonomies.hpp>
#include <vast/uuid.hpp>

namespace vast::plugins::show {

namespace {

caf::error print_definition(const data& definition, bool as_yaml) {
  if (as_yaml) {
    if (auto yaml = to_yaml(definition))
      fmt::print("{}\n", *yaml);
    else
      return std::move(yaml.error());
  } else {
    if (auto json = to_json(definition))
      fmt::print("{}\n", *json);
    else
      return std::move(json.error());
  }
  return caf::none;
}

list to_definition(const concepts_map& concepts) {
  auto result = list{};
  result.reserve(concepts.size());
  for (const auto& [name, concept_] : concepts) {
    auto fields = list{};
    fields.reserve(concept_.fields.size());
    for (const auto& field : concept_.fields)
      fields.push_back(field);
    auto nested_concepts = list{};
    nested_concepts.reserve(concept_.concepts.size());
    for (const auto& concept_ : concept_.concepts)
      nested_concepts.push_back(concept_);
    auto entry = record{
      {"concept",
       record{
         {"name", name},
         {"description", concept_.description},
         {"fields", std::move(fields)},
         {"concepts", std::move(nested_concepts)},
       }},
    };
    result.push_back(std::move(entry));
  }
  return result;
}

list to_definition(const models_map& models) {
  auto result = list{};
  result.reserve(models.size());
  for (const auto& [name, model] : models) {
    auto definition = list{};
    definition.reserve(model.definition.size());
    for (const auto& definition_entry : model.definition)
      definition.push_back(definition_entry);
    auto entry = record{
      {"model",
       record{
         {"name", name},
         {"description", model.description},
         {"definition", std::move(definition)},
       }},
    };
    result.push_back(std::move(entry));
  }
  return result;
}

list to_definition(const type_set& types) {
  auto result = list{};
  result.reserve(types.size());
  for (const auto& type : types)
    result.push_back(type.to_definition());
  return result;
}

caf::message show_command(const invocation& inv, caf::actor_system& sys) {
  const auto as_yaml = caf::get_or(inv.options, "vast.show.yaml", false);
  const auto show_concepts
    = inv.full_name == "show" || inv.full_name == "show concepts";
  const auto show_models
    = inv.full_name == "show" || inv.full_name == "show models";
  const auto show_schemas
    = inv.full_name == "show" || inv.full_name == "show schemas";
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
  // Get the catalog actor.
  auto components
    = system::get_node_components<system::catalog_actor>(self, node);
  if (!components)
    return caf::make_message(std::move(components.error()));
  auto [catalog] = std::move(*components);
  // show!
  auto command_result = caf::expected<list>{list{}};
  if (show_concepts || show_models) {
    self->request(catalog, caf::infinite, atom::get_v, atom::taxonomies_v)
      .receive(
        [&](const taxonomies& taxonomies) mutable {
          if (show_concepts) {
            auto concepts_definition = to_definition(taxonomies.concepts);
            command_result->insert(command_result->end(),
                                   concepts_definition.begin(),
                                   concepts_definition.end());
          }
          if (show_models) {
            auto models_definition = to_definition(taxonomies.models);
            command_result->insert(command_result->end(),
                                   models_definition.begin(),
                                   models_definition.end());
          }
        },
        [&](caf::error& err) mutable {
          command_result = caf::make_error(ec::unspecified,
                                           fmt::format("'show' failed to get "
                                                       "taxonomies from "
                                                       "catalog: {}",
                                                       std::move(err)));
        });
  }
  if (show_schemas && !command_result.error()) {
    auto catch_all_query = expression{predicate{
      meta_extractor{meta_extractor::type},
      relational_operator::not_equal,
      data{""},
    }};
    auto query_context
      = query_context::make_extract("show", self, std::move(catch_all_query));
    query_context.id = uuid::random();
    self
      ->request(catalog, caf::infinite, atom::candidates_v,
                std::move(query_context))
      .receive(
        [&](const system::catalog_result& catalog_result) {
          auto types = type_set{};
          for (const auto& partition : catalog_result.partitions) {
            if (partition.schema)
              types.insert(partition.schema);
          }
          auto types_definition = to_definition(types);
          command_result->insert(command_result->end(),
                                 types_definition.begin(),
                                 types_definition.end());
        },
        [&](caf::error& err) mutable {
          command_result = caf::make_error(ec::unspecified,
                                           fmt::format("'show' failed to get "
                                                       "types from catalog: {}",
                                                       std::move(err)));
        });
  }
  if (!command_result)
    return caf::make_message(std::move(command_result.error()));
  if (auto err = print_definition(*command_result, as_yaml))
    return caf::make_message(std::move(err));
  return caf::none;
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
                         show->options);
    show->add_subcommand("models", "print all registered model definitions",
                         show->options);
    show->add_subcommand("schemas", "print all registered schemas",
                         show->options);
    auto factory = command::factory{
      {"show", show_command},
      {"show concepts", show_command},
      {"show models", show_command},
      {"show schemas", show_command},
    };
    return {std::move(show), std::move(factory)};
  };
};

} // namespace

} // namespace vast::plugins::show

VAST_REGISTER_PLUGIN(vast::plugins::show::plugin)

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

#include <caf/blocking_actor.hpp>

#include <csignal>

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

bool matches_filter(std::string_view name, std::string_view filter) {
  if (filter.empty())
    return true;
  const auto [name_mismatch, filter_mismatch]
    = std::mismatch(name.begin(), name.end(), filter.begin(), filter.end());
  const auto filter_consumed = filter_mismatch == filter.end();
  if (!filter_consumed)
    return false;
  const auto name_consumed = name_mismatch == name.end();
  if (name_consumed)
    return true;
  return *name_mismatch == '.';
}

list to_definition(const concepts_map& concepts, std::string_view filter) {
  auto result = list{};
  result.reserve(concepts.size());
  for (const auto& [name, concept_] : concepts) {
    if (!matches_filter(name, filter))
      continue;
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

list to_definition(const models_map& models, std::string_view filter) {
  auto result = list{};
  result.reserve(models.size());
  for (const auto& [name, model] : models) {
    if (!matches_filter(name, filter))
      continue;
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

list to_definition(const type_set& types, std::string_view filter,
                   bool expand) {
  auto result = list{};
  result.reserve(types.size());
  for (const auto& type : types) {
    if (!matches_filter(type.name(), filter))
      continue;
    result.push_back(type.to_definition(expand));
  }
  return result;
}

caf::message show_command(const invocation& inv, caf::actor_system& sys) {
  if (inv.arguments.size() > 1)
    return caf::make_message(caf::make_error(
      ec::invalid_argument, "show command expects at most one argument"));
  const auto filter
    = inv.arguments.empty() ? std::string_view{} : inv.arguments[0];
  const auto expand = caf::get_or(inv.options, "vast.show.expand", false);
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
    self->send(catalog, atom::get_v, atom::taxonomies_v);
    self->receive(
      [&](const taxonomies& taxonomies) mutable {
        if (show_concepts) {
          auto concepts_definition = to_definition(taxonomies.concepts, filter);
          command_result->insert(command_result->end(),
                                 concepts_definition.begin(),
                                 concepts_definition.end());
        }
        if (show_models) {
          auto models_definition = to_definition(taxonomies.models, filter);
          command_result->insert(command_result->end(),
                                 models_definition.begin(),
                                 models_definition.end());
        }
      },
      [&](caf::error& err) mutable {
        command_result
          = caf::make_error(ec::unspecified, fmt::format("'show' failed to get "
                                                         "taxonomies from "
                                                         "catalog: {}",
                                                         std::move(err)));
      },
      [&](atom::signal, int signal) {
        VAST_DEBUG("{} received signal {}", __PRETTY_FUNCTION__,
                   ::strsignal(signal));
        VAST_ASSERT(signal == SIGINT || signal == SIGTERM);
      });
  }
  if (show_schemas && !command_result.error()) {
    auto catch_all_query = expression{negation{expression{}}};
    auto query_context
      = query_context::make_extract("show", self, std::move(catch_all_query));
    query_context.id = uuid::random();
    self->send(catalog, atom::candidates_v, std::move(query_context));
    self->receive(
      [&](const system::catalog_lookup_result& catalog_result) {
        auto types = type_set{};
        for (const auto& [type, partitions] : catalog_result.candidate_infos) {
          for (const auto& partition_info : partitions.partition_infos) {
            if (partition_info.schema)
              types.insert(partition_info.schema);
          }
        }
        auto types_definition = to_definition(types, filter, expand);
        command_result->insert(command_result->end(), types_definition.begin(),
                               types_definition.end());
      },
      [&](caf::error& err) mutable {
        command_result = caf::make_error(ec::unspecified,
                                         fmt::format("'show' failed to get "
                                                     "types from catalog: {}",
                                                     std::move(err)));
      },
      [&](atom::signal, int signal) {
        VAST_DEBUG("{} received signal {}", __PRETTY_FUNCTION__,
                   ::strsignal(signal));
        VAST_ASSERT(signal == SIGINT || signal == SIGTERM);
      });
  }
  if (!command_result)
    return caf::make_message(std::move(command_result.error()));
  if (auto err = print_definition(*command_result, as_yaml))
    return caf::make_message(std::move(err));
  return {};
}

class plugin final : public virtual command_plugin {
public:
  plugin() = default;
  ~plugin() override = default;

  caf::error initialize(data) override {
    return caf::none;
  }

  [[nodiscard]] std::string name() const override {
    return "show";
  }

  [[nodiscard]] std::pair<std::unique_ptr<command>, command::factory>
  make_command() const override {
    auto show = std::make_unique<command>(
      "show", "print configuration objects as JSON",
      command::opts("?vast.show")
        .add<bool>("expand",
                   "use long-form notiation in output where applicable")
        .add<bool>("yaml", "format output as YAML"));
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

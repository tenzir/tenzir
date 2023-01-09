//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/make_pipelines.hpp"

#include "vast/concept/convertible/to.hpp"
#include "vast/detail/settings.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/plugin.hpp"
#include "vast/si_literals.hpp"
#include "vast/system/node.hpp"
#include "vast/system/spawn_arguments.hpp"
#include "vast/system/transformer.hpp"
#include "vast/table_slice.hpp"

#include <caf/actor.hpp>
#include <caf/actor_cast.hpp>
#include <caf/config_value.hpp>
#include <caf/expected.hpp>
#include <caf/local_actor.hpp>
#include <caf/settings.hpp>
#include <caf/typed_event_based_actor.hpp>

using namespace vast::binary_byte_literals;

namespace vast::system {

// An example of a pipeline with two steps:
//
// remove_action:
//   - delete:
//       field: alert.action
//   - replace:
//       field: dns.rrname
//       value: "foobar.net"
//
caf::error parse_pipeline_operators(pipeline& pipeline,
                                    const caf::config_value::list& operators) {
  for (auto config_operator : operators) {
    auto* dict = caf::get_if<caf::config_value::dictionary>(&config_operator);
    if (!dict)
      return caf::make_error(ec::invalid_configuration, "operator is not a "
                                                        "dict");
    if (dict->size() != 1)
      return caf::make_error(ec::invalid_configuration, "operator has more "
                                                        "than 1 entry");
    auto& [name, value] = *dict->begin();
    auto* opts = caf::get_if<caf::config_value::dictionary>(&value);
    if (!opts)
      return caf::make_error(ec::invalid_configuration,
                             "expected pipeline operator configuration to be a "
                             "dict");
    auto rec = to<record>(*opts);
    if (!rec)
      return rec.error();
    auto pipeline_operator = make_pipeline_operator(name, *rec);
    if (!pipeline_operator)
      return pipeline_operator.error();
    pipeline.add_operator(std::move(*pipeline_operator));
  }
  return caf::none;
}

caf::expected<std::vector<pipeline>>
make_pipelines(pipelines_location location, const caf::settings& settings) {
  std::vector<pipeline> result;
  std::string key;
  bool server = true;
  switch (location) {
    case pipelines_location::server_import:
      key = "vast.pipeline-triggers.import";
      server = true;
      break;
    case pipelines_location::server_export:
      key = "vast.pipeline-triggers.export";
      server = true;
      break;
    case pipelines_location::client_sink:
      key = "vast.pipeline-triggers.export";
      server = false;
      break;
    case pipelines_location::client_source:
      key = "vast.pipeline-triggers.import";
      server = false;
      break;
  }
  auto pipelines_list = caf::get_if<caf::config_value::list>(&settings, key);
  if (!pipelines_list) {
    // TODO: Distinguish between the case where no pipelines were specified
    // (= return) and where there is something other than a list (= error).
    VAST_DEBUG("unable to find transformations for key {}", key);
    return result;
  }
  // (name, [event_type]), ...
  std::vector<std::pair<std::string, std::vector<std::string>>>
    pipeline_triggers;
  for (auto list_item : *pipelines_list) {
    auto* pipeline = caf::get_if<caf::config_value::dictionary>(&list_item);
    if (!pipeline)
      return caf::make_error(ec::invalid_configuration, "pipeline definition "
                                                        "must be dict");
    if (pipeline->find("location") == pipeline->end())
      return caf::make_error(ec::invalid_configuration,
                             "missing 'location' key for pipeline trigger");
    // For backwards compatibility, we also support 'transform' as a key to
    // specify the pipeline.
    auto pipeline_key = std::string{"pipeline"};
    if (pipeline->find("pipeline") == pipeline->end()) {
      if (pipeline->find("transform") != pipeline->end())
        pipeline_key = "transform";
      else
        return caf::make_error(ec::invalid_configuration,
                               "missing 'pipeline' key for pipeline trigger");
    }
    if (pipeline->find("events") == pipeline->end())
      return caf::make_error(ec::invalid_configuration, "missing 'events' key "
                                                        "for pipeline trigger");
    auto* location = caf::get_if<std::string>(&(*pipeline)["location"]);
    if (!location || (*location != "server" && *location != "client"))
      return caf::make_error(ec::invalid_configuration, "pipeline location "
                                                        "must be either "
                                                        "'server' or 'client'");
    auto* name = caf::get_if<std::string>(&(*pipeline)[pipeline_key]);
    if (!name)
      return caf::make_error(ec::invalid_configuration, "pipeline name must "
                                                        "be a string");
    auto events = detail::unpack_config_list_to_vector<std::string>(
      (*pipeline)["events"]);
    if (!events) {
      VAST_ERROR("Events extraction from pipeline config failed");
      return events.error();
    }
    auto server_pipeline = *location == "server";
    if (server != server_pipeline)
      continue;
    pipeline_triggers.emplace_back(*name, *events);
  }
  if (pipeline_triggers.empty()) {
    return result;
  }
  result.reserve(pipeline_triggers.size());
  auto pipeline_definitions
    = caf::get_if<caf::config_value::dictionary>(&settings, "vast.pipelines");
  if (!pipeline_definitions) {
    return caf::make_error(ec::invalid_configuration, "invalid");
  }
  std::map<std::string, caf::config_value::list> pipelines;
  for (auto [name, value] : *pipeline_definitions) {
    auto* pipeline_operators = caf::get_if<caf::config_value::list>(&value);
    if (!pipeline_operators) {
      return caf::make_error(ec::invalid_configuration,
                             "could not interpret pipeline operators as list");
    }
    pipelines[name] = *pipeline_operators;
  }
  for (auto [name, event_types] : pipeline_triggers) {
    if (!pipelines.contains(name)) {
      return caf::make_error(ec::invalid_configuration,
                             fmt::format("unknown pipeline '{}'", name));
    }
    auto& pipeline = result.emplace_back(name, std::move(event_types));
    if (auto err = parse_pipeline_operators(pipeline, pipelines.at(name)))
      return err;
  }
  return result;
}

caf::expected<pipeline_ptr>
make_pipeline(const std::string& name,
              const std::vector<std::string>& event_types,
              const caf::settings& pipelines) {
  if (!pipelines.contains(name))
    return caf::make_error(ec::invalid_configuration,
                           fmt::format("unknown pipeline '{}'", name));
  auto pipeline = std::make_shared<vast::pipeline>(
    name, std::vector<std::string>{event_types});
  auto list = caf::get_if<caf::config_value::list>(&pipelines, name);
  if (!list)
    return caf::make_error(
      ec::invalid_configuration,
      fmt::format("expected a list of steps in pipeline '{}'", name));
  if (auto err = parse_pipeline_operators(*pipeline, *list))
    return err;
  return pipeline;
}

caf::expected<std::vector<std::unique_ptr<pipeline_operator>>>
make_pipeline(std::string_view pipeline_string) {
  std::vector<std::unique_ptr<pipeline_operator>> pipeline;
  auto pipeline_str_it
    = pipeline_string.substr(0, pipeline_string.find_first_of(' '));
  while (pipeline_str_it != pipeline_string.end()) {
    auto pipeline_op_parse_result
      = parse_pipeline_operator(pipeline_str_it, pipeline_string);
    if (pipeline_op_parse_result->second.error()) {
      return pipeline_op_parse_result->second.error();
    }
    pipeline.emplace_back(std::move(*pipeline_op_parse_result->second));
    pipeline_str_it = pipeline_op_parse_result->first;
  }
  return pipeline;
}

} // namespace vast::system

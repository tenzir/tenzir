//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/system/actors.hpp"
#include "vast/system/transformer.hpp"

#include <caf/expected.hpp>
#include <caf/typed_actor.hpp>

#include <vector>

namespace vast::system {

enum class pipelines_location {
  server_import,
  server_export,
  client_source,
  client_sink,
};

/// Validates the passed `settings` and creates the set of pipelines
/// that were configured for the desired location.
/// @param location Selects which part of the config file should be parsed.
/// @param settings The VAST settings objects including transform configuration.
caf::expected<std::vector<pipeline>>
make_pipelines(pipelines_location location, const caf::settings& settings);

/// Validates the passed `caf::settings` and creates the named pipeline defined
/// in the passed pipelines configuration.
/// @param name The name of the created pipeline
/// @param event_types The event types to be operated on by the pipeline. An
///        empty vector implies all possible event types.
/// @param pipelines The converted YAML configuration containing the pipeline
/// definitions.
caf::expected<pipeline_ptr>
make_pipeline(const std::string& name,
              const std::vector<std::string>& event_types,
              const caf::settings& pipelines);

/// Parse and validate a series of pipeline operators, and add them to the given
/// pipeline.
caf::error parse_pipeline_operators(pipeline& pipeline,
                                    const caf::config_value::list& operators);
caf::expected<std::vector<std::unique_ptr<pipeline_operator>>>
make_pipeline(std::string_view pipeline_string);

enum class parsing_mode {
  NONE,
  EXTRACTOR,
  SHORT_OPTION_KEY,
  LONG_OPTION_KEY,
  EXTRACTOR_ASSIGNMENT,
  SHORT_OPTION_ASSIGNMENT,
  LONG_OPTION_ASSIGNMENT,
  EXTRACTOR_VALUE,
  SHORT_OPTION_VALUE,
  LONG_OPTION_VALUE
};

struct pipeline_parsing_result {
  vast::list extractors;
  vast::list assignments;
  record long_form_options;
  record short_form_options;
  std::string_view::iterator new_str_it;
  caf::error parse_error;
};

pipeline_parsing_result parse_pipeline(std::string_view str);

} // namespace vast::system

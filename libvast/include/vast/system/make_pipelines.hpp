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

/// Validates the passed `caf::settings` and creates the transform of the given
/// name.
caf::expected<pipeline_ptr>
make_pipeline(const std::string& name,
              const std::vector<std::string>& event_types,
              const caf::settings& pipelines);

} // namespace vast::system

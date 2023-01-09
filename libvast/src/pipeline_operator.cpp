//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/pipeline_operator.hpp"

#include "vast/error.hpp"
#include "vast/plugin.hpp"

#include <fmt/format.h>

namespace vast {

// TODO: It would be more consistent with the rest of the code base to have a
// `pipeline_operator_factory` to create the steps. All pipeline operators from
// plugins would be registered at startup. However, that will require some more
// refactoring since `plugins::get()` only gives us unique pointers, so we can't
// really store the plugin anywhere to later create an operator from it.
caf::expected<std::unique_ptr<pipeline_operator>>
make_pipeline_operator(const std::string& name, const vast::record& options) {
  for (const auto& plugin : plugins::get()) {
    if (name != plugin->name())
      continue;
    const auto* t = plugin.as<pipeline_operator_plugin>();
    if (!t)
      return caf::make_error(ec::invalid_configuration,
                             fmt::format("pipeline operator '{}' does not "
                                         "refer to a pipeline plugin",
                                         name));
    return t->make_pipeline_operator(options);
  }
  return caf::make_error(ec::invalid_configuration,
                         fmt::format("unknown pipeline operator '{}'", name));
}

caf::expected<std::pair<std::string_view::iterator,
                        caf::expected<std::unique_ptr<pipeline_operator>>>>
parse_pipeline_operator(std::string_view name, std::string_view str) {
  if (auto plugin = plugins::find<pipeline_operator_plugin>(name)) {
    return plugin->parse_pipeline_string(str);
  }
  return caf::make_error(ec::invalid_configuration,
                         fmt::format("unknown pipeline operator '{}'", name));
}

} // namespace vast

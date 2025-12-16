//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/version.hpp"

#include "tenzir/concept/printable/tenzir/data.hpp"
#include "tenzir/concept/printable/to_string.hpp"
#include "tenzir/config.hpp"
#include "tenzir/data.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/plugin.hpp"

#include <arrow/util/config.h>

#include <iostream>
#include <sstream>

namespace tenzir {

record retrieve_versions(const record& cfg) {
  record result;
  result["Tenzir"] = version::version;
  result["Build Configuration"] = record{
    {"Type", version::build::type},
    {"Tree Hash", version::build::tree_hash},
    {"Assertions", version::build::has_assertions},
    {"Address Sanitizer", version::build::has_address_sanitizer},
    {"Undefined Behavior Sanitizer",
     version::build::has_undefined_behavior_sanitizer},
  };
  std::ostringstream caf_version;
  caf_version << CAF_MAJOR_VERSION << '.' << CAF_MINOR_VERSION << '.'
              << CAF_PATCH_VERSION;
  result["CAF"] = caf_version.str();
  std::ostringstream arrow_version;
  arrow_version << ARROW_VERSION_MAJOR << '.' << ARROW_VERSION_MINOR << '.'
                << ARROW_VERSION_PATCH;
  result["Apache Arrow"] = arrow_version.str();
  list plugin_names;
  for (const auto& plugin : plugins::get()) {
    if (plugin.type() == plugin_ptr::type::builtin) {
      continue;
    }
    if (auto v = plugin.version()) {
      plugin_names.push_back(fmt::format("{}-{}", plugin->name(), v));
    } else {
      plugin_names.push_back(fmt::format("{}", plugin->name()));
    }
  }
  result["plugins"] = std::move(plugin_names);
  list features;
  for (auto feature : tenzir_features(cfg)) {
    features.push_back(feature);
  }
  result["features"] = features;
  return result;
}

auto tenzir_features(const record& cfg) -> std::vector<std::string> {
  TENZIR_UNUSED(cfg);
  // A list of features that are supported by this version of the node. This is
  // intended to support the rollout of potentially breaking new features, so
  // that downstream API consumers can adjust their behavior depending on the
  // capabilities of the node. We remove entries once they're stabilized in the
  // Tenzir Platform.
  auto result = std::vector<std::string>{
    // The node supports passing the `--limit` flag to the TQL1 `chart` operator
    "chart_limit",
    // The node supports modules in TQL2. Alongside this a few operators were
    // renamed, e.g., `package_add` was renamed to `package::add`.
    "modules",
    // The node supports the TQL2 `from` and `to` operators.
    "tql2_from",
    // Schema definitions use the new format that represents Tenzir's type
    // system exactly.
    "exact_schema",
    // TQL2-only mode is enabled.
    "tql2_only",
    // The `/serve-multi` is supported
    "serve-multi",
    // High resolution pipeline activity info is available
    "hr-pipeline-activity",
  };
  return result;
}

} // namespace tenzir

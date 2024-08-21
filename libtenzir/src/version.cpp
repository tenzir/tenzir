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

#if TENZIR_ENABLE_JEMALLOC
#  include <jemalloc/jemalloc.h>
#endif

#include <iostream>
#include <sstream>

namespace tenzir {

record retrieve_versions() {
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
#if TENZIR_ENABLE_JEMALLOC
  result["jemalloc"] = JEMALLOC_VERSION;
#else
  result["jemalloc"] = data{};
#endif
  list plugin_names;
  for (const auto& plugin : plugins::get()) {
    if (plugin.type() == plugin_ptr::type::builtin)
      continue;
    if (auto v = plugin.version())
      plugin_names.push_back(fmt::format("{}-{}", plugin->name(), v));
    else
      plugin_names.push_back(fmt::format("{}", plugin->name()));
  }
  result["plugins"] = std::move(plugin_names);
  list features;
  for (auto feature : tenzir_features())
    features.push_back(feature);
  result["features"] = features;
  return result;
}

auto tenzir_features() -> std::vector<std::string> {
  // A list of features that are supported by this version of the node.
  // This is intended to support the rollout of potentially breaking new
  // features, so that downstream API consumers can adjust their behavior
  // depending on the capabilities of the node.
  return {
    // The platform plugin understands the `alternate_payload_destination` field
    // and can send out-of-band responses.
    "large_responses",
    // The pipeline manager plugin has a new `/pipeline/launch` endpoint and can
    // thus unify the workflows of running & deploying pipelines.
    "launch_endpoint",
    // The pipeline manager plugin supports pipeline labels that can be modified
    // using the `/pipeline/update` endpoint.
    "pipeline_labels",
    // The pipeline manager can autostart and autodelete pipelines, as per the
    // user's requests.
    "extended_pipeline_actions",
    // The `chart` operator is supported, so that the frontend can conditionally
    // render results as charts.
    "chart_operator",
    // The `/serve` endpoint supports the new `use_simple_format: bool` option.
    "serve_use_simple_format_option",
    // The node supports HTTP PUT upload to a platform-provided URL for
    // downloading Explorer results.
    "http_put_upload",
    // The node supports a customizable pipeline retry delay.
    "retry_delay",
    // The pipeline manager supports editable definitions.
    "editable_definitions",
    // The export, diagnostics, and metrics oeprators support combining live and
    // retro exports.
    "export_live_and_retro",
    // Pipelines can be unstoppable - they can not be paused or stopped manually,
    // and run & repeat indefinitely.
    "unstoppable",
    // There is a `packages` operator can display package information in an
    // extended format.
    "extended_package_format",
  };
}

} // namespace tenzir

//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/actors.hpp"
#include "tenzir/import_stream.hpp"
#include "tenzir/plugin.hpp"

#include <caf/typed_event_based_actor.hpp>

namespace tenzir {

/// Periodically run all registered health metrics
class healthmetrics_collector_state {
public:
  // -- constants --------------------------------------------------------------

  static inline constexpr auto name = "healthmetrics_collector";

  // -- constructors, destructors, and assignment operators --------------------

  healthmetrics_collector_state() = default;

  using healthcheck = std::function<record()>;

  // list of health checks to run
  std::unordered_map<std::string, typename health_metrics_plugin::collector>
    collectors;

  // time between checks
  caf::timespan collection_interval = std::chrono::seconds{30};

  // output stream
  std::unique_ptr<import_stream> importer = nullptr;
};

/// Spawn a HEALTHCHECKER actor. Periodically queries `index` and erases all
/// hits from `archive`.
/// @param interval The time between two query executions.
/// @param index A handle to the INDEX.
auto healthmetrics_collector(
  healthmetrics_collector_actor::stateful_pointer<healthmetrics_collector_state>
    self,
  caf::timespan collection_interval, const node_actor& node)
  -> healthmetrics_collector_actor::behavior_type;

} // namespace tenzir

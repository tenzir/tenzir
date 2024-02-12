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

/// Periodically collects all registered metrics.
class metrics_collector_state {
public:
  // -- constants --------------------------------------------------------------

  static inline constexpr auto name = "metrics_collector";

  // -- constructors, destructors, and assignment operators --------------------

  metrics_collector_state() = default;

  // -------- member functions -------------------------------------------------

  auto collect_and_import_metrics() -> void;

  // -------- data members -----------------------------------------------------

  metrics_collector_actor::pointer self;

  // A handle to the node actor.
  node_actor node;

  // List of health checks to run
  using collectors_map
    = std::unordered_map<std::string, typename metrics_plugin::collector>;
  collectors_map collectors;

  // Time to wait between checks.
  caf::timespan collection_interval = std::chrono::seconds{60};

  // The output stream for writing metrics events.
  importer_actor importer;
};

/// Spawn a HEALTHCHECKER actor. Periodically queries `index` and erases all
/// hits from `archive`.
/// @param interval The time between two query executions.
/// @param index A handle to the INDEX.
auto metrics_collector(
  metrics_collector_actor::stateful_pointer<metrics_collector_state> self,
  caf::timespan collection_interval, const node_actor& node)
  -> metrics_collector_actor::behavior_type;

} // namespace tenzir

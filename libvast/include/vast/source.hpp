//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/actors.hpp"
#include "vast/expression.hpp"
#include "vast/format/reader.hpp"
#include "vast/instrumentation.hpp"
#include "vast/module.hpp"
#include "vast/report.hpp"
#include "vast/status.hpp"

#include <caf/broadcast_downstream_manager.hpp>
#include <caf/stream_source.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <optional>
#include <unordered_map>

namespace vast {

/// The source state.
/// @tparam Reader The reader type, which must model the *Reader* concept.
struct source_state {
  // -- constructor ------------------------------------------------------------

  source_state() = default;

  // -- member types -----------------------------------------------------------

  using downstream_manager = caf::broadcast_downstream_manager<table_slice>;

  // -- member variables -------------------------------------------------------

  /// A pointer to the parent actor handle.
  caf::scheduled_actor* self = {};

  /// Filters events, i.e., causes the source to drop all matching events.
  caf::optional<expression> filter = {};

  /// Maps types to the tailored filter.
  std::unordered_map<type, expression> checkers = {};

  /// Actor for collecting statistics.
  accountant_actor accountant = {};

  /// The `source` only supports a single sink, so we track here if we
  /// already got it.
  bool has_sink = {};

  /// Wraps the format-specific parser.
  format::reader_ptr reader = {};

  /// Pretty name for log files.
  const char* name = "source";

  /// Takes care of transmitting batches.
  caf::stream_source_ptr<downstream_manager> mgr = {};

  /// An accumulator for the amount of produced events.
  size_t count = 0;

  /// The maximum number of events to ingest.
  std::optional<size_t> requested = {};

  /// The import-local module.
  vast::module local_module = {};

  /// The maximum size for a table slice.
  size_t table_slice_size = {};

  /// Current metrics for the accountant.
  measurement metrics = {};

  /// Per-event counters for the accountant.
  std::unordered_map<std::string, uint64_t> event_counters = {};

  /// The amount of time to wait until the next wakeup.
  std::chrono::milliseconds wakeup_delay = std::chrono::milliseconds::zero();

  /// Indicates whether the stream source is waiting for input.
  bool waiting_for_input = false;

  /// Indicates whether the stream source is done.
  bool done = false;

  // -- utility functions -----------------------------------------------------

  /// Initializes the state.
  void initialize(const catalog_actor& catalog, std::string type_filter);

  void send_report();

  void filter_and_push(table_slice slice,
                       const std::function<void(table_slice)>& push_to_out);
};

/// An event producer.
/// @tparam Reader The concrete source implementation.
/// @param self The actor handle.
/// @param reader The reader instance.
/// @param table_slice_size The maximum size for a table slice.
/// @param max_events The optional maximum amount of events to import.
/// @param catalog The actor handle for the catalog component.
/// @param local_module Additional local schemas to consider.
/// @param type_filter Restriction for considered types.
/// @param accountant_actor The actor handle for the accountant component.
caf::behavior
source(caf::stateful_actor<source_state>* self, format::reader_ptr reader,
       size_t table_slice_size, std::optional<size_t> max_events,
       const catalog_actor& catalog, vast::module local_module,
       std::string type_filter, accountant_actor accountant);

} // namespace vast

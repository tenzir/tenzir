//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/aliases.hpp"
#include "vast/expression.hpp"
#include "vast/legacy_pipeline.hpp"
#include "vast/query_context.hpp"
#include "vast/query_options.hpp"
#include "vast/system/actors.hpp"
#include "vast/system/query_status.hpp"
#include "vast/table_slice.hpp"
#include "vast/uuid.hpp"

#include <caf/actor.hpp>
#include <caf/broadcast_downstream_manager.hpp>
#include <caf/scheduled_actor.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <queue>

namespace vast::system {

struct exporter_state {
  /// -- constructor -----------------------------------------------------------

  exporter_state() = default;

  /// -- constants -------------------------------------------------------------

  static inline const char* name = "exporter";

  // -- member variables -------------------------------------------------------

  /// Stores the query.
  struct query_context query_context = {};

  /// Stores a handle to the INDEX for querying results.
  index_actor index = {};

  /// Stores a handle to the SINK that processes results.
  caf::actor sink = {};

  /// Stores a handle to the STATISTICS_SUBSCRIBER that periodically prints
  /// statistics.
  caf::actor statistics_subscriber = {};

  /// Stores a handle to the ACCOUNTANT that collects various statistics.
  accountant_actor accountant = {};

  /// Caches tailored candidate checkers.
  std::unordered_map<type, std::optional<expression>> checkers = {};

  /// Stores the time point for when this actor got started via 'run'.
  std::chrono::system_clock::time_point start = {};

  /// Stores various meta information about the progress we made on the query.
  struct query_status query_status = {};

  /// Stores flags for the query for distinguishing historic and continuous
  /// queries.
  query_options options = {};

  /// Stores the query ID we receive from the INDEX.
  uuid id = {};

  /// Used to send table slices to `sink` in a streaming manner.
  caf::stream_source_ptr<caf::broadcast_downstream_manager<table_slice>>
    result_stream = {};

  /// Provides events to the source of the pipeline.
  std::deque<table_slice> source_buffer = {};

  /// Stores the events that arrive at the sink of the pipeline.
  std::deque<table_slice> sink_buffer = {};

  /// The executor for the pipeline of this exporter.
  generator<caf::expected<void>> executor = {};

  /// The textual representation of this pipeline.
  std::string pipeline_str = {};
};

/// The EXPORTER gradually requests more results from the index until no more
/// results are available or the requested number of events is reached.
/// It also performs a candidate check to filter out false positives.
/// @param self The actor handle of the exporter.
/// @param options The query options.
/// @param pipe The applied pipeline.
/// @param index The index actor.
auto exporter(exporter_actor::stateful_pointer<exporter_state> self,
              query_options options, pipeline pipe, index_actor index)
  -> exporter_actor::behavior_type;

} // namespace vast::system

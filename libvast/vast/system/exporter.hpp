/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include "vast/aliases.hpp"
#include "vast/expression.hpp"
#include "vast/ids.hpp"
#include "vast/query_options.hpp"
#include "vast/status.hpp"
#include "vast/system/accountant.hpp"
#include "vast/system/archive.hpp"
#include "vast/system/query_status.hpp"
#include "vast/uuid.hpp"

#include <chrono>
#include <deque>
#include <memory>
#include <unordered_map>

namespace vast::system {

struct exporter_state {
  /// -- constants -------------------------------------------------------------

  static inline const char* name = "exporter";

  // -- properties -------------------------------------------------------------

  caf::settings status(status_verbosity v);

  // -- member variables -------------------------------------------------------

  /// Stores a handle to the ARCHIVE for fetching candidates.
  archive_type archive;

  /// Stores a handle to the INDEX for querying results.
  caf::actor index;

  /// Stores a handle to the SINK that processes results.
  caf::actor sink;

  /// Stores a handle to the STATISTICS_SUBSCRIBER that periodically prints
  /// statistics.
  caf::actor statistics_subscriber;

  /// Stores a handle to the ACCOUNTANT that collects various statistics.
  accountant_type accountant;

  /// Stores hits from the INDEX.
  ids hits;

  /// Caches tailored candidate checkers.
  std::unordered_map<type, expression> checkers;

  /// Caches results for the SINK.
  std::vector<table_slice_ptr> results;

  /// Stores the time point for when this actor got started via 'run'.
  std::chrono::system_clock::time_point start;

  /// Stores various meta information about the progress we made on the query.
  query_status query;

  /// Stores flags for the query for distinguishing historic and continuous
  /// queries.
  query_options options;

  /// Stores the query ID we receive from the INDEX.
  uuid id;

  /// Stores the user-defined export query.
  expression expr;
};

/// The EXPORTER receives index hits, looks up the corresponding events in the
/// archive, and performs a candidate check to select the resulting stream of
/// matching events.
/// @param self The actor handle.
/// @param ast The AST of query.
/// @param qos The query options.
caf::behavior exporter(caf::stateful_actor<exporter_state>* self,
                       expression expr, query_options opts);

} // namespace vast::system

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

#include <chrono>
#include <deque>
#include <memory>
#include <unordered_map>

#include "vast/aliases.hpp"
#include "vast/event.hpp"
#include "vast/expression.hpp"
#include "vast/ids.hpp"
#include "vast/query_options.hpp"
#include "vast/uuid.hpp"

#include "vast/system/accountant.hpp"
#include "vast/system/archive.hpp"
#include "vast/system/query_status.hpp"

namespace vast::system {

struct exporter_state {
  caf::settings status();

  archive_type archive;
  caf::actor index;
  caf::actor sink;
  accountant_type accountant;
  ids hits;
  std::unordered_map<type, expression> checkers;
  std::deque<event> candidates;
  std::vector<event> results;
  std::chrono::steady_clock::time_point start;
  query_status query;
  query_options options;
  uuid id;
  expression expr;
  static inline const char* name = "exporter";
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

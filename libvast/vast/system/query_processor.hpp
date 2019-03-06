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

#include <array>
#include <cstddef>
#include <cstdint>

#include <caf/behavior.hpp>
#include <caf/fwd.hpp>

#include "vast/fwd.hpp"
#include "vast/uuid.hpp"

namespace vast::system {

/// A query processor takes (1) a query, (2) a processing step function,
/// and (3) a predicate for implementing the following state machine.
///
/// ```
///                    +----------------+
///                    |                |
///               +--->+      idle      |
///               |    |                |
///               |    +-------+--------+
///               |            |
///               |            | (run)
///               |            v
///               |    +-------+--------+
///               |    |                |
///               |    | await query id |
///               |    |                |
///               |    +-------+--------+
///               |            |
///               |            | (query_id, scheduled, total)
///               |            |
///               |            |      +------+
///               |            |      |      |
///               |            v      v      | (ids)
///               |    +-------+------+-+    |
///               |    |                +----+
///               |    |  collect hits  |
///               |    |                +<---+
///               |    +-------+--------+    |
///               |            |             |
///               |            | (done)      |
///               |            v             |
///               |       XXXXXXXXXXXX       |
///               |      XX request  XX      |
///               +----+XX    more    XX+----+
///                no    XX   hits?  XX   yes
///                       XXXXXXXXXXXX
/// ```

class query_processor {
public:
  // -- member types -----------------------------------------------------------

  enum state_name {
    idle,
    await_query_id,
    collect_hits,
  };

  static constexpr size_t num_states = 3;

  // -- constants --------------------------------------------------------------

  /// Human-readable actor name for logging output.
  static inline constexpr const char* name = "query-processor";

  // -- constructors, destructors, and assignment operators --------------------

  /// @warning Calls `set_default_handler(caf::skip)`.
  query_processor(caf::event_based_actor* self);

  virtual ~query_processor();

  // -- convenience functions --------------------------------------------------

  /// Sends the query `expr` to `index_hdl` and transitions from `idle` to
  /// `await_query_id`.
  /// @pre `state() == idle`
  void start(expression expr, caf::actor index_hdl);

  /// @pre `state() == collect_hits`
  /// @pre `n > 0`
  /// @pre `partitions_.received + n <= partitions_.total`
  void request_more_hits(uint32_t n);

  // -- properties -------------------------------------------------------------

  /// @returns the current state.
  state_name state() {
    return state_;
  }

  /// @returns the current behavior.
  caf::behavior& behavior() {
    return behaviors_[state_];
  }

  /// @returns the behavior for state `x`.
  caf::behavior& behavior(state_name x) {
    return behaviors_[x];
  }

protected:
  // -- state management -------------------------------------------------------

  virtual void transition_to(state_name x);

  // -- implementation hooks ---------------------------------------------------

  /// Processes incoming hits from the INDEX.
  virtual void process_hits(const ids& hits);

  /// Processes incoming done messages. The default implementation always
  /// transitions to the idle state.
  virtual void process_end_of_hits();

  // -- member variables -------------------------------------------------------

  /// Stores the name of the current state.
  state_name state_;

  /// Stores a behavior for each named state.
  std::array<caf::behavior, num_states> behaviors_;

  /// Points to the actor that runs this FSM.
  caf::event_based_actor* self_;

  /// Our query ID for collecting more hits.
  uuid query_id_;

  /// Our INDEX for querying and collecting more hits.
  caf::actor index_;

  /// Keeps track of how many partitions were processed.
  struct {
    uint32_t received;
    uint32_t scheduled;
    uint32_t total;
  } partitions_;
};

// -- related functions --------------------------------------------------------

std::string to_string(query_processor::state_name x);

} // namespace vast::system

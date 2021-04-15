//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/ids.hpp"
#include "vast/system/actors.hpp"
#include "vast/system/query_processor.hpp"

#include <string>

namespace vast::system {

/// Periodically queries the INDEX with a configurable expression and erases
/// all hits from the ARCHIVE.
class eraser_state : public query_processor {
public:
  // -- member types -----------------------------------------------------------

  using super = system::query_processor;

  // -- constants --------------------------------------------------------------

  static inline constexpr auto name = "eraser";

  // -- constructors, destructors, and assignment operators --------------------

  eraser_state(caf::event_based_actor* self);

  void init(caf::timespan interval, std::string query, index_actor index);

protected:
  // -- implementation hooks ---------------------------------------------------

  void transition_to(state_name x) override;

private:
  // -- member variables -------------------------------------------------------

  /// Configures the time between two query executions.
  caf::timespan interval_;

  /// The query expression that selects events scheduled for deletion. Note
  /// that we get the query as string on purpose. Taking an ::expression here
  /// instead would fix any query such as `#time < 1 week ago` to the time of
  /// its parsing and not update properly.
  std::string query_;

  /// Collects hits until all deltas arrived.
  ids hits_;

  /// Keeps track whether we were triggered remotely and need to send a
  /// confirmation message and suppress the delayed message.
  caf::response_promise promise_;
};

/// Periodically queries `index` and erases all hits from `archive`.
/// @param interval The time between two query executions.
/// @param query The periodic query that selects events scheduled for deletion.
///              Note that we get the query as string on purpose. Taking an
///              ::expression here instead would fix any query such as `#time <
///              1 week ago` to the time of its parsing and not update
///              properly.
/// @param index A handle to the INDEX under investigation.
caf::behavior
eraser(caf::stateful_actor<eraser_state>* self, caf::timespan interval,
       std::string query, index_actor index);

} // namespace vast::system

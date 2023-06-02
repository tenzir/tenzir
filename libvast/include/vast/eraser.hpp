//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/actors.hpp"
#include "vast/ids.hpp"
#include "vast/index.hpp"

#include <caf/settings.hpp>
#include <caf/typed_response_promise.hpp>

#include <string>

namespace vast {

/// Periodically queries the INDEX with a configurable expression and erases
/// all hits from relevant partitions.
class eraser_state {
public:
  // -- constants --------------------------------------------------------------

  static inline constexpr auto name = "eraser";

  // -- constructors, destructors, and assignment operators --------------------

  eraser_state() = default;

  // -- implementation hooks ---------------------------------------------------

  [[nodiscard]] record status(status_verbosity) const;

  // -- member variables -------------------------------------------------------
  index_actor index_ = {};

  /// Configures the time between two query executions.
  caf::timespan interval_ = {};

  /// The query expression that selects events scheduled for deletion. Note
  /// that we get the query as string on purpose. Taking an ::expression here
  /// instead would fix any query such as `:timestamp < 1 week ago` to the time
  /// of its parsing and not update properly.
  std::string query_;

  /// Collects hits until all deltas arrived.
  ids hits_;

  /// Keeps track whether we were triggered remotely and need to send a
  /// confirmation message and suppress the delayed message.
  caf::typed_response_promise<atom::ok> promise_ = {};
};

/// Periodically queries `index` and erases all hits from `archive`.
/// @param interval The time between two query executions.
/// @param query The periodic query that selects events scheduled for deletion.
///              Note that we get the query as string on purpose. Taking an
///              ::expression here instead would fix any query such as
///              `:timestamp < 1 week ago` to the time of its parsing and not
///              update properly.
/// @param index A handle to the INDEX under investigation.
/// @param pipelines The pipeline config of the node.
eraser_actor::behavior_type
eraser(eraser_actor::stateful_pointer<eraser_state> self,
       caf::timespan interval, std::string query, index_actor index);

} // namespace vast

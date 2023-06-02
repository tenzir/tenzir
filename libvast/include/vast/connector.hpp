//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/actors.hpp"

#include <caf/io/middleman_actor.hpp>
#include <caf/timespan.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <chrono>
#include <optional>

namespace vast {

struct connector_state {
  // Actor responsible for TCP connection with a remote node.
  caf::io::middleman_actor middleman;
};

/// @brief Creates an actor that establishes the connection to a remote VAST
/// node.
/// @param retry_delay Delay between two connection attempts. Don't retry if not
/// set.
/// @param deadline Time point after which the connector can no longer connect
/// to a remote VAST node. Try connecting until success if not set.
/// @return actor handle that can be used to connect with a remote VAST node.
connector_actor::behavior_type
connector(connector_actor::stateful_pointer<connector_state> self,
          std::optional<caf::timespan> retry_delay,
          std::optional<std::chrono::steady_clock::time_point> deadline);

} // namespace vast

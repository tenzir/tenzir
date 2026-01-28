//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/actors.hpp"

#include <caf/io/middleman_actor.hpp>
#include <caf/timespan.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <chrono>
#include <optional>

namespace tenzir {

struct connector_state {
  static constexpr auto name = "connector";

  // Actor responsible for TCP connection with a remote node.
  caf::io::middleman_actor middleman;
};

/// @brief Creates an actor that establishes the connection to a remote Tenzir
/// node.
/// @param retry_delay Delay between two connection attempts. Don't retry if not
/// set.
/// @param deadline Time point after which the connector can no longer connect
/// to a remote Tenzir node. Try connecting until success if not set.
/// @return actor handle that can be used to connect with a remote Tenzir node.
connector_actor::behavior_type
connector(connector_actor::stateful_pointer<connector_state> self,
          std::optional<caf::timespan> retry_delay,
          std::optional<std::chrono::steady_clock::time_point> deadline,
          bool internal_connection = false);

} // namespace tenzir

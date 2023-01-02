//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <vast/atoms.hpp>
#include <vast/fwd.hpp>

#include <caf/typed_actor.hpp>

namespace vast::plugins::tui {

using ui_actor = caf::typed_actor<
  /// Receive a log message.
  caf::reacts_to<std::string>,
  /// Receives a table slice.
  caf::reacts_to<table_slice>,
  /// Create a query for a given pipeline ID, expression, and list of node IDs.
  caf::reacts_to<atom::query, uuid, std::string, std::vector<std::string>>,
  /// Connect to a node.
  caf::reacts_to<atom::connect, caf::settings>,
  /// Kick off the UI main loop.
  caf::reacts_to<atom::run>>;

/// Spawns the UI actor.
ui_actor spawn_ui(caf::actor_system& system);

} // namespace vast::plugins::tui

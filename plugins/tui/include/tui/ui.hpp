//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/atoms.hpp"
#include <memory>

#include <caf/typed_actor.hpp>

namespace vast::plugins::tui {

using ui_actor = caf::typed_actor<
  /// Receive a log message.
  caf::reacts_to<std::string>,
  /// Kick off the UI main loop.
  caf::reacts_to<atom::run>>;

/// Spawns the UI actor.
ui_actor spawn_ui(caf::actor_system& system);

} // namespace vast::plugins::tui

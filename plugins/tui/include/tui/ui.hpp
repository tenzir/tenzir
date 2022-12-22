//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tui/tui.hpp"

#include <vast/atoms.hpp>

#include <caf/typed_event_based_actor.hpp>

#include <vector>

namespace vast::plugins::tui {

using ui_actor = caf::typed_actor<
  // Initiates the main UI loop.
  caf::reacts_to<atom::run>>;

/// @relates ui
struct ui_state {
  ui_state(ui_actor::stateful_pointer<ui_state> self);

  /// Hook ourselves into the system-wide logger.
  void hook_logger();

  /// Render loop and wait forever.
  void loop();

  struct tui tui;

  /// Points to the parent actor.
  ui_actor::pointer self;

  /// Gives this actor a recognizable name in logging output.
  static inline const char* name = "ui";
};

/// Renders the UI.
ui_actor::behavior_type ui(ui_actor::stateful_pointer<ui_state> self);

} // namespace vast::plugins::tui

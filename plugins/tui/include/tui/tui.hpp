//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <memory>
#include <vector>

namespace vast::plugins::tui {

/// @relates tui
struct tui_state;

/// The terminal UI implementation.
class tui {
public:
  /// Initialize the UI.
  tui();

  /// Run the UI main loop in a dedicated thread.
  void loop();

  /// Adds a log line.
  void add_log(std::string line);

  /// Triggers a redraw of the screen.
  void redraw();

private:
  std::shared_ptr<tui_state> state_;
};

} // namespace vast::plugins::tui

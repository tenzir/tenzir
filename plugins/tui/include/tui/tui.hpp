//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <ftxui/component/screen_interactive.hpp>
#include <ftxui/dom/elements.hpp>

#pragma once

namespace vast::plugins::tui {

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
  ftxui::ScreenInteractive screen_;
  std::vector<ftxui::Element> logs_;
};

} // namespace vast::plugins::tui

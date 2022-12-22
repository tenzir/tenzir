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
struct tui {
  /// Initialize the UI.
  tui();

  /// Run forever.
  void loop();

  std::vector<ftxui::Element> logs;
  ftxui::ScreenInteractive screen;
};

} // namespace vast::plugins::tui

//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "explore/ui_state.hpp"

#include "explore/operator_args.hpp"
#include "explore/printer_args.hpp"

namespace tenzir::plugins::explore {

/// Construct the global UI state from the plugin configuration.
auto make_ui_state(const operator_args& args) -> ui_state {
  auto result = ui_state{};
  if (args.navigator_position) {
    if (args.navigator_position->inner == "left")
      result.navigator_position = ftxui::Direction::Left;
    else if (args.navigator_position->inner == "right")
      result.navigator_position = ftxui::Direction::Right;
    else if (args.navigator_position->inner == "top")
      result.navigator_position = ftxui::Direction::Up;
    else if (args.navigator_position->inner == "bottom")
      result.navigator_position = ftxui::Direction::Down;
  }
  if (args.navigator_auto_hide)
    result.navigator_auto_hide = true;
  if (args.hide_types)
    result.hide_types = true;
  return result;
}

auto make_ui_state(const printer_args& args) -> ui_state {
  auto result = ui_state{};
  if (args.hide_types)
    result.hide_types = true;
  return result;
}

} // namespace tenzir::plugins::explore

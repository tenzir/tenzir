//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <ftxui/component/component_options.hpp>
#include <ftxui/dom/elements.hpp>
#include <ftxui/dom/table.hpp>
#include <ftxui/screen/color.hpp>

namespace vast::plugins::tui {

/// The theme colors.
struct palette {
  // General
  ftxui::Color text;
  ftxui::Color subtext;
  ftxui::Color subsubtext;
  ftxui::Color subtle;
  ftxui::Color link_normal;
  ftxui::Color link_followed;
  ftxui::Color link_hover;
  ftxui::Color success;
  ftxui::Color error;
  ftxui::Color warning;
  ftxui::Color info;
  // Window
  ftxui::Color cursor;
  ftxui::Color cursor_text;
  ftxui::Color border_active;
  ftxui::Color border_inactive;
  ftxui::Color border_bell;
  // Colors
  ftxui::Color color0;
  ftxui::Color color1;
  ftxui::Color color2;
  ftxui::Color color3;
  ftxui::Color color4;
  ftxui::Color color5;
  ftxui::Color color6;
  ftxui::Color color7;
  ftxui::Color color8;
  ftxui::Color color9;
  ftxui::Color color10;
  ftxui::Color color11;
  ftxui::Color color12;
  ftxui::Color color13;
  ftxui::Color color14;
  ftxui::Color color15;
  ftxui::Color color16;
  ftxui::Color color17;
  // Language
  ftxui::Color keyword;
  ftxui::Color string;
  ftxui::Color escape;
  ftxui::Color comment;
  ftxui::Color number;
  ftxui::Color operator_;
  ftxui::Color brace;
  ftxui::Color function;
  ftxui::Color parameter;
  ftxui::Color builtin;
  ftxui::Color type;
};

/// Application-wide color and style settings.
struct theme {
  /// A themed FTXUI menu option.
  auto menu_option(ftxui::Direction direction) const -> ftxui::MenuOption;

  /// A themed FTXUI border.
  auto border() const -> ftxui::Decorator;

  /// Returns a themed separator.
  auto separator() const -> ftxui::Element;

  struct palette palette;
};

/// The default theme if the user doesn't adjust one.
const theme default_theme = theme{};

} // namespace vast::plugins::tui

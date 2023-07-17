//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <ftxui/component/component_options.hpp>
#include <ftxui/dom/elements.hpp>
#include <ftxui/dom/table.hpp>
#include <ftxui/screen/color.hpp>

namespace tenzir::plugins::explore {

/// The theme colors.
/// See https://rosepinetheme.com/palette/ for semantics.
struct palette {
  // Backgrounds
  ftxui::Color base;
  ftxui::Color surface;
  ftxui::Color overlay;
  // Foregrounds
  ftxui::Color muted;
  ftxui::Color subtle;
  ftxui::Color text;
  // Colors
  ftxui::Color love;
  ftxui::Color gold;
  ftxui::Color rose;
  ftxui::Color pine;
  ftxui::Color foam;
  ftxui::Color iris;
  // Highlights
  ftxui::Color highlight_low;
  ftxui::Color highlight_med;
  ftxui::Color highlight_high;
};

/// Constructs the default palette.
auto default_palette() -> palette;

/// Application-wide color and style settings.
struct theme {
  /// A themed FTXUI menu option.
  auto menu_option(ftxui::Direction direction) const -> ftxui::MenuOption;

  /// A themed FTXUI border.
  auto border() const -> ftxui::Decorator;

  /// The focus color.
  auto focus_color() const -> ftxui::Decorator;

  /// Returns a themed separator.
  auto separator() const -> ftxui::Element;

  struct palette palette = default_palette();
};

/// Constructs the default scheme.
auto default_theme() -> theme;

} // namespace tenzir::plugins::explore

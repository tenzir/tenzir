//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <vast/view.hpp>

#include <ftxui/component/component_options.hpp>
#include <ftxui/dom/elements.hpp>
#include <ftxui/dom/table.hpp>
#include <ftxui/screen/color.hpp>

namespace vast::plugins::tui {

/// The state of an FTXUI component.
/// @relates theme
struct component_state {
  bool focused;
  bool hovered;
  bool active;
};

/// Application-wide color and style settings.
struct theme {
  /// Varies the style to drive the user attention.
  enum class style { normal, alert };

  /// The theme colors.
  struct color_state {
    ftxui::Color primary = ftxui::Color::Cyan;
    ftxui::Color secondary = ftxui::Color::Blue;
    ftxui::Color frame = ftxui::Color::GrayDark;
    ftxui::Color focus = ftxui::Color::Green;
    ftxui::Color hover = ftxui::Color::GreenLight;
    ftxui::Color alert = ftxui::Color::Red;
  } color;

  template <style Style = style::normal>
  void transform(ftxui::Element& e, const component_state& state) const {
    using namespace ftxui;
    if constexpr (Style == style::normal) {
      if (state.hovered)
        e |= ftxui::color(color.hover);
      if (state.focused)
        e |= ftxui::color(color.focus);
      if (state.active)
        e |= bold;
      if (!state.focused && !state.active)
        e |= dim;
    } else if constexpr (Style == style::alert) {
      if (state.focused || state.hovered)
        e |= ftxui::color(color.alert);
      if (state.active)
        e |= bold;
      if (!state.focused && !state.active)
        e |= dim;
    }
  }

  /// Transforms an element according to given entry state.
  template <style Style>
  void transform(ftxui::Element& e, const ftxui::EntryState& entry) const {
    using namespace ftxui;
    if constexpr (Style == style::normal) {
      if (entry.focused)
        e |= ftxui::color(color.focus);
      if (entry.active)
        e |= ftxui::color(color.secondary) | bold;
      if (!entry.focused && !entry.active)
        e |= ftxui::color(color.secondary) | dim;
    } else if constexpr (Style == style::alert) {
      if (entry.focused)
        e |= ftxui::color(color.alert);
      if (entry.active)
        e |= ftxui::color(color.alert) | bold;
      if (!entry.focused && !entry.active)
        e |= ftxui::color(color.alert) | dim;
    }
  }

  /// Styles the first row of a table.
  /// In general, we're trying to style tables like the LaTeX booktabs package,
  /// i.e., as minimal vertical lines as possible.
  /// @param table The table to style.
  /// @pre The table must have at least one row.
  void style_table_header(ftxui::Table& table) const;

  /// Generates a ButtonOption instance.
  template <style Style = style::normal>
  [[nodiscard]] ftxui::ButtonOption button_option() const {
    using namespace ftxui;
    ButtonOption result;
    result.transform = [=](const EntryState& entry) {
      Element e
        = hbox({text(" "), text(entry.label), text(" ")}) | center | border;
      transform<Style>(e, entry);
      return e;
    };
    return result;
  }

  [[nodiscard]] ftxui::MenuOption structured_data() const;

  [[nodiscard]] ftxui::MenuOption
  navigation(ftxui::MenuOption::Direction direction
             = ftxui::MenuOption::Direction::Right) const;
};

/// The default theme if the user doesn't adjust one.
const theme default_theme = theme{};

/// Computes a color for a given piece of data for a given theme.
ftxui::Color colorize(data_view x, const struct theme& theme = default_theme);

} // namespace vast::plugins::tui

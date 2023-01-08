//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tui/theme.hpp"

#include <vast/detail/overload.hpp>

namespace vast::plugins::tui {

using namespace ftxui;

void theme::style_table_header(Table& table) const {
  auto top = table.SelectRow(0);
  top.Decorate(bold);
  top.SeparatorVertical(EMPTY);
  top.BorderBottom(LIGHT);
}

[[nodiscard]] MenuOption theme::structured_data() const {
  MenuOption result;
  result.entries.transform = [=](const EntryState& entry) {
    Element e = text(entry.label);
    transform<style::normal>(e, entry);
    return e;
  };
  return result;
}

[[nodiscard]] MenuOption
theme::navigation(MenuOption::Direction direction) const {
  using enum MenuOption::Direction;
  MenuOption result;
  result.direction = direction;
  auto horizontal = direction == Left || direction == Right;
  result.entries.transform = [=](const EntryState& entry) {
    Element e = text(entry.label);
    if (horizontal)
      e |= center;
    e |= flex;
    transform<style::normal>(e, entry);
    return e;
  };
  result.underline.enabled = horizontal;
  result.underline.SetAnimation(std::chrono::milliseconds(500),
                                animation::easing::Linear);
  result.underline.color_inactive = Color::Default;
  result.underline.color_active = color.secondary;
  return result;
}

Color colorize(data_view x, const struct theme&) {
  // TODO: compute colors based on theme.
  auto f = detail::overload{
    [](const auto&) {
      return Color::Grey50;
    },
    [](caf::none_t) {
      return Color::Grey35;
    },
    [](view<bool>) {
      return Color::DeepPink3;
    },
    [](view<integer>) {
      return Color::IndianRed1;
    },
    [](view<count>) {
      return Color::IndianRedBis;
    },
    [](view<real>) {
      return Color::IndianRed;
    },
    [](view<duration>) {
      return Color::DeepSkyBlue1;
    },
    [](view<time>) {
      return Color::DeepSkyBlue2;
    },
    [](view<std::string>) {
      return Color::Gold3Bis;
    },
    [](view<pattern>) {
      return Color::Gold1;
    },
    [](view<address>) {
      return Color::Green3;
    },
    [](view<subnet>) {
      return Color::Green4;
    },
  };
  return caf::visit(f, x);
}

} // namespace vast::plugins::tui

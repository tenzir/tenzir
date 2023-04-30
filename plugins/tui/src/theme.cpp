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
using namespace ftxui::literals;

namespace {

/// A Catppuccin color palette. See the [style
/// guide](https://github.com/catppuccin/catppuccin/blob/main/docs/style-guide.md)
/// for guidance on how to map the colors to UI elements.
struct catppuccin_palette {
  ftxui::Color rosewater;
  ftxui::Color flamingo;
  ftxui::Color pink;
  ftxui::Color mauve;
  ftxui::Color red;
  ftxui::Color maroon;
  ftxui::Color peach;
  ftxui::Color yellow;
  ftxui::Color green;
  ftxui::Color teal;
  ftxui::Color sky;
  ftxui::Color sapphire;
  ftxui::Color blue;
  ftxui::Color lavender;

  ftxui::Color text;
  ftxui::Color subtext1;
  ftxui::Color subtext0;
  ftxui::Color overlay2;
  ftxui::Color overlay1;
  ftxui::Color overlay0;
  ftxui::Color surface2;
  ftxui::Color surface1;
  ftxui::Color surface0;

  ftxui::Color base;
  ftxui::Color mantle;
  ftxui::Color crust;
};

auto latte() -> catppuccin_palette {
  catppuccin_palette result;
  result.rosewater = 0xdc8a78_rgb;
  result.flamingo = 0xdd7878_rgb;
  result.pink = 0xea76cb_rgb;
  result.mauve = 0x8839ef_rgb;
  result.red = 0xd20f39_rgb;
  result.maroon = 0xe64553_rgb;
  result.peach = 0xfe640b_rgb;
  result.yellow = 0xdf8e1d_rgb;
  result.green = 0x40a02b_rgb;
  result.teal = 0x179299_rgb;
  result.sky = 0x04a5e5_rgb;
  result.sapphire = 0x209fb5_rgb;
  result.blue = 0x1e66f5_rgb;
  result.lavender = 0x7287fd_rgb;
  result.text = 0x4c4f69_rgb;
  result.subtext1 = 0x5c5f77_rgb;
  result.subtext0 = 0x6c6f85_rgb;
  result.overlay2 = 0x7c7f93_rgb;
  result.overlay1 = 0x8c8fa1_rgb;
  result.overlay0 = 0x9ca0b0_rgb;
  result.surface2 = 0xacb0be_rgb;
  result.surface1 = 0xbcc0cc_rgb;
  result.surface0 = 0xccd0da_rgb;
  result.base = 0xeff1f5_rgb;
  result.mantle = 0xe6e9ef_rgb;
  result.crust = 0xdce0e8_rgb;
  return result;
}

auto mocha() -> catppuccin_palette {
  catppuccin_palette result;
  result.rosewater = 0xf5e0dc_rgb;
  result.flamingo = 0xf2cdcd_rgb;
  result.pink = 0xf5c2e7_rgb;
  result.mauve = 0xcba6f7_rgb;
  result.red = 0xf38ba8_rgb;
  result.maroon = 0xeba0ac_rgb;
  result.peach = 0xfab387_rgb;
  result.yellow = 0xf9e2af_rgb;
  result.green = 0xa6e3a1_rgb;
  result.teal = 0x94e2d5_rgb;
  result.sky = 0x89dceb_rgb;
  result.sapphire = 0x74c7ec_rgb;
  result.blue = 0x89b4fa_rgb;
  result.lavender = 0xb4befe_rgb;
  result.text = 0xcdd6f4_rgb;
  result.subtext1 = 0xbac2de_rgb;
  result.subtext0 = 0xa6adc8_rgb;
  result.overlay2 = 0x9399b2_rgb;
  result.overlay1 = 0x7f849c_rgb;
  result.overlay0 = 0x6c7086_rgb;
  result.surface2 = 0x585b70_rgb;
  result.surface1 = 0x45475a_rgb;
  result.surface0 = 0x313244_rgb;
  result.base = 0x1e1e2e_rgb;
  result.mantle = 0x181825_rgb;
  result.crust = 0x11111b_rgb;
  return result;
};

} // namespace

auto theme::menu_option(Direction direction) const -> MenuOption {
  using enum Direction;
  MenuOption result;
  result.direction = direction;
  auto horizontal = direction == Left || direction == Right;
  result.entries.transform = [=](const EntryState& entry) {
    Element e = text(entry.label);
    if (horizontal)
      e |= center;
    e |= flex;
    // transform<style::normal>(e, entry);
    return e;
  };
  result.underline.enabled = horizontal;
  result.underline.SetAnimation(std::chrono::milliseconds(500),
                                animation::easing::Linear);
  result.underline.color_inactive = palette.link_normal;
  result.underline.color_active = palette.link_hover;
  return result;
}

auto theme::border() const -> ftxui::Decorator {
  return borderStyled(ROUNDED, palette.border_inactive);
}

auto theme::separator() const -> ftxui::Element {
  return ftxui::separator() | ftxui::color(palette.border_inactive);
}


} // namespace vast::plugins::tui

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
namespace catppuccin::latte {

const auto rosewater = 0xdc8a78_rgb;
const auto flamingo = 0xdd7878_rgb;
const auto pink = 0xea76cb_rgb;
const auto mauve = 0x8839ef_rgb;
const auto red = 0xd20f39_rgb;
const auto maroon = 0xe64553_rgb;
const auto peach = 0xfe640b_rgb;
const auto yellow = 0xdf8e1d_rgb;
const auto green = 0x40a02b_rgb;
const auto teal = 0x179299_rgb;
const auto sky = 0x04a5e5_rgb;
const auto sapphire = 0x209fb5_rgb;
const auto blue = 0x1e66f5_rgb;
const auto lavender = 0x7287fd_rgb;
const auto text = 0x4c4f69_rgb;
const auto subtext1 = 0x5c5f77_rgb;
const auto subtext0 = 0x6c6f85_rgb;
const auto overlay2 = 0x7c7f93_rgb;
const auto overlay1 = 0x8c8fa1_rgb;
const auto overlay0 = 0x9ca0b0_rgb;
const auto surface2 = 0xacb0be_rgb;
const auto surface1 = 0xbcc0cc_rgb;
const auto surface0 = 0xccd0da_rgb;
const auto base = 0xeff1f5_rgb;
const auto mantle = 0xe6e9ef_rgb;
const auto crust = 0xdce0e8_rgb;

} // namespace catppuccin::latte

namespace catppuccin::mocha {

const auto rosewater = 0xf5e0dc_rgb;
const auto flamingo = 0xf2cdcd_rgb;
const auto pink = 0xf5c2e7_rgb;
const auto mauve = 0xcba6f7_rgb;
const auto red = 0xf38ba8_rgb;
const auto maroon = 0xeba0ac_rgb;
const auto peach = 0xfab387_rgb;
const auto yellow = 0xf9e2af_rgb;
const auto green = 0xa6e3a1_rgb;
const auto teal = 0x94e2d5_rgb;
const auto sky = 0x89dceb_rgb;
const auto sapphire = 0x74c7ec_rgb;
const auto blue = 0x89b4fa_rgb;
const auto lavender = 0xb4befe_rgb;
const auto text = 0xcdd6f4_rgb;
const auto subtext1 = 0xbac2de_rgb;
const auto subtext0 = 0xa6adc8_rgb;
const auto overlay2 = 0x9399b2_rgb;
const auto overlay1 = 0x7f849c_rgb;
const auto overlay0 = 0x6c7086_rgb;
const auto surface2 = 0x585b70_rgb;
const auto surface1 = 0x45475a_rgb;
const auto surface0 = 0x313244_rgb;
const auto base = 0x1e1e2e_rgb;
const auto mantle = 0x181825_rgb;
const auto crust = 0x11111b_rgb;

} // namespace catppuccin::mocha

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
    e |= bold;
    if (entry.focused)
      e |= focus_color();
    if (entry.active)
      e |= color(palette.link_hover);
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

auto theme::focus_color() const -> ftxui::Decorator {
  return color(palette.cursor_text) | bgcolor(palette.cursor);
}

auto theme::separator() const -> ftxui::Element {
  return ftxui::separator() | ftxui::color(palette.border_inactive);
}

auto mocha() -> theme {
  palette p;
  p.text = catppuccin::mocha::text;
  p.subtext = catppuccin::mocha::subtext0;
  p.subsubtext = catppuccin::mocha::subtext1;
  p.subtle = catppuccin::mocha::overlay1;
  p.link_normal = catppuccin::mocha::blue;
  p.link_followed = catppuccin::mocha::lavender;
  p.link_hover = catppuccin::mocha::sky;
  p.success = catppuccin::mocha::green;
  p.error = catppuccin::mocha::red;
  p.warning = catppuccin::mocha::yellow;
  p.info = catppuccin::mocha::teal;
  p.cursor = catppuccin::mocha::rosewater;
  p.cursor_text = catppuccin::mocha::crust;
  p.border_active = catppuccin::mocha::lavender;
  p.border_inactive = catppuccin::mocha::overlay0;
  p.border_bell = catppuccin::mocha::yellow;
  p.color0 = catppuccin::mocha::surface1;
  p.color1 = catppuccin::mocha::red;
  p.color2 = catppuccin::mocha::green;
  p.color3 = catppuccin::mocha::yellow;
  p.color4 = catppuccin::mocha::blue;
  p.color5 = catppuccin::mocha::pink;
  p.color6 = catppuccin::mocha::teal;
  p.color7 = catppuccin::mocha::subtext1;
  p.color8 = catppuccin::mocha::surface2;
  p.color9 = catppuccin::mocha::red;
  p.color10 = catppuccin::mocha::green;
  p.color11 = catppuccin::mocha::yellow;
  p.color12 = catppuccin::mocha::blue;
  p.color13 = catppuccin::mocha::pink;
  p.color14 = catppuccin::mocha::teal;
  p.color15 = catppuccin::mocha::subtext0;
  p.color16 = catppuccin::mocha::peach;
  p.color17 = catppuccin::mocha::rosewater;
  p.keyword = catppuccin::mocha::mauve;
  p.string = catppuccin::mocha::green;
  p.escape = catppuccin::mocha::pink;
  p.comment = catppuccin::mocha::overlay0;
  p.number = catppuccin::mocha::peach;
  p.operator_ = catppuccin::mocha::sky;
  p.delimiter = catppuccin::mocha::overlay2;
  p.function = catppuccin::mocha::blue;
  p.parameter = catppuccin::mocha::maroon;
  p.builtin = catppuccin::mocha::red;
  p.type = catppuccin::mocha::yellow;
  theme result;
  result.palette = p;
  return result;
}

} // namespace vast::plugins::tui

//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "explore/theme.hpp"

#include <tenzir/detail/overload.hpp>

#include <ftxui/component/component.hpp>

namespace tenzir::plugins::explore {

using namespace ftxui;
using namespace ftxui::literals;

namespace {

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

namespace rose_pine {

const auto base = 0x191724_rgb;
const auto surface = 0x1f1d2e_rgb;
const auto overlay = 0x26233a_rgb;
const auto muted = 0x6e6a86_rgb;
const auto subtle = 0x908caa_rgb;
const auto text = 0xe0def4_rgb;
const auto love = 0xeb6f92_rgb;
const auto gold = 0xf6c177_rgb;
const auto rose = 0xebbcba_rgb;
const auto pine = 0x31748f_rgb;
const auto foam = 0x9ccfd8_rgb;
const auto iris = 0xc4a7e7_rgb;
const auto highlight_low = 0x21202e_rgb;
const auto highlight_med = 0x403d52_rgb;
const auto highlight_high = 0x524f67_rgb;

} // namespace rose_pine

auto rose_pine_palette() -> palette {
  return {
    .base = rose_pine::base,
    .surface = rose_pine::base,
    .overlay = rose_pine::overlay,
    .muted = rose_pine::muted,
    .subtle = rose_pine::subtle,
    .text = rose_pine::text,
    .love = rose_pine::love,
    .gold = rose_pine::gold,
    .rose = rose_pine::rose,
    .pine = rose_pine::pine,
    .foam = rose_pine::foam,
    .iris = rose_pine::iris,
    .highlight_low = rose_pine::highlight_low,
    .highlight_med = rose_pine::highlight_med,
    .highlight_high = rose_pine::highlight_high,
  };
}

} // namespace

auto default_palette() -> palette {
  return rose_pine_palette();
}

auto theme::menu(std::vector<std::string>* entries, int* selected,
                 ftxui::Direction direction) const -> ftxui::Component {
  using enum Direction;
  auto menu_direction = [](Direction x) {
    switch (x) {
      case Left:
      case Right:
        return Down;
      case Up:
      case Down:
        return Left;
    }
  };
  auto horizontal = direction == Up || direction == Down;
  MenuOption option;
  option.direction = menu_direction(direction);
  option.entries_option.transform
    = [this, horizontal](const EntryState& entry) {
        Element e = text(entry.label);
        if (horizontal)
          e |= center;
        e |= flex;
        if (!entry.active)
          e |= color(palette.muted);
        if (entry.focused)
          e |= bgcolor(palette.highlight_high);
        return e;
      };
  option.underline.enabled = false;
  return Menu(entries, selected, option);
}

auto theme::focus_color() const -> ftxui::Decorator {
  return color(palette.text) | bgcolor(palette.highlight_high);
}

auto theme::separator(bool focused) const -> ftxui::Element {
  auto color = focused ? palette.highlight_high : palette.highlight_med;
  return ftxui::separator() | ftxui::color(color);
}

auto theme::border(bool focused) const -> ftxui::Decorator {
  auto color = focused ? palette.highlight_high : palette.highlight_med;
  return borderStyled(ROUNDED, color);
}

auto default_theme() -> theme {
  return {};
}

} // namespace tenzir::plugins::explore

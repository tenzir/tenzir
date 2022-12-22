//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tui/tui.hpp"

#include <ftxui/component/component.hpp>
#include <ftxui/component/screen_interactive.hpp>
#include <ftxui/dom/node.hpp>
#include <ftxui/screen/color.hpp>
#include <ftxui/screen/screen.hpp>

namespace vast::plugins::tui {

using namespace ftxui;

tui::tui() : screen{ScreenInteractive::Fullscreen()} {
  // TODO: get the logger output in the bottom split.
  // setup_logger(screen, logs);
  logs.emplace_back(text("dummy log line"));
  logs.emplace_back(text("another one"));
}

void tui::loop() {
  auto middle = Renderer([] {
    return text("middle") | center;
  });
  auto left = Renderer([] {
    return text("Left") | center;
  });
  auto right = Renderer([] {
    return text("right") | center;
  });
  auto top = Renderer([] {
    return text("top") | center;
  });
  auto bottom = Renderer([&] {
    return vbox(logs);
  });
  int left_size = 20;
  int right_size = 20;
  int top_size = 10;
  int bottom_size = 10;
  auto container = middle;
  container = ResizableSplitLeft(left, container, &left_size);
  container = ResizableSplitRight(right, container, &right_size);
  container = ResizableSplitTop(top, container, &top_size);
  container = ResizableSplitBottom(bottom, container, &bottom_size);
  auto renderer = Renderer(container, [&] {
    return container->Render() | border;
  });
  screen.Loop(renderer);
}

} // namespace vast::plugins::tui

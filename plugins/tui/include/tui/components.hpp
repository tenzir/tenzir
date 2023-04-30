//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tui/ui_state.hpp"

#include <vast/fwd.hpp>
#include <vast/view.hpp>

#include <ftxui/component/component.hpp>
#include <ftxui/dom/elements.hpp>

namespace vast::plugins::tui {

// We are adding a "deep" event catching helper here becase we are facing the
// same issue of a parent component masking the events from its children as
// reported in https://github.com/ArthurSonzogni/FTXUI/discussions/428.

enum class catch_policy {
  child,
  parent,
};

template <catch_policy Policy>
class CatchBase : public ftxui::ComponentBase {
public:
  // Constructor.
  explicit CatchBase(std::function<bool(ftxui::Event)> on_event)
    : on_event_{std::move(on_event)} {
  }

  bool OnEvent(ftxui::Event event) override {
    if constexpr (Policy == catch_policy::child)
      return ComponentBase::OnEvent(event) || on_event_(event);
    else
      return on_event_(event) || ComponentBase::OnEvent(event);
  }

protected:
  std::function<bool(ftxui::Event)> on_event_;
};

template <catch_policy Policy>
auto Catch(ftxui::Component child,
           std::function<bool(ftxui::Event event)> on_event)
  -> ftxui::Component {
  auto out = Make<CatchBase<Policy>>(std::move(on_event));
  out->Add(std::move(child));
  return out;
}

template <catch_policy Policy>
auto Catch(std::function<bool(ftxui::Event)> on_event)
  -> ftxui::ComponentDecorator {
  return [on_event = std::move(on_event)](ftxui::Component child) {
    return Catch<Policy>(std::move(child),
                         [on_event = on_event](ftxui::Event event) {
                           return on_event(std::move(event));
                         });
  };
}

/// A focusable data cell.
/// @param contents The cell contents.
/// @param color The cell color.
/// @returns The FTXUI component.
auto Cell(std::string contents, ftxui::Color color) -> ftxui::Component;

/// A focusable data cell.
/// @param x The data view to render.
/// @returns The FTXUI component.
auto Cell(view<data> x, const struct theme& theme) -> ftxui::Component;

/// The help window.
/// @returns The FTXUI component.
auto Help() -> ftxui::Component;

/// The top-level component of the application.
/// @param screen The screen to hook for UI events.
/// @param state The UI state.
/// @returns The FTXUI component.
auto MainWindow(ftxui::ScreenInteractive* screen, ui_state* state)
  -> ftxui::Component;

} // namespace vast::plugins::tui

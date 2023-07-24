//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/tui/ui_state.hpp"

#include <tenzir/fwd.hpp>
#include <tenzir/view.hpp>

#include <ftxui/component/component.hpp>
#include <ftxui/dom/elements.hpp>

namespace tenzir::tui {

/// Lifts an FTXUI element into a component.
auto lift(ftxui::Element e) -> ftxui::Component;

/// Makes a component a vertically scrollable in a frame.
auto enframe(const ftxui::Component& component) -> ftxui::Component;

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

  auto OnEvent(ftxui::Event event) -> bool override {
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

/// A major UI component in a focusable, bordered frame.
auto Pane(ui_state* state, ftxui::Component component) -> ftxui::Component;

/// A data frame.
auto DataFrame(ui_state* state, const type& schema) -> ftxui::Component;

/// The schema navigator.
auto Navigator(ui_state* state, int* index, int* width) -> ftxui::Component;

/// The Explorer.
auto Explorer(ui_state* state) -> ftxui::Component;

/// The top-level component of the application.
/// @param screen The screen to hook for UI events.
/// @param state The UI state.
/// @returns The FTXUI component.
auto MainWindow(ftxui::ScreenInteractive* screen, ui_state* state)
  -> ftxui::Component;

} // namespace tenzir::tui

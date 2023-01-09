//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tui/theme.hpp"

#include <vast/fwd.hpp>
#include <vast/view.hpp>

#include <ftxui/component/component.hpp>
#include <ftxui/dom/elements.hpp>

namespace vast::plugins::tui {

// We are adding our "deep" event catching helper here becase we are facing the
// smae issue of a parent component masking the events from its children as
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
ftxui::Component Catch(ftxui::Component child,
                       std::function<bool(ftxui::Event event)> on_event) {
  auto out = Make<CatchBase<Policy>>(std::move(on_event));
  out->Add(std::move(child));
  return out;
}

template <catch_policy Policy>
ftxui::ComponentDecorator Catch(std::function<bool(ftxui::Event)> on_event) {
  return [on_event = std::move(on_event)](ftxui::Component child) {
    return Catch<Policy>(std::move(child),
                         [on_event = on_event](ftxui::Event event) {
                           return on_event(std::move(event));
                         });
  };
}

// For our handrolled FTXUI elements and components, we slightly deviate from
// VAST naming convention. We use PascalCase for our own components, so that
// it's clear we're dealing with a component when using them alongside native
// FTXUI components.

/// A focusable component that changes its state when hovering.
// Implementation adapted from ftxui::Button.
class HoverComponent : public ftxui::ComponentBase {
public:
  explicit HoverComponent(ftxui::Element element);

  ftxui::Element Render() override;

  bool OnEvent(ftxui::Event event) override;

  bool OnMouseEvent(ftxui::Event event);

  [[nodiscard]] bool Focusable() const final;

private:
  ftxui::Element element_;
  bool mouse_hover_ = false;
  ftxui::Box box_;
};

/// Creates a HoverComponent
/// @param element The element to wrap into a hover component.
/// @returns The wrapped Element as component.
ftxui::Component Hover(ftxui::Element element);

/// A button-based dropdown that toggles a component right beneath it.
/// @param title The title of the button.
/// @param component The component to toggle on button click.
/// @returns The dropdown component.
ftxui::Component DropdownButton(std::string title, ftxui::Component component,
                                const struct theme& theme = default_theme);

/// Creates a Collapsible with a data instance.
/// @param name The top-level name for the collapsed data.
/// @param x The data instance.
/// @returns A collapsible component.
ftxui::Component Collapsible(std::string name, const data& x);

/// A focusable data cell.
/// @param contents The cell contents.
/// @param color The cell color.
/// @returns The component representing the data.
ftxui::Component
Cell(std::string contents, ftxui::Color color = ftxui::Color::Default);

/// A focusable data cell.
/// @param x The data view to render.
/// @returns The component representing the data.
ftxui::Component Cell(view<data> x);

/// A component that renders data as scrollable table.
/// @param slice The data to render.
/// @returns An interactive component that displays the table.
ftxui::Component VerticalDataView(table_slice slice);

} // namespace vast::plugins::tui

//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tui/components.hpp"

#include "tui/theme.hpp"

#include <vast/concept/printable/to_string.hpp>
#include <vast/concept/printable/vast/data.hpp>
#include <vast/detail/overload.hpp>
#include <vast/table_slice.hpp>
#include <vast/table_slice_column.hpp>

namespace vast::plugins::tui {

using namespace ftxui;

HoverComponent::HoverComponent(Element element)
  : element_{std::move(element)} {
}

Element HoverComponent::Render() {
  const bool active = Active();
  const bool focused = Focused();
  using ftxui::select;
  auto focus_management = focused ? focus : active ? select : nothing;
  auto state = component_state{
    .focused = focused,
    .hovered = mouse_hover_,
    .active = active,
  };
  auto element = element_;
  default_theme.transform(element, state);
  return element | focus_management | reflect(box_);
}

bool HoverComponent::OnEvent(Event event) {
  if (event.is_mouse())
    return OnMouseEvent(event);
  // Handle a key-board click.
  if (event == Event::Return) {
    // Click!
    return true;
  }
  return false;
}

bool HoverComponent::OnMouseEvent(Event event) {
  mouse_hover_
    = box_.Contain(event.mouse().x, event.mouse().y) && CaptureMouse(event);
  if (!mouse_hover_)
    return false;
  if (event.mouse().button == Mouse::Left
      && event.mouse().motion == Mouse::Pressed) {
    TakeFocus();
    return true;
  }
  return false;
}

[[nodiscard]] bool HoverComponent::Focusable() const {
  return true;
}

Component Hover(Element element) {
  return Make<HoverComponent>(std::move(element));
}

Component Collapsible(std::string name, const data& x) {
  auto f = detail::overload{
    [&](const auto&) {
      return Renderer([str = fmt::to_string(x)] {
        return text(str);
      });
    },
    [](const list& xs) {
      Components components;
      components.reserve(xs.size());
      for (const auto& x : xs)
        components.emplace_back(Collapsible("[...]", x));
      auto vertical = Container::Vertical(std::move(components));
      return Renderer(vertical, [vertical] {
        return hbox({
          text(" "),
          vertical->Render(),
        });
      });
    },
    [](const record& xs) {
      Components components;
      components.reserve(xs.size());
      for (const auto& [k, v] : xs)
        components.emplace_back(Collapsible(k, v));
      auto vertical = Container::Vertical(std::move(components));
      return Renderer(vertical, [vertical] {
        return hbox({
          text(" "),
          vertical->Render(),
        });
      });
    },
  };
  return Collapsible(std::move(name), caf::visit(f, x));
}

/// A focusable cell in a DataView.
Component Cell(std::string contents, Color color) {
  return Renderer([x = std::move(contents), color](bool focused) mutable {
    auto element = text(x) | ftxui::color(color);
    if (focused)
      element = element | inverted | focus;
    return element;
  });
}

/// A focusable cell in a DataView.
Component Cell(view<data> x) {
  return Cell(to_string(x), colorize(x));
}

Component VerticalDataView(table_slice slice) {
  auto table = Container::Horizontal({});
  for (size_t i = 0; i < slice.columns(); ++i) {
    auto column = Container::Vertical({});
    // Add column header.
    const auto& schema = caf::get<record_type>(slice.layout());
    auto name = schema.key(schema.resolve_flat_index(i));
    column->Add(Cell(std::move(name)));
    column->Add(Renderer([=] {
      return separatorLight() | color(default_theme.color.frame);
    }));
    // Add column data.
    auto col = table_slice_column(slice, i);
    for (size_t j = 0; j < col.size(); ++j)
      column->Add(Cell(col[j]));
    table->Add(column);
    if (i != slice.columns() - 1)
      table->Add(Renderer([=] {
        return separatorLight() | color(default_theme.color.frame);
      }));
  }
  return Renderer(table, [=] {
    return table->Render() | border | vscroll_indicator | frame;
  });
}

} // namespace vast::plugins::tui

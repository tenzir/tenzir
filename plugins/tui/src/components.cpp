//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tui/components.hpp"

#include "tui/elements.hpp"

#include <vast/detail/narrow.hpp>
#include <vast/detail/stable_set.hpp>
#include <vast/table_slice.hpp>
#include <vast/table_slice_column.hpp>

#include <ftxui/component/screen_interactive.hpp>

namespace vast::plugins::tui {

using namespace ftxui;

namespace {

/// Makes a component a vertically scrollable in a frame.
auto enframe(Component component) -> Component {
  return Renderer(component, [component] {
    return component->Render() | frame;
  });
}

/// A double-row focusable cell in the table header.
auto Help() -> Component {
  return Renderer([] {
    auto table = Table({
      {" Key ", " Alias ", " Description "},
      {"k", "↑", "move focus one window up"},
      {"j", "↓", "move focus one window down"},
      {"h", "←", "move focus one window to the left"},
      {"l", "→", "move focus one window to the right"},
      {"K", "p", "move up in schema navigator"},
      {"J", "n", "move down in schema navigator"},
      {"?", "", "show this help"},
      {"q", "", "quit the UI"},
    });
    table.SelectAll().Border(ROUNDED);
    // Set the table header apart from the rest
    table.SelectRow(0).Decorate(bold);
    table.SelectRow(0).SeparatorHorizontal(ROUNDED);
    table.SelectRow(0).Border(ROUNDED);
    // Align center the first column.
    table.SelectColumn(0).DecorateCells(center);
    table.SelectColumn(1).DecorateCells(center);
    return table.Render();
  });
}

/// A single-row focusable cell in the table header.
auto RecordHeader(std::string top, const struct theme& theme) -> Component {
  return Renderer([top_text = std::move(top),
                   top_color = color(theme.palette.text),
                   focus_color = theme.focus_color()](bool focused) mutable {
    auto header = text(top_text) | bold | center;
    return focused ? header | focus_color : header | top_color;
  });
}

auto Explorer(ui_state* state) -> Component {
  class Impl : public ComponentBase {
  public:
    Impl(ui_state* state) : state_{state} {
      auto loading = Renderer([] {
        return Vee() | center | flex;
      });
      tab_ = Container::Tab({loading}, &index_); // to be filled
      auto style = state_->theme.menu_option(Direction::Down);
      menu_ = Menu(&schemas_, &index_, style);
      auto lhs = Container::Horizontal({
        Container::Vertical({menu_, component(filler())}),
        component(text(" ")),
        fingerprints_,
      });
      auto split = ResizableSplitLeft(lhs, tab_, &menu_width_);
      Add(split);
    }

    auto Render() -> Element override {
      if (types_.size() == state_->tables.size())
        return ComponentBase::Render();
      VAST_ASSERT(types_.size() < state_->tables.size());
      // Assemble new tables and update components.
      for (auto [type, _] : state_->tables) {
        if (!types_.contains(type)) {
          if (types_.empty())
            tab_->DetachAllChildren(); // remove loading boilerplate
          auto flat_index = size_t{0};
          const auto& parent = caf::get<record_type>(type);
          auto table = enframe(assemble(type, parent, flat_index));
          tab_->Add(table);
          types_.emplace(type);
          schemas_.emplace_back(type.name());
          auto fingerprint = type.make_fingerprint();
          // One extra character for the separator.
          auto width = type.name().size() + fingerprint.size() + 1;
          menu_width_ = std::max(menu_width_, detail::narrow_cast<int>(width));
          auto element = text(std::move(fingerprint))
                         | color(state_->theme.palette.subtle);
          fingerprints_->Add(component(std::move(element)));
        }
      }
      // Reset menu width if we only have a single schema.
      if (schemas_.size() == 1)
        menu_width_ = 0;
      return ComponentBase::Render();
    }

    auto OnEvent(Event event) -> bool override {
      if (event == Event::Character('J') || event == Event::Character('n')) {
        menu_->TakeFocus();
        return menu_->OnEvent(Event::ArrowDown);
      }
      if (event == Event::Character('K') || event == Event::Character('p')) {
        menu_->TakeFocus();
        return menu_->OnEvent(Event::ArrowUp);
      }
      return ComponentBase::OnEvent(event);
    }

  private:
    /// Assembles a nested table component from the columns in the UI state.
    auto assemble(const type& schema, const record_type& parent, size_t& index)
      -> Component {
      auto result = Container::Horizontal({});
      auto first = true;
      for (auto field : parent.fields()) {
        if (first)
          first = false;
        else
          result->Add(component(state_->theme.separator()));
        if (auto nested_record = caf::get_if<record_type>(&field.type)) {
          auto column = Container::Vertical({});
          column->Add(RecordHeader(std::string{field.name}, state_->theme));
          column->Add(component(state_->theme.separator()));
          column->Add(assemble(schema, *nested_record, index));
          result->Add(column);
        } else {
          VAST_ASSERT(state_->tables.contains(schema));
          auto& table_state = state_->tables[schema];
          // Prepend rids.
          if (index == 0) {
            result->Add(table_state.rids);
            result->Add(component(state_->theme.separator()));
          }
          // Fetch leaf column from UI state.
          VAST_ASSERT(index < table_state.leaves.size());
          auto column = table_state.leaves[index++];
          result->Add(column);
        }
      }
      return Renderer(result, [result]() {
        return result->Render() | yflex_grow;
      });
    }

    ui_state* state_;

    /// The width of the navigation split.
    int menu_width_ = 0;

    /// The currently selected schema.
    int index_ = 0;

    /// The menu items for the navigator. In sync with the tab.
    std::vector<std::string> schemas_;

    /// The schema menu.
    Component menu_;

    /// The component that shows the fingerprints.
    Component fingerprints_ = Container::Vertical({});

    /// The tab component containing all table viewers. In sync with schemas.
    Component tab_;

    /// The tables by schema.
    detail::stable_set<type> types_;
  };
  return Make<Impl>(state);
}

} // namespace

auto MainWindow(ScreenInteractive* screen, ui_state* state) -> Component {
  class Impl : public ComponentBase {
  public:
    Impl(ScreenInteractive* screen, ui_state* state)
      : screen_{screen}, state_{state} {
      auto main = Explorer(state_);
      auto help = Help();
      main |= Modal(help, &show_help_);
      main |= Catch<catch_policy::child>([=](Event event) {
        if (show_help_) {
          if (event == Event::Character('q') || event == Event::Escape) {
            show_help_ = false;
            return true;
          }
        } else {
          if (event == Event::Character('q') || event == Event::Escape) {
            screen_->Exit();
            return true;
          }
          // Show help via '?'
          if (event == Event::Character('?')) {
            show_help_ = true;
            return true;
          }
        }
        return false;
      });
      Add(main);
    }

    auto Render() -> Element override {
      return ComponentBase::Render() | state_->theme.border();
    }

  private:
    ScreenInteractive* screen_;
    ui_state* state_;
    bool show_help_ = false;
  };
  return Make<Impl>(screen, state);
};

} // namespace vast::plugins::tui

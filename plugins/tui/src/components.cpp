//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tui/components.hpp"

#include "tui/elements.hpp"

#include <vast/concept/printable/to_string.hpp>
#include <vast/concept/printable/vast/data.hpp>
#include <vast/detail/narrow.hpp>
#include <vast/detail/stable_map.hpp>
#include <vast/table_slice.hpp>
#include <vast/table_slice_column.hpp>

#include <ftxui/component/screen_interactive.hpp>

namespace vast::plugins::tui {

using namespace ftxui;

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

Component HeaderCell(std::string top, std::string bottom) {
  return Renderer(
    [t = std::move(top), b = std::move(bottom)](bool focused) mutable {
      auto header = text(t) | color(Color::Green); // TODO: use theme color
      if (focused)
        header = header | inverted | focus;
      auto element = vbox({
        std::move(header),
        text(b) | color(default_theme.color.frame),
      });
      return element;
    });
}

Component Help() {
  return Renderer([] {
    auto table = Table({
      {"Key", "Description"},
      {"q", "quit the UI"},
      {"<UP>", "move focus one window up"},
      {"<DOWN>", "move focus one window down"},
      {"<LEFT>", "move focus one window to the left"},
      {"<RIGHT>", "move focus one window to the right"},
      {"?", "render this help"},
    });
    table.SelectAll().Border(LIGHT);
    // Set the table header apart from the rest
    table.SelectRow(0).Decorate(bold);
    table.SelectRow(0).SeparatorHorizontal(LIGHT);
    table.SelectRow(0).Border(LIGHT);
    // Align center the first column.
    table.SelectColumn(0).DecorateCells(center);
    return table.Render();
  });
}

Component Explorer(ui_state* state) {
  class Impl : public ComponentBase {
  public:
    Impl(ui_state* state) : state_{state} {
      tab_ = Container::Tab({}, &index_); // to be filled
      auto loading = Renderer([] {
        return Vee() | center | flex;
      });
      Add(loading);
    }

    Element Render() override {
      if (state_->data.empty())
        return ComponentBase::Render();
      // Update table components.
      for (auto&& slice : state_->data) {
        tables_[slice.schema()].append(slice);
        state_->data.clear();
        // Update surrounding UI components.
        if (tables_.size() == 1) {
          // If we only have a single schema, we don't need a schema navigation.
          if (schemas_.empty()) {
            auto& [type, table] = *tables_.begin();
            auto component = table.component();
            schemas_.push_back(std::string{type.name()});
            tab_->Add(component);
            DetachAllChildren();
            Add(component);
          }
        } else {
          VAST_ASSERT(tables_.size() > 1);
          if (schemas_.size() == 1) {
            // Rebuild UI with outer framing when we have more than 1 schema.
            DetachAllChildren();
            auto style = state_->theme.navigation(MenuOption::Direction::Down);
            auto menu = Menu(&schemas_, &index_, style);
            auto split = ResizableSplitLeft(menu, tab_, &menu_width_);
            Add(split);
          }
          if (schemas_.size() != tables_.size()) {
            // If there's a delta, we need to add new table viewers.
            for (auto& [type, table] : tables_) {
              auto width = detail::narrow_cast<int>(type.name().size());
              menu_width_ = std::max(menu_width_, width);
              // Skip schemas we already processed.
              auto name = std::string{type.name()};
              auto it = std::find(schemas_.begin(), schemas_.end(), name);
              if (it != schemas_.end())
                continue;
              schemas_.push_back(name);
              tab_->Add(table.component());
            }
          }
        }
      }
      return ComponentBase::Render();
    }

  private:
    static auto make_separator() {
      return Renderer([] {
        return separator() | color(default_theme.color.frame);
      });
    }

    /// A table component for a fixed schema that grows incrementally.
    class table_viewer {
    public:
      /// Appends a new table slice at the bottom.
      /// @param slice The slice to append at the bottom of the table.
      /// @pre `schema.slice()` is the same across all invocations.
      void append(table_slice slice) {
        // If this is the first slice, we'll assemble the header first.
        const auto& schema = caf::get<record_type>(slice.schema());
        if (columns_.empty()) {
          // Add header for row ID column.
          rids_->Add(HeaderCell(" # ", ""));
          rids_->Add(make_separator());
          table_->Add(rids_);
          table_->Add(make_separator());
          // Add header for schema columns.
          for (size_t i = 0; i < slice.columns(); ++i) {
            auto column = Container::Vertical({});
            auto offset = schema.resolve_flat_index(i);
            auto type = fmt::to_string(schema.field(offset).type);
            column->Add(HeaderCell(schema.key(offset), std::move(type)));
            column->Add(make_separator());
            columns_.push_back(column);
            table_->Add(column);
            table_->Add(make_separator());
          }
        }
        // Append table slice data.
        VAST_ASSERT(columns_.size() == slice.columns());
        for (size_t j = 0; j < slice.rows(); ++j) {
          auto rid = rids_->ChildCount() - 2; // subtract header rows
          rids_->Add(Renderer([=] {
            return text(to_string(rid)) | color(default_theme.color.frame);
          }));
        }
        for (size_t i = 0; i < slice.columns(); ++i) {
          auto column = table_slice_column(slice, i);
          for (size_t j = 0; j < column.size(); ++j)
            columns_[i]->Add(Cell(column[j]));
        }
      }

      Component component() const {
        return Renderer(table_, [=] {
          return table_->Render() | vscroll_indicator | frame;
        });
      }

    private:
      Component rids_ = Container::Vertical({});
      Components columns_;
      Component table_ = Container::Horizontal({});
    };

    ui_state* state_;

    /// The width of the navigation split.
    int menu_width_ = 25;

    /// The currently selected schema.
    int index_ = 0;

    /// The menu items for the navigator. In sync with the tab.
    std::vector<std::string> schemas_;

    /// The tab component containing all table viewers. In sync with schemas.
    Component tab_;

    /// The tables with data viewers.
    detail::stable_map<type, table_viewer> tables_;
  };
  return Make<Impl>(state);
}

Component MainWindow(ScreenInteractive* screen, ui_state* state) {
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

    Element Render() override {
      return ComponentBase::Render() | border;
    }

  private:
    ScreenInteractive* screen_;
    ui_state* state_;
    bool show_help_ = false;
  };
  return Make<Impl>(screen, state);
};

} // namespace vast::plugins::tui

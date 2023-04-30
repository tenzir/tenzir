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

namespace {

/// Lifts an FTXUI element into a component.
template <class T>
auto component(T x) -> ftxui::Component {
  return ftxui::Renderer([x = std::move(x)]() {
    return x;
  });
}

/// Makes a component a vertically scrollable in a frame.
auto enframe(Component component) -> Component {
  return Renderer(component, [component] {
    return component->Render() | vscroll_indicator | frame;
  });
}

auto render(data_view v, const struct theme& theme) -> Element {
  auto align_left = [](const auto& x) {
    return text(to_string(x));
  };
  auto align_center = [](const auto& x) {
    return hbox({filler(), text(to_string(x)), filler()});
  };
  auto align_right = [](const auto& x) {
    return hbox({filler(), text(to_string(x))});
  };
  auto f = detail::overload{
    [&](const auto& x) {
      return align_left(x) | color(theme.palette.color0);
    },
    [&](caf::none_t x) {
      return align_center("∅") | color(theme.palette.subtle);
    },
    [&](view<bool> x) {
      return align_left(x) | color(theme.palette.number);
    },
    [&](view<int64_t> x) {
      return align_right(x) | color(theme.palette.number);
    },
    [&](view<uint64_t> x) {
      return align_right(x) | color(theme.palette.number);
    },
    [&](view<double> x) {
      return align_right(x) | color(theme.palette.number);
    },
    [&](view<duration> x) {
      return align_right(x) | color(theme.palette.number);
    },
    [&](view<time> x) {
      return align_left(x) | color(theme.palette.number);
    },
    [&](view<std::string> x) {
      return align_left(x) | color(theme.palette.string);
    },
    [&](view<pattern> x) {
      return align_left(x) | color(theme.palette.string);
    },
    [&](view<ip> x) {
      return align_left(x) | color(theme.palette.string);
    },
    [&](view<subnet> x) {
      return align_left(x) | color(theme.palette.string);
    },
  };
  return caf::visit(f, v);
}

} // namespace

/// A focusable cell in a DataView.
auto Cell(view<data> x, const struct theme& theme) -> Component {
  return Renderer([element = render(x, theme)](bool focused) mutable {
    if (focused)
      return element | inverted | focus;
    return element;
  });
}

auto HeaderCell(std::string top, std::string bottom, const struct theme& theme)
  -> Component {
  return Renderer(
    [top_text = std::move(top), bottom_text = std::move(bottom),
     top_color = color(theme.palette.text),
     bottom_color = color(theme.palette.subtext)](bool focused) mutable {
      auto header = text(top_text) | center | top_color;
      if (focused)
        header = header | inverted | focus;
      auto element = vbox({
        std::move(header),
        text(bottom_text) | center | bottom_color,
      });
      return element;
    });
}

auto Help() -> Component {
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
    table.SelectAll().Border(ROUNDED);
    // Set the table header apart from the rest
    table.SelectRow(0).Decorate(bold);
    table.SelectRow(0).SeparatorHorizontal(LIGHT);
    table.SelectRow(0).Border(ROUNDED);
    // Align center the first column.
    table.SelectColumn(0).DecorateCells(center);
    return table.Render();
  });
}

auto Explorer(ui_state* state) -> Component {
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
      // Update table state with new data.
      for (auto&& slice : state_->data)
        append(slice);
      state_->data.clear();
      // Update surrounding UI components.
      if (tables_.size() == 1) {
        // If we only have a single schema, we don't need a schema navigation.
        if (schemas_.empty()) {
          auto& [type, state] = *tables_.begin();
          auto component = enframe(state.table);
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
          auto style = state_->theme.menu_option(Direction::Down);
          auto menu = Menu(&schemas_, &index_, style);
          menu_width_ = detail::narrow_cast<int>(schemas_[0].size());
          auto split = ResizableSplitLeft(menu, tab_, &menu_width_);
          Add(split);
        }
        if (schemas_.size() != tables_.size()) {
          // If there's a delta, we need to add new table viewers.
          for (auto& [type, state] : tables_) {
            auto width = detail::narrow_cast<int>(type.name().size());
            menu_width_ = std::max(menu_width_, width);
            // Skip schemas we already processed.
            auto name = std::string{type.name()};
            auto it = std::find(schemas_.begin(), schemas_.end(), name);
            if (it != schemas_.end())
              continue;
            schemas_.push_back(name);
            tab_->Add(enframe(state.table));
          }
        }
      }
      return ComponentBase::Render();
    }

  private:
    /// The append-only state per unique schema.
    struct table_state {
      Components columns = {};
      Component rids = Container::Vertical({});
      Component table = Container::Horizontal({});
    };

    auto append(table_slice slice) -> void {
      auto& tbl = tables_[slice.schema()];
      // If this is the first slice, we'll assemble the header first.
      const auto& schema = caf::get<record_type>(slice.schema());
      if (tbl.columns.empty()) {
        // Add header for row ID column.
        tbl.rids->Add(HeaderCell(" # ", "", state_->theme));
        tbl.rids->Add(component(state_->theme.separator()));
        tbl.table->Add(tbl.rids);
        // Add header for schema columns.
        for (size_t i = 0; i < slice.columns(); ++i) {
          auto column = Container::Vertical({});
          auto offset = schema.resolve_flat_index(i);
          auto type = fmt::to_string(schema.field(offset).type);
          column->Add(
            HeaderCell(schema.key(offset), std::move(type), state_->theme));
          column->Add(component(state_->theme.separator()));
          tbl.columns.push_back(column);
          tbl.table->Add(component(state_->theme.separator()));
          tbl.table->Add(column);
        }
      }
      VAST_ASSERT(tbl.columns.size() == slice.columns());
      // Append RIDs.
      for (size_t j = 0; j < slice.rows(); ++j) {
        auto rid = uint64_t{tbl.rids->ChildCount() - 2}; // subtract header rows
        tbl.rids->Add(Cell(view<data>{rid}, state_->theme));
      }
      // Append table slice data.
      for (size_t i = 0; i < slice.columns(); ++i) {
        auto column = table_slice_column(slice, i);
        for (size_t j = 0; j < column.size(); ++j)
          tbl.columns[i]->Add(Cell(column[j], state_->theme));
      }
    }

    ui_state* state_;

    /// The width of the navigation split.
    int menu_width_ = 0;

    /// The currently selected schema.
    int index_ = 0;

    /// The menu items for the navigator. In sync with the tab.
    std::vector<std::string> schemas_;

    /// The tab component containing all table viewers. In sync with schemas.
    Component tab_;

    /// The tables with data viewers.
    detail::stable_map<type, table_state> tables_;
  };
  return Make<Impl>(state);
}

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

    Element Render() override {
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

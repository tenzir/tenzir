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

Component VerticalDataView(table_slice slice, size_t max_rows) {
  auto table = Container::Horizontal({});
  auto rids = Container::Vertical({});
  rids->Add(HeaderCell("#", ""));
  rids->Add(Renderer([=] {
    return separator() | color(default_theme.color.frame);
  }));
  for (size_t i = 0; i < std::min(size_t{slice.rows()}, max_rows); ++i)
    rids->Add(Renderer([=] {
      return text(to_string(i)) | color(default_theme.color.frame);
    }));
  table->Add(std::move(rids));
  table->Add(Renderer([=] {
    return separator() | color(default_theme.color.frame);
  }));
  for (size_t i = 0; i < slice.columns(); ++i) {
    auto column = Container::Vertical({});
    // Add column header.
    const auto& schema = caf::get<record_type>(slice.schema());
    auto offset = schema.resolve_flat_index(i);
    auto bottom = fmt::to_string(schema.field(offset).type);
    column->Add(HeaderCell(schema.key(offset), std::move(bottom)));
    // Separate column header from data.
    column->Add(Renderer([=] {
      return separator() | color(default_theme.color.frame);
    }));
    // Assemble a column.
    auto col = table_slice_column(slice, i);
    for (size_t j = 0; j < std::min(col.size(), max_rows); ++j)
      column->Add(Cell(col[j]));
    // Append column to table.
    table->Add(column);
    // Separate inner columns.
    table->Add(Renderer([=] {
      return separator() | color(default_theme.color.frame);
    }));
  }
  return Renderer(table, [=] {
    return table->Render() | vscroll_indicator | frame;
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
      // Create the schema navigator.
      auto menu_style = state_->theme.navigation(MenuOption::Direction::Down);
      auto nav = Menu(&schemas_, &index_, menu_style);
      // Build the containers that the menu references.
      auto loading = Renderer([] {
        return Vee() | center | flex; // fancy spinner instead?
      });
      data_views_ = Container::Tab({loading}, &index_);
      nav = Renderer(nav, [=] {
        return vbox({
          nav->Render(),
          filler(),
        });
      });
      auto split = ResizableSplitLeft(nav, data_views_, &nav_width_);
      Add(split);
    }

    Element Render() override {
      // We only check for updates when we can expect more data to come.
      auto num_slices = state_->data.size();
      if (num_slices == slices_processed_)
        return ComponentBase::Render();
      VAST_ASSERT(num_slices > slices_processed_);
      // TODO: make rebuilding of components incremental, i.e, the amount of
      // work should be proportional to the new batches that arrive. Right
      // now, we're starting from scratch for every batch, which is quadratic
      // work.
      index_ = 0;
      schemas_.clear();
      data_views_->DetachAllChildren();
      // Left-fold all slices with the same schema name into a dataset.
      std::map<std::string, std::vector<table_slice>> dataset;
      for (const auto& slice : state_->data)
        dataset[std::string{slice.schema().name()}].push_back(slice);
      // Display the dataset by schema name & count.
      for (const auto& [schema, slices] : dataset) {
        size_t num_records = 0;
        for (const auto& slice : slices)
          num_records += slice.rows();
        // FIXME: Do not only show the first few events. We currently do that
        // because it would overwhelm the DOM rendering otherwise.
        const auto& first = slices[0];
        auto n = fmt::format(std::locale("en_US.UTF-8"), "{:L}", num_records);
        schemas_.push_back(fmt::format("{} ({})", schema, n));
        data_views_->Add(VerticalDataView(first, 100));
      }
      VAST_ASSERT(data_views_->ChildCount() == schemas_.size());
      slices_processed_ = num_slices;
      return ComponentBase::Render();
    }

  private:
    ui_state* state_;

    /// The width of the navigation split.
    int nav_width_ = 25;

    /// The number of slices already processed.
    size_t slices_processed_ = 0;

    /// The currently selected schema.
    int index_ = 0;

    /// The menu items for the navigator.
    std::vector<std::string> schemas_;

    /// The main tab.
    Component data_views_;
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

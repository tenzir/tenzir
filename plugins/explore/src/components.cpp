//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "explore/components.hpp"

#include "explore/elements.hpp"
#include "explore/ui_state.hpp"

#include <vast/collect.hpp>
#include <vast/concept/printable/to_string.hpp>
#include <vast/concept/printable/vast/data.hpp>
#include <vast/detail/narrow.hpp>
#include <vast/table_slice.hpp>
#include <vast/table_slice_column.hpp>

#include <ftxui/component/component.hpp>
#include <ftxui/component/screen_interactive.hpp>
#include <ftxui/dom/elements.hpp>

#include <unordered_set>

namespace vast::plugins::explore {

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

/// A table header component.
auto LeafHeader(std::string top, std::string bottom, int height,
                const struct theme& theme) -> Component {
  return Renderer([height, &theme, top_text = std::move(top),
                   bottom_text = std::move(bottom)](bool focused) mutable {
    auto header = text(top_text) | bold | center;
    header |= focused ? focus | theme.focus_color() : color(theme.palette.text);
    auto element = vbox({
                     filler(),
                     std::move(header),
                     text(bottom_text) | center | color(theme.palette.comment),
                     filler(),
                   })
                   | size(HEIGHT, EQUAL, height);
    return element;
  });
}

/// A cell in the table body.
auto Cell(view<data> v, const struct theme& theme) -> Component {
  enum alignment { left, center, right };
  auto make_cell = [&](const auto& x, alignment align, auto data_color) {
    auto focus_color = theme.focus_color();
    return Renderer([=, element = text(to_string(x)),
                     normal_color = color(data_color)](bool focused) {
      auto value
        = focused ? element | focus | focus_color : element | normal_color;
      switch (align) {
        case left:
          return value;
        case center:
          return hbox({filler(), std::move(value), filler()});
        case right:
          return hbox({std::move(value), filler()});
      }
    });
  };
  auto f = detail::overload{
    [&](const auto& x) {
      return make_cell(x, left, theme.palette.color0);
    },
    [&](caf::none_t) {
      return make_cell("∅", center, theme.palette.subtle);
    },
    [&](view<bool> x) {
      return make_cell(x, left, theme.palette.number);
    },
    [&](view<int64_t> x) {
      return make_cell(x, right, theme.palette.number);
    },
    [&](view<uint64_t> x) {
      return make_cell(x, right, theme.palette.number);
    },
    [&](view<double> x) {
      return make_cell(x, right, theme.palette.number);
    },
    [&](view<duration> x) {
      return make_cell(x, right, theme.palette.operator_);
    },
    [&](view<time> x) {
      return make_cell(x, left, theme.palette.operator_);
    },
    [&](view<std::string> x) {
      return make_cell(x, left, theme.palette.string);
    },
    [&](view<pattern> x) {
      return make_cell(x, left, theme.palette.string);
    },
    [&](view<ip> x) {
      return make_cell(x, left, theme.palette.type);
    },
    [&](view<subnet> x) {
      return make_cell(x, left, theme.palette.type);
    },
  };
  return caf::visit(f, v);
}

/// A leaf column consisting of header and body.
auto LeafColumn(ui_state* state, const type& schema, offset index)
  -> Component {
  class Impl : public ComponentBase {
  public:
    Impl(ui_state* state, const type& schema, offset index)
      : state_{state}, table_{state->tables[schema]}, index_{std::move(index)} {
      VAST_ASSERT(table_ != nullptr);
      VAST_ASSERT(!table_->slices.empty());
      const auto& record
        = caf::get<record_type>(table_->slices.front().schema());
      auto depth = record.depth();
      auto field = record.field(index_);
      auto height = detail::narrow_cast<int>((depth - index_.size() + 1) * 2);
      auto header
        = LeafHeader(std::string{field.name}, fmt::to_string(field.type),
                     height, state_->theme);
      auto container = Container::Vertical({});
      container->Add(header);
      container->Add(component(state_->theme.separator()));
      container->Add(body_);
      Add(container);
    }

    auto Render() -> Element override {
      if (num_slices_rendered_ == table_->slices.size())
        return ComponentBase::Render();
      for (auto i = num_slices_rendered_; i < table_->slices.size(); ++i) {
        const auto& slice = table_->slices[i];
        auto col = caf::get<record_type>(slice.schema()).flat_index(index_);
        for (size_t row = 0; row < slice.rows(); ++row)
          body_->Add(Cell(slice.at(row, col), state_->theme));
      }
      num_slices_rendered_ = table_->slices.size();
      return ComponentBase::Render();
    }

  private:
    ui_state* state_;
    table_state_ptr table_;
    offset index_;
    size_t num_slices_rendered_ = 0;
    Component body_ = Container::Vertical({});
  };
  return Make<Impl>(state, schema, index);
}

/// A single-row focusable cell in the table header.
auto RecordHeader(std::string_view top, const struct theme& theme)
  -> Component {
  return Renderer([top_text = std::string{top},
                   top_color = color(theme.palette.text),
                   focus_color = theme.focus_color()](bool focused) mutable {
    auto header = text(top_text) | bold;
    return focused ? header | focus | focus_color : header | top_color;
  });
}

/// A collapsible table column that can be a record or
auto RecordColumn(ui_state* state, std::vector<Component> columns)
  -> Component {
  VAST_ASSERT(!columns.empty());
  auto container = Container::Horizontal({});
  auto first = true;
  for (auto& column : columns) {
    if (first)
      first = false;
    else
      container->Add(component(state->theme.separator()));
    container->Add(std::move(column));
  }
  return container;
}

///// @relates traverse
// enum class traversal {
//   pre_order,
//   in_order,
//   post_order,
// };
//
///// Genereates an offset trail in a specific traversal order.
// template <traversal Traversal>
// auto traverse(const class type& type)
//   -> generator<std::pair<offset, class type>> {
//   static_assert(Traversal != traversal::in_order,
//                 "in-order traversal not well-defined on non-binary trees");
//   // Helper until C++23 std::vector::append_range is here.
//   auto result = offset{};
//   if (const auto& rec = caf::get_if<record_type>(&type)) {
//     result.push_back(0);
//     if (Traversal == traversal::pre_order)
//       co_yield {result, type};
//     for (const auto& field : rec->fields()) {
//       for (auto [i, nested] : traverse<Traversal>(field.type)) {
//         if (!i.empty()) {
//           auto copy = result;
//           copy.insert(copy.end(), i.begin(), i.end());
//           co_yield {copy, std::move(nested)};
//         }
//       }
//       ++result.back();
//     }
//     if (Traversal == traversal::post_order)
//       co_yield {result, type};
//     result.pop_back();
//   }
// }

auto make_column(ui_state* state, const type& schema, offset index = {})
  -> Component {
  auto parent = schema;
  if (!index.empty())
    parent = caf::get<record_type>(schema).field(index).type;
  auto f = detail::overload{
    [&](const auto&) {
      VAST_ASSERT(!index.empty());
      return LeafColumn(state, schema, index);
    },
    [&](const list_type&) {
      // TODO
      VAST_ASSERT(!index.empty());
      return LeafColumn(state, schema, index);
    },
    [&](const record_type& record) {
      auto column = Container::Vertical({});
      if (!index.empty()) {
        // Only show a top-level record header for nested records.
        auto field = caf::get<record_type>(schema).field(index);
        column->Add(RecordHeader(field.name, state->theme));
        column->Add(component(state->theme.separator()));
      }
      // Build columns.
      std::vector<Component> columns;
      index.push_back(0);
      columns.reserve(record.num_fields());
      for (size_t i = 0; i < record.num_fields(); ++i) {
        columns.push_back(make_column(state, schema, index));
        ++index.back();
      }
      column->Add(RecordColumn(state, std::move(columns)));
      return column;
    },
  };
  return caf::visit(f, parent);
}

auto Explorer(ui_state* state) -> Component {
  class Impl : public ComponentBase {
  public:
    Impl(ui_state* state) : state_{state} {
      // Construct menu.
      auto menu_style = state_->theme.menu_option(Direction::Down);
      menu_ = Menu(&schema_names_, &index_, std::move(menu_style));
      // Construct navigator.
      auto loading = Renderer([] {
        return Vee() | center | flex;
      });
      tab_ = Container::Tab({loading}, &index_); // to be filled
      auto navigator = Container::Horizontal({
        Container::Vertical({menu_, component(filler())}),
        component(text(" ")),
        fingerprints_,
      });
      // Construct full page.
      auto split = ResizableSplit({
        .main = std::move(navigator),
        .back = tab_,
        .direction = Direction::Left,
        .main_size = &menu_width_,
        .separator_func =
          [&] {
            return state_->theme.separator();
          },
      });
      Add(std::move(split));
    }

    auto Render() -> Element override {
      auto num_schemas = state_->tables.size();
      if (schema_cache_.size() == num_schemas)
        return ComponentBase::Render();
      // Once we get a new schema, we must add it to the navigator.
      VAST_ASSERT(schema_cache_.size() < num_schemas);
      if (schema_cache_.empty()) {
        // When we enter here for the first time, clear the boilerplate in our
        // components.
        tab_->DetachAllChildren();
        fingerprints_->DetachAllChildren();
      }
      // Assemble new tables and update components.
      for (const auto& [type, table] : state_->tables) {
        if (!schema_cache_.contains(type)) {
          schema_cache_.insert(type);
          schema_names_.emplace_back(type.name());
          tab_->Add(enframe(make_column(state_, type)));
          auto fingerprint = type.make_fingerprint();
          // One extra character for the separator.
          auto width = type.name().size() + fingerprint.size() + 1;
          menu_width_ = std::max(menu_width_, detail::narrow_cast<int>(width));
          auto element = text(std::move(fingerprint))
                         | color(state_->theme.palette.subtle);
          fingerprints_->Add(component(std::move(element)));
        }
      }
      VAST_ASSERT(num_schemas == state_->tables.size());
      VAST_ASSERT(num_schemas == schema_cache_.size());
      VAST_ASSERT(num_schemas == schema_names_.size());
      VAST_ASSERT(num_schemas == fingerprints_->ChildCount());
      VAST_ASSERT(num_schemas == tab_->ChildCount());
      // Only show the navigator when we have more than one schema.
      if (num_schemas == 1)
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
    ui_state* state_;

    /// The width of the navigation split.
    int menu_width_ = 0;

    /// The currently selected schema.
    int index_ = 0;

    /// The menu items for the navigator. In sync with the tab.
    std::vector<std::string> schema_names_;

    /// The navigator menu.
    Component menu_;

    /// The navigator component that shows the fingerprints.
    Component fingerprints_ = Container::Vertical({});

    /// The tab component containing all table viewers. In sync with schemas.
    Component tab_;

    /// The tables by schema.
    std::unordered_set<type> schema_cache_;
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
      main |= Modal(Help(), &show_help_);
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
      Add(std::move(main));
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

} // namespace vast::plugins::explore

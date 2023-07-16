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

auto lift(Element e) -> Component {
  class Impl : public ComponentBase {
  public:
    explicit Impl(Element e) : element_{std::move(e)} {
    }

    auto Render() -> Element final {
      return element_;
    }

  private:
    Element element_;
  };
  return Make<Impl>(std::move(e));
}

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

enum class alignment { left, center, right };

auto colorize(auto& make, view<data> value, const struct theme& theme) {
  auto factory = detail::overload{
    [&](const auto& x) {
      return make(x, alignment::left, theme.palette.color0);
    },
    [&](caf::none_t) {
      return make("∅", alignment::center, theme.palette.subtle);
    },
    [&](view<bool> x) {
      return make(x, alignment::left, theme.palette.number);
    },
    [&](view<int64_t> x) {
      return make(x, alignment::right, theme.palette.number);
    },
    [&](view<uint64_t> x) {
      return make(x, alignment::right, theme.palette.number);
    },
    [&](view<double> x) {
      return make(x, alignment::right, theme.palette.number);
    },
    [&](view<duration> x) {
      return make(x, alignment::right, theme.palette.operator_);
    },
    [&](view<time> x) {
      return make(x, alignment::left, theme.palette.operator_);
    },
    [&](view<std::string> x) {
      return make(x, alignment::left, theme.palette.string);
    },
    [&](view<pattern> x) {
      return make(x, alignment::left, theme.palette.string);
    },
    [&](view<ip> x) {
      return make(x, alignment::left, theme.palette.type);
    },
    [&](view<subnet> x) {
      return make(x, alignment::left, theme.palette.type);
    },
  };
  return caf::visit(factory, value);
}

auto align_element(alignment align, Element element) {
  switch (align) {
    case alignment::left:
      return element;
    case alignment::center:
      return hbox({filler(), std::move(element), filler()});
    case alignment::right:
      return hbox({std::move(element), filler()});
  }
}

auto StaticCell(view<data> value, const struct theme& theme) -> Element {
  auto make = [&](const auto& x, alignment align, auto data_color) {
    auto element = text(to_string(x)) | color(data_color);
    return align_element(align, std::move(element));
  };
  return colorize(make, value, theme);
}

/// A cell in the table body.
// auto InteractiveCell(view<data> value, const struct theme& theme) ->
// Component {
//   auto make = [&](const auto& x, alignment align, auto data_color) {
//     auto focus_color = theme.focus_color();
//     return Renderer([=, txt = text(to_string(x)),
//                      normal_color = color(data_color)](bool focused) {
//       auto element = focused ? txt | focus | focus_color : txt |
//       normal_color; return align_element(align, std::move(element));
//     });
//   };
//   return colorize(make, value, theme);
// }

/// A table header component.
auto LeafColumnHeader(std::string top, std::string bottom, int height,
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
        = LeafColumnHeader(std::string{field.name}, fmt::to_string(field.type),
                           height, state_->theme);
      auto container = Container::Vertical({});
      container->Add(header);
      container->Add(lift(state_->theme.separator()));
      container->Add(body_);
      Add(container);
    }

    auto Render() -> Element override {
      if (num_slices_rendered_ == table_->slices.size())
        return ComponentBase::Render();
      for (auto i = num_slices_rendered_; i < table_->slices.size(); ++i) {
        const auto& slice = table_->slices[i];
        auto col = caf::get<record_type>(slice.schema()).flat_index(index_);
        // TODO: make the component configurable with respect to static vs.
        // dynamic behavior. Uncommenting the cell.
        // for (size_t row = 0; row < slice.rows(); ++row) {
        //  auto cell = InteractiveCell(slice.at(row, col), state_->theme);
        //  body_->Add(cell);
        //}
        Elements body;
        body.reserve(slice.rows());
        for (size_t row = 0; row < slice.rows(); ++row) {
          auto cell = StaticCell(slice.at(row, col), state_->theme);
          body.push_back(std::move(cell));
        }
        body_->Add(lift(vbox(std::move(body))));
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
auto RecordColumnHeader(std::string top, const struct theme& theme)
  -> Component {
  return Renderer([top_text = std::move(top),
                   top_color = color(theme.palette.text),
                   focus_color = theme.focus_color()](bool focused) mutable {
    auto header = text(top_text) | bold;
    return focused ? header | focus | focus_color : header | top_color;
  });
}

/// A collapsible column for an entire record.
auto RecordColumn(ui_state* state, Components columns, std::string name = {})
  -> Component {
  class Impl : public ComponentBase {
  public:
    Impl(ui_state* state, Components columns, std::string name)
      : state_{state} {
      VAST_ASSERT(!columns.empty());
      if (!name.empty()) {
        header_ = RecordColumnHeader(std::move(name), state_->theme);
        header_ |= Catch<catch_policy::child>([=](Event event) {
          if (event == Event::Character('c')
              || event == Event::Character(' ')) {
            if (collapsed_)
              uncollapse();
            else
              collapse();
            collapsed_ = !collapsed_;
            return true;
          }
          return false;
        });
      }
      auto first = true;
      for (auto& column : columns) {
        if (first)
          first = false;
        else
          body_->Add(lift(state_->theme.separator()));
        body_->Add(std::move(column));
      }
      auto result = Container::Vertical({});
      if (header_) {
        result->Add(header_);
        result->Add(lift(state_->theme.separator()));
      }
      result->Add(body_);
      Add(result);
    }

  private:
    void uncollapse() {
      if (!header_)
        return;
      auto result = Container::Vertical({
        header_,
        lift(state_->theme.separator()),
        body_,
      });
      DetachAllChildren();
      Add(result);
    }

    void collapse() {
      if (!header_)
        return;
      auto result = Container::Vertical({
        header_,
        lift(state_->theme.separator()),
        lift(text("...") | center),
      });
      DetachAllChildren();
      Add(result);
    }

    ui_state* state_;
    bool collapsed_ = false;
    Component header_;
    Component body_ = Container::Horizontal({});
  };
  return Make<Impl>(state, std::move(columns), std::move(name));
}

/// A table that represents values of events in a column-oriented fashion.
auto VerticalTable(ui_state* state, const type& schema, offset index = {})
  -> Component {
  auto parent = schema;
  auto top_level = index.empty();
  if (!top_level)
    parent = caf::get<record_type>(schema).field(index).type;
  auto f = detail::overload{
    [&](const auto&) {
      return LeafColumn(state, schema, index);
    },
    [&](const list_type&) {
      // TODO: support lists natively.
      return LeafColumn(state, schema, index);
    },
    [&](const record_type& record) {
      std::string record_name;
      if (!top_level)
        record_name = caf::get<record_type>(schema).field(index).name;
      Components columns;
      index.push_back(0);
      columns.reserve(record.num_fields());
      for (size_t i = 0; i < record.num_fields(); ++i) {
        columns.push_back(VerticalTable(state, schema, index));
        ++index.back();
      }
      return RecordColumn(state, std::move(columns), std::move(record_name));
    },
  };
  return caf::visit(f, parent);
}

// auto FieldHeader(std::string top, std::string bottom, const struct theme&
// theme)
//   -> Component {
//   return Renderer([&theme, top_text = std::move(top),
//                    bottom_text = std::move(bottom)](bool focused) mutable {
//     auto header = text(top_text) | bold | center;
//     header |= focused ? focus | theme.focus_color() :
//     color(theme.palette.text); return vbox({
//              std::move(header),
//              text(bottom_text) | center | color(theme.palette.comment),
//            })
//            | vcenter;
//   });
// }
//
// auto HeaderCell(std::string_view name, const type& type,
//                 const struct theme& theme) -> Component {
//   return {};
// }
//
// auto HeaderCell(std::string_view name, Components fields,
//                 const struct theme& theme) -> Component {
//   return {};
// }
//
// auto TableHeader(ui_state* state, const type& schema, offset index = {})
//  -> Component {
//  auto field = record_type::field_view{};
//  if (index.empty())
//    field.type = schema;
//  else
//    field = caf::get<record_type>(schema).field(index);
//  auto f = detail::overload{
//    [&](const auto&) {
//      VAST_ASSERT(!index.empty());
//      return HeaderCell(field.name, field.type, state->theme);
//    },
//    [&](const list_type&) {
//      VAST_ASSERT(!index.empty());
//      return HeaderCell(field.name, field.type, state->theme);
//    },
//    [&](const record_type& record) {
//      auto fields = Container::Horizontal({});
//      index.push_back(0);
//      auto first = true;
//      for (size_t i = 0; i < record.num_fields(); ++i) {
//        if (first)
//          first = false;
//        else
//          fields->Add(lift(state->theme.separator()));
//        fields->Add(TableHeader(state, schema, index));
//        ++index.back();
//      }
//      return Container::Vertical({
//        HeaderCell(field.name, field.type, state->theme),
//        lift(state->theme.separator()),
//        std::move(fields),
//      });
//    },
//  };
//  return caf::visit(f, field.type);
//}
//
/// Generates a row-wise table component.
/// A table that represents values of events in a row-oriented fashion.
// auto HorizontalTable(ui_state* state, const type& schema) -> Component {
//   return {};
// }

auto Navigator(ui_state* state, int* index) -> Component {
  class Impl : public ComponentBase {
  public:
    Impl(ui_state* state, int* index) : state_{state}, index_{index} {
      fingerprints_ = Container::Vertical({});
      fingerprints_->DetachAllChildren();
      menu_ = Menu(&schema_names_, index_,
                   state_->theme.menu_option(Direction::Down));
      auto navigator = Container::Horizontal({
        Container::Vertical({menu_, lift(filler())}),
        lift(text(" ")),
        fingerprints_,
      });
      Add(std::move(navigator));
    }

    auto Render() -> Element override {
      auto num_schemas = state_->tables.size();
      if (schema_cache_.size() == num_schemas)
        return ComponentBase::Render();
      // Assemble new tables and update components.
      for (const auto& [type, table] : state_->tables) {
        if (!schema_cache_.contains(type)) {
          schema_cache_.insert(type);
          schema_names_.emplace_back(type.name());
          auto fingerprint = type.make_fingerprint();
          auto element = text(std::move(fingerprint))
                         | color(state_->theme.palette.subtle);
          fingerprints_->Add(lift(std::move(element)));
        }
      }
      VAST_ASSERT(num_schemas == state_->tables.size());
      VAST_ASSERT(num_schemas == schema_cache_.size());
      VAST_ASSERT(num_schemas == schema_names_.size());
      VAST_ASSERT(num_schemas == fingerprints_->ChildCount());
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

    /// The currently selected schema.
    int* index_ = nullptr;

    /// The menu items for the navigator. In sync with the tab.
    std::vector<std::string> schema_names_;

    /// The navigator menu.
    Component menu_;

    /// The navigator component that shows the fingerprints.
    Component fingerprints_;

    /// The tables by schema.
    std::unordered_set<type> schema_cache_;
  };
  return Make<Impl>(state, index);
}

auto Explorer(ui_state* state) -> Component {
  class Impl : public ComponentBase {
  public:
    Impl(ui_state* state) : state_{state} {
      auto navigator = Navigator(state, &index_);
      tab_ = Container::Tab({}, &index_); // to be filled
      // Construct full page.
      auto split = ResizableSplit({
        .main = std::move(navigator),
        .back = tab_,
        .direction = Direction::Left,
        .main_size = &navigator_width_,
        .separator_func =
          [&] {
            return state_->theme.separator();
          },
      });
      Add(std::move(split));
    }

    auto Render() -> Element override {
      auto num_tables = state_->tables.size();
      if (tables_.size() == num_tables)
        return ComponentBase::Render();
      if (tables_.empty())
        tab_->DetachAllChildren();
      for (const auto& [type, table] : state_->tables) {
        if (!tables_.contains(type)) {
          auto component = enframe(VerticalTable(state_, type));
          tables_.emplace(type, component);
          tab_->Add(component);
          navigator_width_ = std::max(
            navigator_width_, detail::narrow_cast<int>(type.name().size()));
        }
      }
      VAST_ASSERT(num_tables == state_->tables.size());
      VAST_ASSERT(num_tables == tables_.size());
      VAST_ASSERT(num_tables == tab_->ChildCount());
      // Only show the navigator when we have more than one schema.
      if (num_tables == 1)
        navigator_width_ = 0;
      return ComponentBase::Render();
    }

  private:
    ui_state* state_;

    /// The width of the navigation split.
    int navigator_width_ = 0;

    /// The currently selected schema.
    int index_ = 0;

    /// The tab component containing all table viewers. In sync with schemas.
    Component tab_;

    /// The tables by schema.
    std::unordered_map<type, Component> tables_;
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

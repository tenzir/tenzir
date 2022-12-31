//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tui/ui.hpp"

#include "tui/actor_sink.hpp"

#include <vast/concept/parseable/vast/json.hpp>
#include <vast/concept/printable/vast/data.hpp>
#include <vast/data.hpp>
#include <vast/defaults.hpp>
#include <vast/detail/narrow.hpp>
#include <vast/detail/stable_map.hpp>
#include <vast/logger.hpp>
#include <vast/system/actors.hpp>
#include <vast/system/connect_to_node.hpp>
#include <vast/system/node.hpp>

#include <caf/event_based_actor.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/stateful_actor.hpp>
#include <fmt/format.h>
#include <ftxui/component/component.hpp>
#include <ftxui/component/screen_interactive.hpp>
#include <ftxui/dom/elements.hpp>
#include <ftxui/dom/node.hpp>
#include <ftxui/dom/table.hpp>
#include <ftxui/screen/color.hpp>
#include <ftxui/screen/screen.hpp>

#include <chrono>

namespace vast::plugins::tui {

using namespace ftxui;
using namespace std::string_literals;
using namespace std::chrono_literals;

// Style note:
// For our handrolled FTXUI elements and components, we slightly deviate from
// our naming convention. We use PascalCase for our own custom components, so
// that their composition becomes clearer in the FTXUI context.

namespace {

/// State of the current theme.
struct theme {
  /// Varies the style to drive the user attention.
  enum class style { normal, alert };

  /// The theme colors.
  struct color_state {
    Color primary = Color::Cyan;
    Color secondary = Color::Blue;
    Color focus = Color::Green;
    Color alert = Color::Red;
  } color;

  /// Transforms an element according to given entry state.
  template <style Style>
  void transform(Element& e, const EntryState& entry) const {
    if constexpr (Style == style::normal) {
      if (entry.focused)
        e |= ftxui::color(color.focus);
      if (entry.active)
        e |= ftxui::color(color.secondary) | bold;
      if (!entry.focused && !entry.active)
        e |= ftxui::color(color.secondary) | dim;
    } else if constexpr (Style == style::alert) {
      if (entry.focused)
        e |= ftxui::color(color.alert);
      if (entry.active)
        e |= ftxui::color(color.alert) | bold;
      if (!entry.focused && !entry.active)
        e |= ftxui::color(color.alert) | dim;
    }
  }

  /// Generates a ButtonOption instance.
  template <style Style = style::normal>
  [[nodiscard]] ButtonOption button_option() const {
    ButtonOption result;
    result.transform = [=](const EntryState& entry) {
      Element e
        = hbox({text(" "), text(entry.label), text(" ")}) | center | border;
      transform<Style>(e, entry);
      return e;
    };
    return result;
  }

  [[nodiscard]] MenuOption structured_data() const {
    MenuOption result;
    result.entries.transform = [=](const EntryState& entry) {
      Element e = text(entry.label);
      transform<style::normal>(e, entry);
      return e;
    };
    return result;
  }

  [[nodiscard]] MenuOption navigation(MenuOption::Direction direction
                                      = MenuOption::Direction::Right) const {
    using enum MenuOption::Direction;
    MenuOption result;
    result.direction = direction;
    auto horizontal = direction == Left || direction == Right;
    result.entries.transform = [=](const EntryState& entry) {
      Element e = text(entry.label);
      if (horizontal)
        e |= center;
      e |= flex;
      transform<style::normal>(e, entry);
      return e;
    };
    result.underline.enabled = horizontal;
    result.underline.SetAnimation(std::chrono::milliseconds(500),
                                  animation::easing::Linear);
    result.underline.color_inactive = Color::Default;
    result.underline.color_active = color.secondary;
    return result;
  }
};

/// The FTXUI main loop is the only entity that *mutates* this state. The owning
/// entity must ensure that interaction with the contained screen is safe.
struct ui_state {
  /// The screen.
  ScreenInteractive screen = ScreenInteractive::Fullscreen();

  /// The active theme.
  struct theme theme;

  /// The messages from the logger.
  std::vector<std::string> log_messages;

  /// The state per connected VAST node.
  struct node_state {
    /// A handle to the remote node.
    system::node_actor actor;
    /// The settings to connect to the remote node.
    caf::settings opts;
    /// The last status.
    data status;
  };

  /// The list of connected nodes.
  detail::stable_map<std::string, node_state> nodes;

  /// A handle to the UI actor so that it's possible to initiate actions through
  /// user actions.
  ui_actor parent;
};

/// Creates a collapsible component from a data instance.
/// @param name The top-level name for the collapsed data.
/// @param x The data instance.
/// @returns A collapsible component.
Component make_collapsible(std::string name, const data& x) {
  auto f = detail::overload{
    [&](const auto&) {
      return Renderer([str = fmt::to_string(x)] {
        return text(str);
      });
    },
    [](const record& xs) {
      Components components;
      components.reserve(xs.size());
      for (const auto& [k, v] : xs)
        components.emplace_back(make_collapsible(k, v));
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

/// Applies consistent styling of table headers.
/// In general, we're trying to style tables like the LaTeX booktabs package,
/// i.e., as minimal vertical lines as possible.
/// @relates make_table
void apply_styling(Table& table) {
  auto top = table.SelectRow(0);
  top.Decorate(bold);
  top.SeparatorVertical(EMPTY);
  top.BorderBottom(LIGHT);
}

/// Creates a key-value table from a record. Nested records will be rendered as
/// part of the value.
/// @param key The name of the first column header.
/// @param value The name of the second column header.
/// @returns A FTXUI table.
Table make_table(std::string key, std::string value, const record& xs) {
  std::vector<std::vector<std::string>> contents;
  contents.reserve(xs.size() + 1);
  auto header = std::vector<std::string>(2);
  header[0] = std::move(key);
  header[1] = std::move(value);
  contents.push_back(std::move(header));
  for (const auto& [k, v] : xs) {
    auto row = std::vector<std::string>(2);
    row[0] = k;
    row[1] = fmt::to_string(v);
    contents.push_back(std::move(row));
  }
  auto table = Table{std::move(contents)};
  apply_styling(table);
  return table;
}

/// Creates a table that shows type statistics for all events in a VAST node.
/// @param status An instance of a status record.
/// @returns An event table
/// @relates make_table
Table make_schema_table(const data& status) {
  using row_tuple = std::tuple<std::string, uint64_t, float>;
  std::vector<row_tuple> rows;
  if (auto xs = caf::get_if<record>(&status)) {
    if (auto i = xs->find("index"); i != xs->end()) {
      if (auto ys = caf::get_if<record>(&i->second)) {
        if (auto j = ys->find("statistics"); j != ys->end()) {
          if (auto zs = caf::get_if<record>(&j->second)) {
            if (auto k = zs->find("layouts"); k != zs->end()) {
              if (auto layouts = caf::get_if<record>(&k->second)) {
                for (auto& [name, details] : *layouts) {
                  if (auto obj = caf::get_if<record>(&details)) {
                    row_tuple row;
                    std::get<0>(row) = name;
                    if (auto cnt = obj->find("count"); cnt != obj->end())
                      if (auto n = caf::get_if<integer>(&cnt->second))
                        std::get<1>(row)
                          = detail::narrow_cast<uint64_t>(n->value);
                    if (auto perc = obj->find("percentage"); perc != obj->end())
                      if (auto frac = caf::get_if<real>(&perc->second))
                        std::get<2>(row) = *frac / 100;
                    rows.push_back(std::move(row));
                  }
                }
              }
            }
          }
        }
      }
    }
  }
  // Sort by event count.
  std::sort(rows.begin(), rows.end(), [](const auto& xs, const auto& ys) {
    return std::get<1>(xs) > std::get<1>(ys);
  });
  // Render the data.
  std::vector<std::vector<Element>> contents;
  contents.reserve(rows.size() + 1);
  std::vector<Element> header(4);
  header[0] = text("Schema");
  header[1] = text("Events");
  header[2] = text("Percentage");
  header[3] = text("Histogram");
  contents.push_back(std::move(header));
  for (auto& [name, count, percentage] : rows) {
    std::vector<Element> row(4);
    row[0] = text(std::move(name));
    row[1] = text(fmt::format(std::locale("en_US.UTF-8"), "{:L}", count));
    row[2] = text(fmt::format("{:.1f}", percentage));
    row[3] = gauge(percentage);
    contents.push_back(std::move(row));
  }
  auto table = Table{std::move(contents)};
  apply_styling(table);
  table.SelectColumns(1, 2).DecorateCells(align_right);
  return table;
}

/// Creates a table that shows the build configuration.
/// @param status An instance of a status record.
/// @returns A table of the build configuration.
/// @relates make_table
Table make_build_configuration_table(const data& status) {
  if (const auto* xs = caf::get_if<record>(&status)) {
    if (auto i = xs->find("version"); i != xs->end()) {
      if (const auto* ys = caf::get_if<record>(&i->second)) {
        if (auto j = ys->find("Build Configuration"); j != ys->end()) {
          if (auto zs = caf::get_if<record>(&j->second)) {
            auto t = make_table("Option", "Value", *zs);
            apply_styling(t);
            return t;
          }
        }
      }
    }
  }
  return {};
}

/// Creates a table that shows the VAST version details.
/// @param status An instance of a status record.
/// @returns A table of the version details fo the various components.
/// @relates make_table
Table make_version_table(const data& status) {
  if (const auto* xs = caf::get_if<record>(&status)) {
    if (auto i = xs->find("version"); i != xs->end()) {
      if (const auto* ys = caf::get_if<record>(&i->second)) {
        auto copy = *ys;
        if (auto j = copy.find("Build Configuration"); j != copy.end())
          copy.erase(j);
        auto t = make_table("Component", "Version", copy);
        apply_styling(t);
        return t;
      }
    }
  }
  return {};
}

// Element Vee() {
//   static constexpr auto vee = {
//     R"(////////////    **************************)",
//     R"( ////////////    ************************ )",
//     R"(  ////////////    **********************  )",
//     R"(   ////////////    ********************   )",
//     R"(    ////////////    ******************    )",
//     R"(     ////////////         ***********     )",
//     R"(      ////////////       ***********      )",
//     R"(       ////////////     ***********       )",
//     R"(        ////////////    **********        )",
//     R"(         ////////////    ********         )",
//     R"(          ////////////    ******          )",
//     R"(           ////////////    ****           )",
//     R"(            ////////////    **            )",
//     R"(             ////////////                 )",
//     R"(              ////////////                )",
//   };
//   Elements elements;
//   for (const auto* line : vee)
//     elements.emplace_back(text(line));
//   return vbox(elements);
// }

Element Vee() {
  Elements elements;
  auto line = [&](auto... xs) {
    elements.emplace_back(hbox(xs...));
  };
  auto c1 = [](auto x) {
    return text(x) | color(Color::Blue);
  };
  auto c2 = [](auto x) {
    return text(x) | color(Color::Cyan);
  };
  line(c1("////////////    "), c2("*************************"));
  line(c1(" ////////////    "), c2("*********************** "));
  line(c1("  ////////////    "), c2("*********************  "));
  line(c1("   ////////////    "), c2("*******************   "));
  line(c1("    ////////////    "), c2("*****************    "));
  line(c1("     ////////////         "), c2("**********     "));
  line(c1("      ////////////       "), c2("**********      "));
  line(c1("       ////////////     "), c2("**********       "));
  line(c1("        ////////////    "), c2("*********        "));
  line(c1("         ////////////    "), c2("*******         "));
  line(c1("          ////////////    "), c2("*****          "));
  line(c1("           ////////////    "), c2("***           "));
  line(c1("            ////////////    "), c2("*            "));
  line(c1("             ////////////                 "));
  line(c1("              ////////////                "));
  return vbox(elements);
}

Element VAST() {
  static constexpr auto letters = {
    "@@@@@@        @@@@@@    @@@@@            @@@@@@@@      @@@@@@@@@@@@@@@@",
    " @@@@@@      @@@@@@    @@@@@@@        @@@@@@@@@@@@@@   @@@@@@@@@@@@@@@@",
    "  @@@@@@    @@@@@@    @@@@@@@@@      @@@@@@                 @@@@@@     ",
    "   @@@@@   @@@@@@    @@@@@ @@@@@      @@@@@@@@@@@@          @@@@@@     ",
    "    @@@@@  @@@@@    @@@@@   @@@@@       @@@@@@@@@@@@@       @@@@@@     ",
    "     @@@@@@@@@@    @@@@@@@@@@@@@@@              @@@@@@      @@@@@@     ",
    "      @@@@@@@@    @@@@@@@@@@@@@@@@@   @@@@@@   @@@@@@       @@@@@@     ",
    "       @@@@@@     @@@@@       @@@@@@    @@@@@@@@@@@@        @@@@@@     ",
  };
  Elements elements;
  for (const auto* line : letters)
    elements.emplace_back(text(line));
  return vbox(elements);
}

/// The help component.
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
Component ConnectWindow(ui_state* state) {
  class Impl : public ComponentBase {
  public:
    Impl(ui_state* state) : state_{state} {
      auto action = [=] {
        static const auto default_node_id
          = std::string{defaults::system::node_id};
        static const auto default_endpoint
          = std::string{defaults::system::endpoint};
        caf::settings opts;
        auto node_id = node_id_.empty() ? default_node_id : node_id_;
        auto endpoint = endpoint_.empty() ? default_endpoint : endpoint_;
        // Do not allow duplicates.
        if (state_->nodes.contains(node_id)) {
          VAST_WARN("ignoring request to add duplicate node");
          return;
        }
        caf::put(opts, "vast.node-id", std::move(node_id));
        caf::put(opts, "vast.endpoint", std::move(endpoint));
        caf::anon_send(state_->parent, atom::connect_v, std::move(opts));
      };
      auto node_id = Input(&node_id_, std::string{defaults::system::node_id});
      auto endpoint
        = Input(&endpoint_, std::string{defaults::system::endpoint});
      auto connect = Button("Connect", action, state->theme.button_option());
      auto container = Container::Vertical({
        node_id,  //
        endpoint, //
        connect,  //
      });
      auto renderer = Renderer(container, [=] {
        return vbox({
                 text("Connect to VAST Node") | center | bold,
                 separator(),
                 hbox(text("ID:     "),
                      node_id->Render() | color(state->theme.color.primary)),
                 hbox(text("Endpoint: "),
                      endpoint->Render() | color(state->theme.color.primary)),
                 separator(),
                 connect->Render() | center,
               })
               | size(WIDTH, GREATER_THAN, 40) //
               | border | center;
      });
      Add(renderer);
    }

    Element Render() override {
      return ComponentBase::Render();
    }

  private:
    ui_state* state_;
    std::string node_id_;
    std::string endpoint_;
  };
  return Make<Impl>(state);
}

/// A component that displays the node status.
Component NodeStatus(ui_state* state, std::string node_id) {
  class Impl : public ComponentBase {
  public:
    Impl(ui_state* state, std::string node_id)
      : state_{state}, node_id_{std::move(node_id)} {
      flexbox_config_.direction = FlexboxConfig::Direction::Row;
      flexbox_config_.wrap = FlexboxConfig::Wrap::Wrap;
      flexbox_config_.justify_content
        = FlexboxConfig::JustifyContent::SpaceAround;
      flexbox_config_.align_items = FlexboxConfig::AlignItems::FlexStart;
      flexbox_config_.align_content = FlexboxConfig::AlignContent::FlexStart;
      auto charts = Renderer([=] {
        return flexbox({chart("RAM"),    //
                        chart("Memory"), //
                        chart("Ingestion")},
                       flexbox_config_);
      });
      // Add statistics.
      auto& node = state_->nodes[node_id_];
      VAST_ASSERT(node.actor);
      auto stats = Renderer([=] {
        auto version = make_version_table(node.status).Render();
        auto build_cfg = make_build_configuration_table(node.status).Render();
        auto schema = make_schema_table(node.status).Render();
        auto make_box = [](auto x, auto... xs) {
          // return vbox({text(x) | center, separator(), xs...}) | border;
          return window(text(x) | center, xs...);
        };
        return flexbox(
          {
            make_box("Schema Distribution", schema),
            make_box("Version", version),
            make_box("Build Configuration", build_cfg),
          },
          flexbox_config_);
      });
      auto status = make_collapsible("Status", node.status);
      auto action = [=] {
        state_->nodes.erase(node_id_);
      };
      auto remove_node
        = Button("Remove Node", action,
                 state->theme.button_option<theme::style::alert>());
      auto container = Container::Vertical({
        charts,
        stats,
        status,
        remove_node,
      });
      auto renderer = Renderer(container, [=] {
        return vbox({
                 text(node_id_) | hcenter | bold,
                 separator(),
                 charts->Render(),
                 separator(),
                 stats->Render(),
                 separator(),
                 status->Render(),
                 separator(),
                 remove_node->Render() | xflex,
               })
               | vscroll_indicator | frame;
      });
      Add(renderer);
    }

    Element Render() override {
      return ComponentBase::Render();
    }

  private:
    static Element chart(std::string name) {
      return window(text(std::move(name)),
                    graph(dummy_graph) | color(Color::GrayLight)) //
             | size(WIDTH, EQUAL, 30) | size(HEIGHT, EQUAL, 15);
    }

    static std::vector<int> dummy_graph(int width, int height) {
      std::vector<int> result(width);
      for (int i = 0; i < width; ++i)
        result[i] = i % (height - 4) + 2;
      return result;
    }

    ui_state* state_;
    std::string node_id_;
    FlexboxConfig flexbox_config_;
  };
  return Make<Impl>(state, std::move(node_id));
}

/// An overview of the managed VAST nodes.
Component Fleet(ui_state* state) {
  class Impl : public ComponentBase {
  public:
    Impl(ui_state* state) : state_{state} {
      // Create button to add new node.
      auto action = [=] {
        mode_index_ = 1;
      };
      auto button = Button("+ Add Node", action, state->theme.button_option());
      // Create node menu.
      auto menu = Menu(&labels_, &menu_index_,
                       state->theme.navigation(MenuOption::Direction::Down));
      // When clicking nodes in the menu, go back to main mode.
      menu |= CatchEvent([=](Event event) {
        if (menu->Focused() && !labels_.empty())
          if (event == Event::Return
              || (event.mouse().button == Mouse::Left
                  && event.mouse().motion == Mouse::Released))
            mode_index_ = 0;
        return false;
      });
      // The menu and button make up the navigation.
      auto navigation_container = Container::Vertical({
        menu,   //
        button, //
      });
      // Render the navigation.
      auto navigation = Renderer(navigation_container, [=] {
        return vbox({
                 text("Nodes") | center,
                 separator(),
                 menu->Render(),
                 filler(),
                 button->Render() | xflex,
               })
               | size(WIDTH, GREATER_THAN, 20); //
      });
      // The connection window is always first; the button toggles it.
      menu_tab_ = Container::Tab({}, &menu_index_);
      auto connect_window = ConnectWindow(state_);
      auto mode_tab = Container::Tab({menu_tab_, connect_window}, &mode_index_);
      auto split = ResizableSplitLeft(navigation, mode_tab, &mode_width_);
      Add(split);
    }

    Element Render() override {
      // Monitor state changes.
      auto num_nodes = state_->nodes.size();
      if (num_nodes > num_nodes_) {
        // Register the newly add node.
        num_nodes_ = num_nodes;
        const auto& [id, node] = as_vector(state_->nodes).back();
        // Add new menu entry.
        labels_.emplace_back(id);
        menu_index_ = static_cast<int>(labels_.size());
        // Add corresponding status page.
        menu_tab_->Add(NodeStatus(state_, id));
        // Focus status pane.
        mode_index_ = 0;
      } else if (num_nodes < num_nodes_) {
        // Remove the deleted node.
        num_nodes_ = num_nodes;
        // Figure out which node got removed.
        auto i = std::find_if(labels_.begin(), labels_.end(),
                              [&](const auto& label) {
                                return !state_->nodes.contains(label);
                              });
        VAST_ASSERT(i != labels_.end());
        // Remove the corresponding component.
        auto idx = std::distance(labels_.begin(), i);
        auto page = menu_tab_->ChildAt(idx);
        VAST_ASSERT(page != nullptr);
        page->Detach();
        labels_.erase(i);
        // Go back to connect pane.
        menu_index_ = 0;
        mode_index_ = 1;
      }
      return ComponentBase::Render();
    }

  private:
    ui_state* state_;
    Component menu_tab_;
    std::vector<std::string> labels_;
    int menu_index_ = 0;
    int mode_index_ = 1;
    int mode_width_ = 20;
    size_t num_nodes_ = 0;
  };
  return Make<Impl>(state);
};

Component Hunt() {
  return Renderer([] {
    return text("not yet implemented") | flex | center;
  });
}

Component About() {
  return Renderer([] {
    return vbox({
             Vee() | center,                  //
             text(""),                        //
             text(""),                        //
             VAST() | center,                 //
             text(""),                        //
             text(""),                        //
             text("http://vast.io") | center, //
           })
           | flex | center;
  });
}

Component LogPane(ui_state* state) {
  class Impl : public ComponentBase {
  public:
    Impl(ui_state* state) : state_{state} {
      Add(
        Menu(&state_->log_messages, &index_, state_->theme.structured_data()));
    }

    Element Render() override {
      auto size = static_cast<int>(state_->log_messages.size());
      if (saved_size_ != size) {
        saved_size_ = size;
        index_ = size - 1;
      }
      return ComponentBase::Render() | vscroll_indicator | frame;
    }

  private:
    ui_state* state_;
    int index_ = 0;
    int saved_size_ = 0;
  };
  return Make<Impl>(state);
}

Component MainWindow(ui_state* state) {
  class Impl : public ComponentBase {
  public:
    Impl(ui_state* state) : state_{state} {
      Components pages;
      auto add_page = [&](std::string name, Component page) {
        page_names_.push_back(std::move(name));
        pages.push_back(std::move(page));
      };
      add_page("Fleet", Fleet(state_));
      add_page("Hunt", Hunt());
      add_page("About", About());
      // Create the navigation.
      auto page_menu
        = Menu(&page_names_, &page_index_, state_->theme.navigation());
      // Build the containers that the menu references.
      auto page_tab = Container::Tab(std::move(pages), &page_index_);
      auto log_pane = LogPane(state_);
      page_tab = ResizableSplitBottom(log_pane, page_tab, &log_height_);
      // Build the main container.
      auto container = Container::Vertical({
        page_menu,
        page_tab,
      });
      auto main = Renderer(container, [=] {
        return vbox({
          hbox({page_menu->Render() | flex}),
          page_tab->Render() | flex,
        });
      });
      auto help = Help();
      main |= Modal(help, &show_help_);
      main |= CatchEvent([=](Event event) {
        if (show_help_) {
          if (event == Event::Character('q') || event == Event::Escape) {
            show_help_ = false;
            return true;
          }
        } else {
          if (event == Event::Character('q') || event == Event::Escape) {
            state_->screen.Exit();
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
    ui_state* state_;
    std::vector<std::string> page_names_;
    bool show_help_ = false;
    int page_index_ = -1;
    int log_height_ = 10;
  };
  return Make<Impl>(state);
};

struct ui_actor_state {
  /// The only function to update the UI.
  template <class Function>
  void mutate(Function f) {
    auto task = [=]() mutable {
      f(&ui);
    };
    // Execute the task asynchronously.
    ui.screen.Post(task);
    // Always redraw the screen after a state mutation.
    ui.screen.PostEvent(Event::Custom);
  };

  /// The FTXUI screen shared with the worker actor. We're passing data from
  /// this actor to the UI via the mutation helper function.
  ui_state ui;

  /// The actor owning the UI main loop.
  caf::actor loop;

  /// Pointer to the owning actor.
  ui_actor::pointer self;

  /// Actor name.
  static inline const char* name = "ui";
};

/// The implementation of the UI actor.
ui_actor::behavior_type ui(ui_actor::stateful_pointer<ui_actor_state> self) {
  self->state.ui.parent = self;
  // Monkey-patch the logger. ¯\_(ツ)_/¯
  // FIXME: major danger / highly inappropriate. This is not thread safe. We
  // probably want a dedicated logger plugin that allows for adding custom
  // sinks. This yolo approach is only temporary.
  auto receiver = caf::actor_cast<caf::actor>(self);
  auto sink = std::make_shared<actor_sink_mt>(std::move(receiver));
  detail::logger()->sinks().clear();
  detail::logger()->sinks().push_back(std::move(sink));
  // Terminate if we get a signal from the outside world.
  self->set_exit_handler([=](const caf::exit_msg& msg) {
    // TODO: is Exit() thread-safe or do we need to go through mutate()?
    self->state.ui.screen.Exit();
    self->quit(msg.reason);
  });
  self->set_down_handler([=](const caf::down_msg& msg) {
    // If the main loop has exited, we're done.
    if (msg.source == self->state.loop) {
      self->quit(msg.reason);
      return;
    }
    // We're also monitoring remote VAST nodes.
    auto f = [remote = msg.source](ui_state* state) {
      for (auto& [_, node] : state->nodes)
        if (node.actor.address() == remote)
          node.actor = system::node_actor{};
    };
    self->state.mutate(f);
  });
  return {
    // Process a message from the logger.
    // Warning: do not call the VAST_* log macros in this function. It will
    // cause an infite loop because this handler is called for every log
    // message.
    [=](std::string& message) {
      auto f = [msg = std::move(message)](ui_state* ui) {
        ui->log_messages.emplace_back(std::move(msg));
      };
      self->state.mutate(f);
    },
    [=](atom::connect, const caf::settings& opts) {
      // We're creating a scoped actor only because connect_to_node requires
      // one. Otherwise we could have used `self`.
      caf::scoped_actor scoped_self{self->home_system()};
      auto node = system::connect_to_node(scoped_self, opts);
      if (!node) {
        VAST_ERROR(node.error());
        return;
      }
      // Get the status after connection.
      // NB: it would be nice if VAST buffered the keye statistics so that we
      // have the key stats immediately to display, as opposed to slowly
      // accumulating them over time here at the client.
      caf::settings options;
      caf::put(options, "vast.status.detailed", true);
      invocation inv{
        .options = std::move(options),
        .full_name = "status",
      };
      self->request(*node, 5s, atom::run_v, std::move(inv))
        .then(
          [=](const caf::message&) {
            // In theory, we should be processing the status here. But it
            // happens down below. The status handling urgently needs a
            // refactoring. This dance through caf::error is also taking place
            // in the /status endpoint plugin.
          },
          [=](caf::error& error) {
            if (caf::sec{error.code()} != caf::sec::unexpected_response) {
              VAST_ERROR(error);
              return;
            }
            std::string actual_result;
            auto ctx = error.context();
            caf::message_handler{[&](caf::message& msg) {
              caf::message_handler{[&](std::string& str) {
                actual_result = std::move(str);
              }}(msg);
            }}(ctx);
            // Re-parse as data and update node state.
            if (auto json = from_json(actual_result)) {
              VAST_DEBUG("got status");
              auto f = [=, status = std::move(*json)](ui_state* ui) mutable {
                auto node_state = ui_state::node_state{
                  .actor = *node,
                  .opts = opts,
                  .status = std::move(status),
                };
                auto id = caf::get<std::string>(opts, "vast.node-id");
                VAST_ASSERT(!id.empty());
                ui->nodes.emplace(std::move(id), std::move(node_state));
              };
              self->state.mutate(f);
            }
          });
    },
    // Handle a connection to a new node.
    [=](atom::run) {
      // Ban UI into dedicated thread. We're getting a down message upon
      // termination, e.g., when pushes the exit button or CTRL+C.
      self->state.loop = self->spawn<caf::detached + caf::monitored>([=] {
        auto main = MainWindow(&self->state.ui);
        self->state.ui.screen.Loop(main);
      });
    },
  };
}

} // namespace

ui_actor spawn_ui(caf::actor_system& system) {
  return system.spawn(ui);
}

} // namespace vast::plugins::tui

//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tui/tui.hpp"

#include <vast/logger.hpp>

#include <ftxui/component/component.hpp>
#include <ftxui/component/screen_interactive.hpp>
#include <ftxui/dom/elements.hpp>
#include <ftxui/dom/node.hpp>
#include <ftxui/dom/table.hpp>
#include <ftxui/screen/color.hpp>
#include <ftxui/screen/screen.hpp>

namespace vast::plugins::tui {

using namespace ftxui;
using namespace std::string_literals;

/// The application global state. The UI thread owns this data structure. It is
/// not thread-safe to modify it outside of this context.
struct tui_state {
  /// The FTXUI screen.
  ftxui::ScreenInteractive screen = ScreenInteractive::Fullscreen();

  /// Flag that indicates whether the help modal is shown.
  bool show_help = false;

  /// State for the log pane.
  struct log_state {
    int height = 10;
    int index = 0;
    std::vector<std::string> messages;
  } log;

  /// Navigation state.
  struct navigation_state {
    int page_index = 0;
    std::vector<std::string> page_names;
  } nav;

  struct theme_state {
    Color primary_color = Color::Green;
    Color secondary_color = Color::Blue;
  } theme;

  // --------------------------------

  /// Thread-safe channel to execute code in the context of FTXUI main thread.
  template <class Function>
  void mutate(Function f) {
    tui_state* state = this;
    auto task = [=]() mutable {
      f(state);
    };
    screen.Post(task);
  }
};

namespace {

auto make_button_option(tui_state::theme_state* state) {
  ButtonOption result;
  result.transform = [=](const EntryState& s) {
    auto element = text(s.label) | border;
    if (s.active)
      element |= bold;
    if (s.focused)
      element |= color(state->primary_color);
    return element;
  };
  return result;
}

auto make_button(auto label, auto action, tui_state* state) {
  return Button(label, action, make_button_option(&state->theme));
}

// Style note:
// For our handrolled FTXUI elements and components, we slightly deviate from
// our naming convention. We use PascalCase for our own custom components, so
// that their composition becomes clearer in the FTXUI context.

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
    "       @@@@@@     @@@@@       @@@@@@   @@@@@@@@@@@@@        @@@@@@     ",
  };
  Elements elements;
  for (const auto* line : letters)
    elements.emplace_back(text(line));
  return vbox(elements);
}

/// The help component.
Component Help() {
  return Renderer([&] {
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

struct page {
  std::string name;
  Component component;
};

page home_page(tui_state* state) {
  auto action = [=] { VAST_INFO("test!"); };
  auto connect = make_button(" Connect ", action, state);
  auto container = Container::Vertical({
    connect //
  });
  auto renderer = Renderer(container, [=] {
    return vbox({
             connect->Render(),
           })
           | flex | center;
  });
  return {"Home", renderer};
}

page hunt_page() {
  return {"Hunt", Renderer([] {
            return text("hunt!") | flex | center;
          })};
}

page settings_page() {
  return {"Settings", Renderer([] {
            return text("settings") | flex | center;
          })};
}

page about_page() {
  return {"About", Renderer([] {
            return vbox({
                     Vee() | center,                        //
                     text(""),                              //
                     text(""),                              //
                     VAST() | color(Color::Green) | center, //
                   })
                   | flex | center;
          })};
}

Component LogPane(tui_state* state) {
  MenuOption menu_option;
  menu_option.entries.transform = [=](const EntryState& entry) {
    Element e = text(entry.label);
    if (entry.focused)
      e |= color(state->theme.primary_color);
    if (entry.active)
      e |= bold;
    if (!entry.focused && !entry.active)
      e |= dim;
    return e;
  };
  auto menu = Menu(&state->log.messages, &state->log.index, menu_option);
  auto container = Container::Vertical({
      menu
      });
  return Renderer(container, [=] {
    return vbox({menu->Render() | vscroll_indicator | frame});
  });
}

Component MainWindow(tui_state* state) {
  // Make the navigation a tad prettier.
  auto option = MenuOption::HorizontalAnimated();
  option.underline.SetAnimation(std::chrono::milliseconds(500),
                                animation::easing::Linear);
  option.entries.transform = [=](EntryState entry_state) {
    Element e = text(entry_state.label) | hcenter | flex;
    if (entry_state.active && entry_state.focused)
      e = e | bold | color(state->theme.primary_color);
    if (!entry_state.focused && !entry_state.active)
      e = e | dim;
    return e;
  };
  option.underline.color_inactive = Color::Default;
  option.underline.color_active = Color::Green;
  // Register the pages.
  auto pages = {
    home_page(state), //
    hunt_page(),      //
    settings_page(),  //
    about_page(),     //
  };
  Components components;
  components.reserve(pages.size());
  for (const auto& page : pages) {
    state->nav.page_names.push_back(page.name);
    components.push_back(page.component);
  }
  // Create the navigation.
  auto menu = Menu(&state->nav.page_names, &state->nav.page_index, option);
  // Build the containers that the menu references.
  auto content = Container::Tab(components, &state->nav.page_index);
  auto log_pane = LogPane(state);
  content = ResizableSplitBottom(log_pane, content, &state->log.height);
  // Build the main container.
  auto container = Container::Vertical({
    menu,
    content,
  });
  auto main = Renderer(container, [&] {
    return vbox({
             menu->Render(),
             content->Render() | flex,
           })
           | border;
  });
  auto help = Help();
  main |= Modal(help, &state->show_help);
  //  Catch key events.
  main |= CatchEvent([&](Event event) {
    if (state->show_help) {
      if (event == Event::Character('q') || event == Event::Escape) {
        state->show_help = false;
        return true;
      }
    } else {
      if (event == Event::Character('q') || event == Event::Escape) {
        state->screen.ExitLoopClosure()();
        return true;
      }
      // Show help via '?'
      if (event == Event::Character('?')) {
        state->show_help = true;
        return true;
      }
    }
    return false;
  });
  return main;
}

} // namespace

tui::tui() : state_{std::make_unique<tui_state>()} {
}

tui::~tui() = default;

void tui::loop() {
  auto main = MainWindow(state_.get());
  state_->screen.Loop(main);
}

void tui::add_log(std::string line) {
  state_->mutate([line = std::move(line)](tui_state* state) mutable {
    state->log.messages.emplace_back(std::move(line));
    // Always select last element when new log lines arrive.
    state->log.index = static_cast<int>(state->log.messages.size() - 1);
  });
}

void tui::redraw() {
  state_->screen.PostEvent(Event::Custom);
}

} // namespace vast::plugins::tui

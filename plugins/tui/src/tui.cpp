//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tui/tui.hpp"

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

// Style note:
// For our handrolled FTXUI elements and components, we slightly deviate from
// our naming convention. We use PascalCase for our own custom components, so
// that their composition becomes clearer in the FTXUI context.

/// The application global state.
struct tui_state {
  ftxui::ScreenInteractive screen = ScreenInteractive::Fullscreen();
  bool show_help = false;
  struct navigation {
    int page_index = 0;
    std::vector<std::string> page_names;
  } nav;
  std::vector<ftxui::Element> logs;
};

namespace {

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

Element VASTslanted() {
  static constexpr auto banner = {
    R"( _   _____   __________)",
    R"(| | / / _ | / __/_  __/)",
    R"(| |/ / __ |_\ \  / /   )",
    R"(|___/_/ |_/___/ /_/    )",
  };
  Elements elements;
  for (const auto* line : banner)
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

page home_page() {
  return {"Home", Renderer([&] {
            return text("home") | color(Color::Green) | flex | center;
          })};
}

page hunt_page() {
  return {"Hunt", Renderer([&] {
            return text("hunt!") | flex | center;
          })};
}

page settings_page() {
  return {"Settings", Renderer([&] {
            return text("settings") | flex | center;
          })};
}

page about_page() {
  return {"About", Renderer([&] {
            return vbox({
                     Vee() | center,                        //
                     text(""),                              //
                     text(""),                              //
                     VAST() | color(Color::Green) | center, //
                   })
                   | flex | center;
          })};
}

Component MainWindow(tui_state* state) {
  // Make the navigation a tad prettier.
  auto option = MenuOption::HorizontalAnimated();
  option.underline.SetAnimation(std::chrono::milliseconds(500),
                                animation::easing::Linear);
  option.entries.transform = [](EntryState entry_state) {
    Element e = text(entry_state.label) | hcenter | flex;
    if (entry_state.active && entry_state.focused)
      e = e | bold;
    if (!entry_state.focused && !entry_state.active)
      e = e | dim;
    return e;
  };
  option.underline.color_inactive = Color::Default;
  option.underline.color_active = Color::Green;
  // Register the pages.
  auto pages = {
    home_page(),     //
    hunt_page(),     //
    settings_page(), //
    about_page(),    //
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

tui::tui() : state_{std::make_shared<tui_state>()} {
}

void tui::loop() {
  auto main = MainWindow(state_.get());
  state_->screen.Loop(main);
}

void tui::add_log(std::string line) {
  state_->logs.emplace_back(text(std::move(line)));
}

void tui::redraw() {
  state_->screen.PostEvent(Event::Custom);
}

} // namespace vast::plugins::tui

//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tui/ui.hpp"

#include "tui/actor_sink.hpp"

#include <caf/event_based_actor.hpp>
#include <caf/stateful_actor.hpp>

#include <vast/logger.hpp>
#include <vast/system/actors.hpp>
#include <vast/system/connect_to_node.hpp>
#include <vast/system/node.hpp>

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

namespace {

/// The FTXUI main loop owns this state data structure. The implementation style
/// therefore follows a functional reactive programming pattern.
///
/// Any data that comes from the outside must be handed over to this structure
/// in a thread-safe manner, which is only possible by moving the data into a
/// lambda function f through ScreenInteractive::Post(f). The main loop then
/// executes this function to update the application state and decides when to
/// redraw the screen next.
///
/// Since we need to performance communication with the rest of the VAST
/// ecosystem, we need an actor system to spawn helper actors that interact with
/// remote VAST nodes, e.g., sending it commands and receiving data.
struct ui_state {
  explicit ui_state(ui_actor::stateful_pointer<ui_state> self) : self{self} {
  }

  /// The FTXUI screen.
  ScreenInteractive screen = ScreenInteractive::Fullscreen();

  /// Flag that indicates whether the help modal is shown.
  bool show_help = false;

  /// State for the log pane.
  struct log_state {
    int height = 10;
    int index = 0;
    std::vector<std::string> messages;
    caf::actor receiver;
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

  /// The state per connected VAST node.
  struct node_state {
    std::string name;
    std::string endpoint;
    system::node_actor node;
    caf::settings opts;
  };

  std::vector<node_state> nodes;
  int node_index = -1;

  /// Pointer to the owning actor.
  ui_actor::pointer self;

  /// Actor name.
  static inline const char* name = "ui";
};

auto make_button_option(ui_state::theme_state* state) {
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

auto make_button(auto label, auto action, ui_state* state) {
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

page home_page(ui_state* state) {
  // FIXME
  // auto action = [=] {
  //  const auto* node_state = state->nodes[state->node_index];
  //  auto actor
  //    = system::connect_to_node(node_state->self, node_state->settings);
  //  if (actor
  //  VAST_INFO("test!");
  //};
  auto action = [] {
  VAST_INFO("test!");
  };
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

Component LogPane(ui_state* state) {
  MenuOption menu_option;
  menu_option.entries.transform = [=](const EntryState& entry) {
    Element e = text(entry.label);
    if (entry.focused)
      e |= color(state->theme.primary_color);
    //if (entry.active)
    //  e |= bold;
    if (!entry.focused && !entry.active)
      e |= dim;
    return e;
  };
  auto menu = Menu(&state->log.messages, &state->log.index, menu_option);
  auto container = Container::Vertical({menu});
  return Renderer(container, [=] {
    return vbox({menu->Render()}) | focusPositionRelative(0, 1) | vscroll_indicator | frame;
  });
}

Component MainWindow(ui_state* state) {
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
  option.underline.color_active = state->theme.primary_color;
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

/// The implementation of the UI actor.
ui_actor::behavior_type ui(ui_actor::stateful_pointer<ui_state> self) {
  // Monkey-patch the logger. ¯\_(ツ)_/¯
  // FIXME: major danger / highly inappropriate. This is not thread safe. We
  // probably want a dedicated logger plugin that allows for adding custom
  // sinks. This yolo approach is only temporary.
  auto receiver = caf::actor_cast<caf::actor>(self);
  auto sink = std::make_shared<actor_sink_mt>(std::move(receiver));
  detail::logger()->sinks().clear();
  detail::logger()->sinks().push_back(std::move(sink));
  /// This function is the only safe function to update the state.
  auto mutate = [=](auto f) {
    auto task = [=]() mutable {
      f(&self->state);
    };
    self->state.screen.Post(task);
  };
  /// Helper function to redraw the screen.
  auto redraw = [=] {
    self->state.screen.PostEvent(Event::Custom); // redraw
  };
  self->set_down_handler([=](const caf::down_msg& msg) {
    // Here's where we can do state cleanup after the FTXUI main loop has
    // terminated.
    self->quit(msg.reason);
  });
  return {
    // Process a message from the logger.
    // Warning: do not call the VAST_* log macros in this function. It will
    // cause an infite loop because this handler is called for every log
    // message.
    [=](std::string& message) {
      auto f = [msg = std::move(message)](ui_state* state) mutable {
        state->log.messages.emplace_back(std::move(msg));
        // Always select last element when new log lines arrive. This is needed
        // in combination with focusPositionRelative, otherwise we're entering
        // in an unfocused area.
        //state->log.index = static_cast<int>(state->log.messages.size() - 1);
      };
      mutate(f);
      redraw();
    },
    [=](atom::run) {
      // Ban UI into dedicated thread. We're getting a down message upon
      // termination, e.g., when pushes the exit button or CTRL+C.
      self->spawn<caf::detached + caf::monitored>([=] {
        auto main = MainWindow(&self->state);
        self->state.screen.Loop(main);
      });
    },
  };
}

} // namespace

ui_actor spawn_ui(caf::actor_system& system) {
  return system.spawn(ui);
}

} // namespace vast::plugins::tui

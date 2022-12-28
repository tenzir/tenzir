//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tui/ui.hpp"

#include "tui/actor_sink.hpp"

#include <vast/defaults.hpp>
#include <vast/logger.hpp>
#include <vast/system/actors.hpp>
#include <vast/system/connect_to_node.hpp>
#include <vast/system/node.hpp>

#include <caf/event_based_actor.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/stateful_actor.hpp>
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

namespace {

/// State of the current theme.
struct theme_state {
  /// The theme colors.
  struct color_state {
    Color primary = Color::Cyan;
    Color secondary = Color::Blue;
    Color focus = Color::Green;
  } color;

  /// Transforms an element according to given entry state.
  void transform(Element& e, const EntryState& entry) const {
    if (entry.focused)
      e |= ftxui::color(color.focus);
    if (entry.active)
      e |= ftxui::color(color.secondary) | bold;
    if (!entry.focused && !entry.active)
      e |= ftxui::color(color.secondary) | dim;
  }

  /// Generates a ButtonOption instance.
  [[nodiscard]] ButtonOption button_option() const {
    ButtonOption result;
    result.transform = [=](const EntryState& entry) {
      Element e
        = hbox({text(" "), text(entry.label), text(" ")}) | center | border;
      transform(e, entry);
      return e;
    };
    return result;
  }

  [[nodiscard]] MenuOption structured_data() const {
    MenuOption result;
    result.entries.transform = [=](const EntryState& entry) {
      Element e = text(entry.label);
      transform(e, entry);
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
      transform(e, entry);
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

/// The FTXUI main loop mutates this actor state. Any data that comes from the
/// "outside" via messages must be handed over to the state in a thread-safe
/// manner, for which there exists an explicit mutation function. It's only safe
/// to stop this actor once the UI thread terminated.
struct ui_state {
  explicit ui_state(ui_actor::stateful_pointer<ui_state> self) : self{self} {
  }

  /// The actor owning the main loop.
  caf::actor main_loop;

  /// The FTXUI screen.
  ScreenInteractive screen = ScreenInteractive::Fullscreen();

  /// The active theme.
  theme_state theme;

  /// The messages from the logger.
  std::vector<std::string> log_messages;

  /// The textual description of node metadata..
  struct node_description {
    /// The node ID.
    std::string id;
    /// The node endpoint.
    std::string endpoint;
  } node_input;

  /// The state per connected VAST node.
  struct node_state {
    /// The node description.
    node_description description;
    /// A handle to the remote node.
    system::node_actor actor;
    /// The settings to connect to the remote node.
    caf::settings opts;
  };

  using node_state_ptr = std::shared_ptr<node_state>;

  /// The list of connected nodes.
  std::vector<node_state_ptr> nodes;

  /// Pointer to the owning actor.
  ui_actor::pointer self;

  /// Actor name.
  static inline const char* name = "ui";
};

/// Constructs node state from the user-provided input.
ui_state::node_state_ptr make_node_state(ui_state::node_description desc) {
  if (desc.id.empty())
    desc.id = defaults::system::node_id;
  if (desc.endpoint.empty())
    desc.endpoint = defaults::system::endpoint;
  auto result = std::make_shared<ui_state::node_state>();
  result->description = desc;
  caf::put(result->opts, "vast.node-id", std::move(desc.id));
  caf::put(result->opts, "vast.endpoint", std::move(desc.endpoint));
  return result;
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

Component
ConnectWindow(ui_state* state,
              std::function<void(ui_state::node_state_ptr)> on_connect = {}) {
  auto action = [=] {
    auto node = make_node_state(state->node_input);
    // We're creating a scoped actor only because connect_to_node requires one.
    // Otherwise we could have used state->self.
    caf::scoped_actor self{state->self->home_system()};
    auto actor = system::connect_to_node(self, node->opts);
    if (!actor) {
      VAST_ERROR(actor.error());
      return;
    }
    node->actor = std::move(*actor);
    state->self->monitor(node->actor);
    state->nodes.push_back(node);
    if (on_connect)
      on_connect(std::move(node));
  };
  auto id
    = Input(&state->node_input.id, std::string{defaults::system::node_id});
  auto endpoint = Input(&state->node_input.endpoint,
                        std::string{defaults::system::endpoint});
  auto connect = Button("Connect", action, state->theme.button_option());
  auto container = Container::Vertical({
    id,       //
    endpoint, //
    connect,  //
  });
  auto renderer = Renderer(container, [=] {
    return vbox({
             text("Connect to VAST Node") | center | bold,
             separator(),
             hbox(text("ID:     "),
                  id->Render() | color(state->theme.color.primary)),
             hbox(text("Endpoint: "),
                  endpoint->Render() | color(state->theme.color.primary)),
             separator(),
             connect->Render() | center,
           })
           | size(WIDTH, GREATER_THAN, 40) //
           | border | center;
  });
  return renderer;
}

/// A component that displays the node status.
Component NodeStatus(ui_state::node_state_ptr node) {
  class Impl : public ComponentBase {
  public:
    Impl(ui_state::node_state_ptr node) : node_{std::move(node)} {
      auto component = Renderer([=] {
        FlexboxConfig config;
        config.direction = FlexboxConfig::Direction::Row;
        config.wrap = FlexboxConfig::Wrap::Wrap;
        config.justify_content = FlexboxConfig::JustifyContent::SpaceAround;
        config.align_items = FlexboxConfig::AlignItems::FlexStart;
        config.align_content = FlexboxConfig::AlignContent::FlexStart;
        auto charts = flexbox({chart("RAM"),    //
                               chart("Memory"), //
                               chart("Ingestion")},
                              config);
        return vbox({
          text(node_->description.id) | hcenter, //
          text(""),                              //
          charts | flex,                         //
        });
      });
      Add(component);
    }

    Element Render() override {
      return ComponentBase::Render();
    }

  private:
    static Element chart(std::string name) {
      return window(text(std::move(name)),
                    graph(dummy_graph) | color(Color::GrayLight)) //
             | size(WIDTH, EQUAL, 40) | size(HEIGHT, EQUAL, 20);
    }

    static std::vector<int> dummy_graph(int width, int height) {
      std::vector<int> result(width);
      for (int i = 0; i < width; ++i)
        result[i] = i % (height - 4) + 2;
      return result;
    }

    ui_state::node_state_ptr node_;
  };
  return Make<Impl>(std::move(node));
};

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
      auto menu_tab = Container::Tab({}, &menu_index_);
      auto on_connect = [=](ui_state::node_state_ptr node) {
        mode_index_ = 0;
        menu_index_ = static_cast<int>(labels_.size());
        labels_.emplace_back(node->description.id);
        menu_tab->Add(NodeStatus(std::move(node)));
      };
      auto mode_tab = Container::Tab(
        {menu_tab, ConnectWindow(state_, on_connect)}, &mode_index_);
      auto split = ResizableSplitLeft(navigation, mode_tab, &menu_width_);
      Add(split);
    }

    Element Render() override {
      return ComponentBase::Render();
    }

  private:
    ui_state* state_;
    std::vector<std::string> labels_;
    int mode_index_ = 1;
    int menu_index_ = 0;
    int menu_width_ = 20;
  };
  return Make<Impl>(state);
};

Component Hunt() {
  return Renderer([] {
    return text("hunt!") | flex | center;
  });
}

Component Settings() {
  return Renderer([] {
    return text("settings") | flex | center;
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
      add_page("Fleet", Fleet(state));
      add_page("Hunt", Hunt());
      add_page("Settings", Settings());
      add_page("About", About());
      // Create the navigation.
      auto page_menu
        = Menu(&page_names_, &page_index_, state->theme.navigation());
      // Build the containers that the menu references.
      auto page_tab = Container::Tab(std::move(pages), &page_index_);
      auto log_pane = LogPane(state);
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
            state->screen.Exit();
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
    self->state.screen.PostEvent(Event::Custom);
  };
  self->set_exit_handler([=](const caf::exit_msg& msg) {
    auto f = [](ui_state* state) mutable {
      state->screen.Exit();
    };
    mutate(f);
    self->quit(msg.reason);
  });
  self->set_down_handler([=](const caf::down_msg& msg) {
    // If the main loop has exited, we're done.
    if (msg.source == self->state.main_loop) {
      self->quit(msg.reason);
      return;
    }
    // We're also monitoring remote VAST nodes. If one goes down, update the UI
    // state accordingly so that it can render the connection status.
    auto f = [remote = msg.source](ui_state* state) mutable {
      for (auto& node : state->nodes)
        if (node->actor.address() == remote)
          node->actor = system::node_actor{};
    };
    mutate(f);
  });
  return {
    // Process a message from the logger.
    // Warning: do not call the VAST_* log macros in this function. It will
    // cause an infite loop because this handler is called for every log
    // message.
    [=](std::string& message) {
      auto f = [msg = std::move(message)](ui_state* state) mutable {
        state->log_messages.emplace_back(std::move(msg));
      };
      mutate(f);
      redraw();
    },
    [=](atom::run) {
      // Ban UI into dedicated thread. We're getting a down message upon
      // termination, e.g., when pushes the exit button or CTRL+C.
      self->state.main_loop = self->spawn<caf::detached + caf::monitored>([=] {
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

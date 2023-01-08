//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tui/ui.hpp"

#include "tui/actor_sink.hpp"
#include "tui/components.hpp"
#include "tui/elements.hpp"
#include "tui/theme.hpp"

#include <vast/concept/parseable/to.hpp>
#include <vast/concept/parseable/vast/expression.hpp>
#include <vast/concept/parseable/vast/json.hpp>
#include <vast/data.hpp>
#include <vast/defaults.hpp>
#include <vast/detail/stable_map.hpp>
#include <vast/logger.hpp>
#include <vast/system/actors.hpp>
#include <vast/system/connect_to_node.hpp>
#include <vast/system/node.hpp>
#include <vast/table_slice.hpp>
#include <vast/uuid.hpp>

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

namespace {

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

  /// State per pipeline.
  struct pipeline_state {
    /// The pipeline expression.
    expression expr;

    /// The buffered data for this pipeline.
    std::vector<table_slice> data;
  };

  /// The list of connected nodes, in order of connection time.
  detail::stable_map<std::string, node_state> nodes;

  /// Tracks pipelines by unique ID, in order of creation.
  detail::stable_map<uuid, pipeline_state> pipelines;

  /// Maps exporters to pipelines.
  std::unordered_map<system::exporter_actor, uuid> exporters;

  /// A handle to the UI actor so that it's possible to initiate actions through
  /// user actions.
  ui_actor parent;
};

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
      auto connect = Button("Connect", action, state_->theme.button_option());
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

// TODO: temporary helper function that produces data for a chart.
std::vector<int> dummy_graph(int width, int height) {
  std::vector<int> result(width);
  for (int i = 0; i < width; ++i)
    result[i] = i % (height - 4) + 2;
  return result;
}

Element chart(std::string title) {
  auto g = graph(dummy_graph)        //
           | color(Color::GrayLight) //
           | size(WIDTH, EQUAL, 30)  //
           | size(HEIGHT, EQUAL, 15);
  return window(text(std::move(title)), std::move(g));
}

Component NodeStatus(ui_state* state, const std::string& node_id) {
  auto container = Container::Vertical({});
  auto title = Renderer([=] {
      return text(node_id) | hcenter | bold;
  });
  container->Add(title);
  // Add charts
  auto charts = hflow({chart("RAM"),    //
                       chart("Memory"), //
                       chart("Ingestion")});
  container->Add(Hover(charts));
  // Add data statistics.
  auto& node_status = state->nodes[node_id].status;
  auto schema_table = make_schema_table(node_status).Render();
  container->Add(Hover(schema_table | xflex));
  // Add node statistics.
  auto version_table = make_version_table(node_status).Render();
  auto build_config = make_build_configuration_table(node_status).Render();
  auto node_summary = Container::Horizontal({});
  node_summary->Add(Hover(std::move(version_table)));
  node_summary->Add(Hover(std::move(build_config)));
  container->Add(node_summary);
  // Add detailed status inspection.
  container->Add(Collapsible("Status", node_status));
  // Add button to remove node.
  auto action = [=] {
    state->nodes.erase(node_id);
  };
  auto remove_node = Button("Remove Node", action,
                            state->theme.button_option<theme::style::alert>());
  container->Add(remove_node);
  return Renderer(container, [=] {
    return container->Render() | vscroll_indicator | frame;
  });
}

/// An overview of the managed VAST nodes.
Component FleetPage(ui_state* state) {
  class Impl : public ComponentBase {
  public:
    Impl(ui_state* state) : state_{state} {
      // Create button to add new node.
      auto action = [=] {
        mode_index_ = 1;
      };
      auto button = Button("+ Add Node", action, state_->theme.button_option());
      // Create node menu.
      auto menu = Menu(&labels_, &menu_index_,
                       state->theme.navigation(MenuOption::Direction::Down));
      // When clicking nodes in the menu, go back to main mode.
      menu |= CatchEvent([=](Event event) {
        if (menu->Focused() && !labels_.empty())
          if (event == Event::Return
              || (event.mouse().button == Mouse::Left
                  && event.mouse().motion == Mouse::Released)) {
            mode_index_ = 0;
          }
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

Component HuntPage(ui_state* state) {
  class Impl : public ComponentBase {
  public:
    Impl(ui_state* state) : state_{state} {
      // The menu and button make up the navigation.
      auto input = Input(&pipeline_input_, "");
      auto action = [=] {
        // Parse input as pipeline.
        if (auto expr = to<expression>(pipeline_input_))
          run(pipeline_input_);
        else
          VAST_WARN("failed to parse pipeline: {}", expr.error());
      };
      auto submit = Button("Run", action, state_->theme.button_option());
      // Create a node selector.
      selector_ = Container::Vertical({});
      auto selector = Renderer(selector_, [=] {
        return window(text("Nodes"),
                      selector_->Render() | vscroll_indicator | yframe
                        | color(state->theme.color.secondary) //
                        | size(HEIGHT, EQUAL, 3));
      });
      selector |= Maybe([=] {
        return !state_->nodes.empty();
      });
      // Put controls together in one row.
      auto top = Container::Horizontal({
        input,
        submit,
        selector,
      });
      top = Renderer(top, [=] {
        return hbox({
                 window(text("Pipeline"), input->Render()) //
                   | xflex                                 //
                   | color(state->theme.color.primary),
                 submit->Render() | size(WIDTH, EQUAL, 9),
                 selector->Render(),
               })
               | size(HEIGHT, GREATER_THAN, 5);
      });
      data_view_ = Container::Vertical({});
      data_view_->Add(Renderer([] {
        return Vee() | center | flex;
      }));
      auto container = Container::Vertical({
        top,
        data_view_ | flex,
      });
      Add(container);
    }

    Element Render() override {
      // Update node selector upon state change.
      auto num_nodes = state_->nodes.size();
      if (num_nodes != num_nodes_) {
        num_nodes_ = num_nodes;
        selector_->DetachAllChildren();
        checkboxes_.resize(num_nodes, true);
        const auto& nodes = as_vector(state_->nodes);
        for (size_t i = 0; i < num_nodes; ++i)
          selector_->Add(Checkbox(nodes[i].first, &checkboxes_[i]));
      }
      // Update data views when new query results arrive.
      auto& pipeline = state_->pipelines[pipeline_id_];
      auto num_slices = pipeline.data.size();
      if (num_slices > num_slices_) {
        VAST_DEBUG("detected new slices: {} -> {}", num_slices_, num_slices);
        // Remove placeholder for the first result.
        if (num_slices_ == 0)
          data_view_->DetachAllChildren();
        // Render new table slices.
        for (size_t i = num_slices_; i < num_slices; ++i)
          data_view_->Add(VerticalDataView(pipeline.data[i]));
        num_slices_ = num_slices;
      }
      return ComponentBase::Render();
    }

  private:
    /// Executes the user-provided pipeline.
    void run(std::string expression) {
      // List of nodes to contact.
      std::vector<std::string> node_ids;
      // Zip through checkboxes and nodes.
      VAST_DEBUG("collecting node actors for new pipeline");
      for (size_t i = 0; i < checkboxes_.size(); ++i) {
        if (checkboxes_[i]) {
          const auto& id = as_vector(state_->nodes)[i].first;
          VAST_DEBUG("selecting node '{}'", id);
          node_ids.push_back(id);
        }
      }
      if (node_ids.empty()) {
        VAST_WARN("no nodes selected, ignoring pipeline: {}", expression);
        return;
      }
      // Create pipeline ID and wait for its updates.
      pipeline_id_ = uuid::random();
      VAST_DEBUG("initiated new pipeline execution with id {}", pipeline_id_);
      caf::anon_send(state_->parent, atom::query_v, pipeline_id_,
                     std::move(expression), std::move(node_ids));
    }

    /// The global app state.
    ui_state* state_;

    /// The user input.
    std::string pipeline_input_;

    /// The node selector with checkboxes.
    Component selector_;

    /// A state flag that tells us when to rebuild the node selector.
    size_t num_nodes_ = 0;

    /// The bottom part of the scren showing the data.
    Component data_view_;

    /// A state flag that tells us when to rebuild the data view.
    size_t num_slices_ = 0;

    /// The boolean flags for the node selector. We're using a deque<bool>
    /// because vector<bool> doesn't provide bool lvalues, which the checkbox
    /// components need. A deque is the next-best thing to sequential storage.
    std::deque<bool> checkboxes_;

    /// The (for now just one and only) UUID for the pipeline state.
    uuid pipeline_id_;
  };
  return Make<Impl>(state);
}

Component AboutPage() {
  return Renderer([] {
    return vbox({
             VAST() | color(Color::Cyan) | center, //
             text(""),                             //
             text(""),                             //
             text("http://vast.io") | center,      //
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
      return ComponentBase::Render() | vscroll_indicator | yframe;
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
      add_page("Fleet", FleetPage(state_));
      add_page("Hunt", HuntPage(state));
      add_page("About", AboutPage());
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
          page_menu->Render() | xflex,
          page_tab->Render() | flex,
        });
      });
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
    // Exit() is thread-safe, so we don't need to go through mutate().
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
      // And exporters.
      if (auto exporter = caf::actor_cast<system::exporter_actor>(remote)) {
        VAST_VERBOSE("removing exporter {}", exporter->address());
        auto i = state->exporters.find(exporter);
        VAST_ASSERT(i != state->exporters.end()); // we registered everyone
        state->exporters.erase(i);
      }
    };
    self->state.mutate(f);
  });
  return {
    // Process a message from the logger.
    // Warning: do not call the VAST_* log macros in this function. It will
    // cause an infite loop because this handler is called for every log
    // message.
    [=](std::string& message) {
      auto f = [msg = std::move(message)](ui_state* ui) mutable {
        ui->log_messages.push_back(std::move(msg));
      };
      self->state.mutate(f);
    },
    [=](table_slice slice) {
      auto remote = self->current_sender();
      // Hand slices to the UI thread that picks them up and renders them.
      auto f = [=](ui_state* state) {
        auto exporter = caf::actor_cast<system::exporter_actor>(remote);
        VAST_ASSERT_CHEAP(state->exporters.contains(exporter));
        auto pipeline_id = state->exporters[exporter];
        auto& pipeline = state->pipelines[pipeline_id];
        VAST_DEBUG("adding table slice with {} events to pipeline {} from {}",
                   slice.rows(), pipeline_id, remote->address());
        pipeline.data.push_back(slice);
      };
      self->state.mutate(f);
    },
    [=](atom::query, uuid pipeline_id, std::string expr,
        std::vector<std::string>& node_ids) {
      auto f = [=](ui_state* state) {
        // Spawn one exporter per pipeline.
        caf::settings options;
        invocation inv = {
          .options = options,
          .full_name = "spawn exporter",
          .arguments = {expr},
        };
        for (const auto& node_id : node_ids) {
          VAST_ASSERT(state->nodes.contains(node_id));
          auto& node = state->nodes[node_id];
          self->request(node.actor, 10s, atom::spawn_v, inv)
            .then(
              // NB: it would be nice to get back the exporter UUID from the
              // node so that we can also access the query through other forms
              // of access, e.g., the REST API.
              [=](caf::actor& actor) {
                auto exporter
                  = caf::actor_cast<system::exporter_actor>(std::move(actor));
                VAST_DEBUG("got new EXPORTER for node '{}'", node_id);
                self->monitor(exporter);
                self->send(exporter, atom::sink_v,
                           caf::actor_cast<caf::actor>(self));
                self->send(exporter, atom::run_v);
                // TODO: consider registering at accountant.
                auto& pipeline = state->pipelines[pipeline_id];
                pipeline.expr = *to<expression>(expr);
                state->exporters[exporter] = pipeline_id;
              },
              [&](caf::error& err) {
                VAST_ERROR("failed to spawn exporter at node '{}': {}", node_id,
                           err);
              });
        }
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

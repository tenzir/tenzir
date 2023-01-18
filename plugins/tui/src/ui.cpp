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
    /// A flag that indicates that the corresponding exporter is still running.
    bool running = true;

    /// The pipeline expression.
    expression expr;

    /// The buffered results for this pipeline.
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

Component NodeStatus(ui_state* state, std::string node_id) {
  class Impl : public ComponentBase {
  public:
    Impl(ui_state* state, std::string node_id)
      : state_{state}, node_id_{std::move(node_id)} {
      // Assemble page as vertical container.
      auto container = Container::Vertical({});
      auto header = Renderer([=] {
        // Not very visible at the moment.
        return text(node_id_) | hcenter | bold;
      });
      container->Add(header);
      // Show charts
      FlexboxConfig flexbox_config;
      flexbox_config.direction = FlexboxConfig::Direction::Row;
      flexbox_config.wrap = FlexboxConfig::Wrap::Wrap;
      flexbox_config.justify_content
        = FlexboxConfig::JustifyContent::SpaceAround;
      flexbox_config.align_items = FlexboxConfig::AlignItems::FlexStart;
      flexbox_config.align_content = FlexboxConfig::AlignContent::FlexStart;
      // Add charts.
      auto charts = flexbox({chart("RAM"), chart("Memory"), chart("Ingestion")},
                            flexbox_config);
      container->Add(Hover(charts));
      // Compute immutable elements that do not change throughout node lifetime.
      auto& status = state_->nodes[node_id_].status;
      // Make schema summary.
      schema_ = Container::Vertical({});
      container->Add(DropdownButton("Schema", schema_));
      // Make build summary.
      auto build_cfg_table = make_build_configuration_table(status);
      auto build_cfg = Renderer([element = build_cfg_table.Render()] {
        return element;
      });
      container->Add(DropdownButton("Build Configuration", build_cfg));
      // Make version summary.
      auto version_table = make_version_table(status);
      auto version = Renderer([element = version_table.Render()] {
        return element;
      });
      container->Add(DropdownButton("Version", version));
      // Add button to remove node.
      auto action = [=] {
        state->nodes.erase(node_id_);
      };
      auto remove_node
        = Button("Remove Node", action,
                 state->theme.button_option<theme::style::alert>());
      container->Add(remove_node);
      Add(container);
    }

    Element Render() override {
      // Only re-render internal state if dynamic status has changed.
      // TODO: determine this properly.
      auto schemas_have_changed = schema_->ChildCount() == 0;
      if (schemas_have_changed) {
        auto& status = state_->nodes[node_id_].status;
        rebuild_schema(status);
      }
      return ComponentBase::Render() | vscroll_indicator | frame;
    }

  private:
    void rebuild_schema(const data& status) {
      auto table = make_schema_table(status);
      schema_->DetachAllChildren();
      auto renderer = Renderer([element = table.Render()] {
        return element;
      });
      schema_->Add(renderer);
    }

    Component schema_;
    ui_state* state_;
    std::string node_id_;
  };
  return Make<Impl>(state, std::move(node_id));
};

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

Component PipelineExplorer(ui_state* state, uuid pipeline_id) {
  class Impl : public ComponentBase {
  public:
    Impl(ui_state* state, uuid pipeline_id)
      : state_{state}, pipeline_id_{pipeline_id} {
      // Create the navigation.
      auto menu_style = state_->theme.navigation(MenuOption::Direction::Down);
      auto nav = Menu(&schemas_, &index_, menu_style);
      // Build the containers that the menu references.
      auto loading = Renderer([] {
        return text("loading...") | center | flex; // fancy spinner instead?
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
      if (complete_)
        return ComponentBase::Render();
      auto& pipeline = state_->pipelines[pipeline_id_];
      auto num_slices = pipeline.data.size();
      if (num_slices > num_slices_) {
        VAST_DEBUG("got new table slices: {} -> {}", num_slices_, num_slices);
        num_slices_ = num_slices;
        // TODO: make rebuilding of components incremental, i.e, the amount of
        // work should be proportional to the new batches that arrive. Right
        // now, we're starting from scratch for every batch, which is quadratic
        // work.
        index_ = 0;
        schemas_.clear();
        data_views_->DetachAllChildren();
        // Left-fold all slices with the same schema name into a dataset.
        std::map<std::string, std::vector<table_slice>> dataset;
        for (const auto& slice : pipeline.data)
          dataset[std::string{slice.layout().name()}].push_back(slice);
        // Display the dataset by schema name & count.
        for (const auto& [schema, slices] : dataset) {
          size_t num_records = 0;
          for (const auto& slice : slices)
            num_records += slice.rows();
          // FIXME: Do not show the first few events. We currently do that
          // because it would overwhelm the DOM rendering otherwise.
          const auto& first = slices[0];
          VAST_DEBUG("rendering 1st slice of {} with {} records", schema,
                     first.rows());
          auto n = fmt::format(std::locale("en_US.UTF-8"), "{:L}", num_records);
          schemas_.push_back(fmt::format("{} ({})", schema, n));
          data_views_->Add(VerticalDataView(first, 100));
        }
        VAST_ASSERT(data_views_->ChildCount() == schemas_.size());
      }
      if (!pipeline.running) {
        complete_ = true;
        if (schemas_.empty()) {
          auto nothing = Renderer([] {
            return text("no results") | center | flex; // fancify?
          });
          data_views_->DetachAllChildren();
          data_views_->Add(nothing);
        }
      }
      return ComponentBase::Render();
    }

  private:
    ui_state* state_;

    /// The width of the navigation split.
    int nav_width_ = 25;

    /// The ID of this pipeline.
    uuid pipeline_id_;

    /// Flag to indicate whether we are done updating.
    bool complete_ = false;

    /// A state flag that tells us when to rebuild the data view.
    size_t num_slices_ = 0;

    /// The currently selected schema.
    int index_ = 0;

    /// The menu items for the navigator.
    std::vector<std::string> schemas_;

    /// The main tab.
    Component data_views_;
  };
  return Make<Impl>(state, pipeline_id);
}

Component ExplorerPage(ui_state* state) {
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
      // Put input controls together at the top.
      auto top = Container::Horizontal({
        input,
        submit,
        selector,
      });
      top = Renderer(top, [=] {
        return vbox({
          hbox({
            window(text("Pipeline"), input->Render()) //
              | xflex                                 //
              | color(state->theme.color.primary),
            submit->Render() | size(WIDTH, EQUAL, 9),
            selector->Render(),
          }) | size(HEIGHT, GREATER_THAN, 5),
          separator() | color(default_theme.color.frame),
        });
      });
      // Put data controls together at the top; a dummy initially.
      bottom_ = Container::Vertical({});
      bottom_->Add(Renderer([] {
        return Vee() | center | flex;
      }));
      auto container = Container::Vertical({
        top,
        bottom_,
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
      // Re-render pipeline once submitted. We're using the pipeline ID as a
      // toggle: it's non-nil only to submit a pipeline instance. Thereafter we
      // forget about it.
      if (pipeline_id_ != uuid::nil()) {
        VAST_DEBUG("creating explorer for pipeline {}", pipeline_id_);
        bottom_->DetachAllChildren();
        bottom_->Add(PipelineExplorer(state_, pipeline_id_));
        pipeline_id_ = uuid::nil();
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

    /// The data explorer.
    Component bottom_;

    /// A state flag that tells us when to rebuild the node selector.
    size_t num_nodes_ = 0;

    /// The boolean flags for the node selector. We're using a deque<bool>
    /// because vector<bool> doesn't provide bool lvalues, which the checkbox
    /// components need. A deque is the next-best thing to sequential storage.
    std::deque<bool> checkboxes_;

    /// The (for now just one and only) UUID for the pipeline state.
    uuid pipeline_id_ = uuid::nil();
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
        TakeFocus();
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
      add_page("Explorer", ExplorerPage(state));
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
        VAST_VERBOSE("got DOWN from exporter {}", exporter->address());
        auto i = state->exporters.find(exporter);
        VAST_ASSERT(i != state->exporters.end()); // we registered everyone
        const auto& pipeline_id = i->second;
        VAST_VERBOSE("marking pipeline as complete: {}", pipeline_id);
        VAST_ASSERT_CHEAP(state->pipelines.contains(pipeline_id));
        // FIXME: table slice can still arrive afterwards!
        state->pipelines[pipeline_id].running = false;
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
        VAST_DEBUG(
          "adding table slice '{}' with {} events to pipeline {} from {}",
          slice.layout().name(), slice.rows(), pipeline_id, remote->address());
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
                VAST_DEBUG("got new EXPORTER {}", exporter->address());
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

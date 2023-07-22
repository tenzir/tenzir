//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "explore/components.hpp"
#include "explore/operator_args.hpp"
#include "explore/printer_args.hpp"
#include "explore/ui_state.hpp"

#include <tenzir/argument_parser.hpp>
#include <tenzir/data.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice.hpp>

#include <ftxui/component/loop.hpp>
#include <ftxui/component/screen_interactive.hpp>
#include <ftxui/screen/screen.hpp>

#include <thread>

namespace tenzir::plugins::explore {

namespace {

/// Construct an FTXUI screen from the plugin configuration.
auto make_interactive_screen(const operator_args& args)
  -> ftxui::ScreenInteractive {
  using namespace ftxui;
  TENZIR_ASSERT((args.width && args.height) || (!args.width && !args.height));
  TENZIR_ASSERT(!args.width
                || (args.width->inner > 0 && args.height->inner > 0));
  if (args.width && args.height)
    return ScreenInteractive::FixedSize(args.width->inner, args.height->inner);
  if (args.fullscreen)
    return ScreenInteractive::Fullscreen();
  return ScreenInteractive::FitComponent();
}

class explore_operator final : public crtp_operator<explore_operator> {
public:
  explore_operator() = default;

  explicit explore_operator(operator_args args) : args_{std::move(args)} {
  }

  auto name() const -> std::string override {
    return "explore";
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<std::monostate> {
    if (!ctrl.has_terminal()) {
      diagnostic::error("no terminal found")
        .note("{} operator requires terminal", name())
        .emit(ctrl.diagnostics());
      co_return;
    }
    using namespace ftxui;
    auto screen = make_interactive_screen(args_);
    auto state = make_ui_state(args_);
    // Ban UI main loop into dedicated thread.
    auto thread = std::thread([&] {
      auto main = MainWindow(&screen, &state);
      screen.Loop(main);
    });
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      // The task executes inside the UI thread. Therefore state access is
      // thread-safe.
      auto task = [&state, &screen, slice] {
        auto& table = state.tables[slice.schema()];
        if (!table)
          table = std::make_shared<table_state>();
        table->slices.push_back(slice);
        screen.PostEvent(Event::Custom); // Redraw screen
      };
      screen.Post(task);
      co_yield {};
    }
    thread.join();
  }

  friend auto inspect(auto& f, explore_operator& x) -> bool {
    (void)f, (void)x;
    return true;
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

private:
  operator_args args_;
};

class table_printer final : public plugin_printer {
public:
  table_printer() = default;

  explicit table_printer(printer_args args) : args_{std::move(args)} {
  }

  // FIXME: this should actually be "table", but it's currently not possible.
  auto name() const -> std::string override {
    return "explore";
  }

  auto instantiate(type, operator_control_plane&) const
    -> caf::expected<std::unique_ptr<printer_instance>> override {
    class screen_printer : public printer_instance {
    public:
      screen_printer(const printer_args& args)
        : state_{make_ui_state(args)}, real_time_{args.real_time} {
      }

      auto process(table_slice slice) -> generator<chunk_ptr> override {
        if (slice.rows() == 0) {
          co_yield {};
          co_return;
        }
        auto& table = state_.tables[slice.schema()];
        if (!table)
          table = std::make_shared<table_state>();
        table->slices.push_back(slice);
        auto& component = components_[slice.schema()];
        if (!component)
          component = DataFrame(&state_, slice.schema());
        if (!real_time_)
          co_return;
        auto result = chunk::make(to_string(component) + '\n');
        state_.tables.clear();
        components_.clear();
        co_yield result;
      }

      auto finish() -> generator<chunk_ptr> override {
        if (!real_time_)
          for (const auto& [schema, component] : components_)
            co_yield chunk::make(to_string(component) + '\n');
      }

    private:
      ui_state state_;
      bool real_time_;
      ftxui::Component component_;
      std::unordered_map<type, ftxui::Component> components_;
    };
    return std::make_unique<screen_printer>(args_);
  }

  auto allows_joining() const -> bool override {
    return true;
  }

  friend auto inspect(auto& f, table_printer& x) -> bool {
    return f.object(x)
      .pretty_name("table_printer")
      .fields(f.field("args", x.args_));
  }

private:
  printer_args args_;
};

class explore_plugin final : public virtual operator_plugin<explore_operator>,
                             public virtual printer_plugin<table_printer> {
public:
  auto name() const -> std::string override {
    return "explore";
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser
      = argument_parser{"explore", fmt::format("https://docs.tenzir.com/docs/"
                                               "connectors/sinks/explore")};
    auto args = operator_args{};
    parser.add("-f,--fullscreen", args.fullscreen);
    parser.add("-w,--width", args.width, "<int>");
    parser.add("-h,--height", args.height, "<int>");
    parser.add("-n,--navigator-position", args.navigator_position, "<string>");
    parser.add("-N,--navigator", args.navigator_auto_hide);
    parser.add("-T,--hide-types", args.hide_types);
    parser.parse(p);
    if (args.width && !args.height)
      diagnostic::error("--width requires also setting --height")
        .primary(args.width->source)
        .throw_();
    else if (args.height && !args.width)
      diagnostic::error("--height requires also setting --width")
        .primary(args.height->source)
        .throw_();
    if (args.navigator_position) {
      const auto& x = args.navigator_position->inner;
      if (!(x == "left" || x == "right" || x == "top" || x == "bottom"))
        diagnostic::error("invalid --navigator value")
          .primary(args.navigator_position->source)
          .note("must be one of 'left|right|top|bottom'")
          .throw_();
    }
    return std::make_unique<explore_operator>(std::move(args));
  }

  auto parse_printer(parser_interface& p) const
    -> std::unique_ptr<plugin_printer> override {
    auto parser
      = argument_parser{"explore", fmt::format("https://docs.tenzir.com/docs/"
                                               "formats/table")};
    auto args = printer_args{};
    parser.add("-r,--real-time", args.real_time);
    parser.add("-T,--hide-types", args.hide_types);
    parser.parse(p);
    return std::make_unique<table_printer>(std::move(args));
  }
};

} // namespace

} // namespace tenzir::plugins::explore

TENZIR_REGISTER_PLUGIN(tenzir::plugins::explore::explore_plugin)
// TENZIR_REGISTER_PLUGIN(tenzir::plugins::explore::table_plugin)

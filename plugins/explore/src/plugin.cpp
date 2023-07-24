//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/data.hpp>
#include <tenzir/error.hpp>
#include <tenzir/location.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tui/components.hpp>
#include <tenzir/tui/ui_state.hpp>

#include <ftxui/component/loop.hpp>
#include <ftxui/component/screen_interactive.hpp>
#include <ftxui/screen/screen.hpp>

#include <thread>

namespace tenzir::plugins::explore {

namespace {

/// The plugin configuration.
struct operator_args {
  std::optional<located<int>> width;
  std::optional<located<int>> height;
  std::optional<location> fullscreen;
  std::optional<location> hide_types;
  std::optional<located<std::string>> navigator_position;
  std::optional<location> navigator_auto_hide;

  friend auto inspect(auto& f, operator_args& x) -> bool {
    return f.object(x)
      .pretty_name("operator_args")
      .fields(f.field("width", x.width), f.field("height", x.height),
              f.field("fullscreen", x.fullscreen),
              f.field("hide_types", x.hide_types),
              f.field("navigator_position", x.navigator_position),
              f.field("navigator_auto_hide", x.navigator_auto_hide));
  }
};

/// Construct the global UI state from the plugin configuration.
auto make_ui_state(const operator_args& args) -> tui::ui_state {
  auto result = tui::ui_state{};
  if (args.navigator_position) {
    if (args.navigator_position->inner == "left")
      result.navigator_position = ftxui::Direction::Left;
    else if (args.navigator_position->inner == "right")
      result.navigator_position = ftxui::Direction::Right;
    else if (args.navigator_position->inner == "top")
      result.navigator_position = ftxui::Direction::Up;
    else if (args.navigator_position->inner == "bottom")
      result.navigator_position = ftxui::Direction::Down;
  }
  if (args.navigator_auto_hide)
    result.navigator_auto_hide = true;
  if (args.hide_types)
    result.hide_types = true;
  return result;
}
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
          table = std::make_shared<tui::table_state>();
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

class plugin final : public virtual operator_plugin<explore_operator> {
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
};

} // namespace

} // namespace tenzir::plugins::explore

TENZIR_REGISTER_PLUGIN(tenzir::plugins::explore::plugin)

//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tui/components.hpp"
#include "tui/elements.hpp"
#include "tui/ui_state.hpp"

#include <vast/concept/parseable/vast/option_set.hpp>
#include <vast/concept/parseable/vast/pipeline.hpp>
#include <vast/data.hpp>
#include <vast/detail/narrow.hpp>
#include <vast/error.hpp>
#include <vast/logger.hpp>
#include <vast/plugin.hpp>
#include <vast/table_slice.hpp>

#include <ftxui/component/screen_interactive.hpp>

#include <thread>

namespace vast::plugins::tui {

namespace {

/// The configuration for the `tui` operator.
struct configuration {
  int width = 0;
  int height = 0;
  std::string mode;

  template <class Inspector>
  friend auto inspect(Inspector& f, configuration& x) {
    return detail::apply_all(f, x.width, x.height, x.mode);
  }

  static inline const record_type& schema() noexcept {
    static auto result = record_type{
      {"height", int64_type{}},
      {"width", int64_type{}},
      {"mode", string_type{}},
    };
    return result;
  }
};

/// Construct an FTXUI screen from the operator configuration.
auto make_screen(const configuration& config) -> ftxui::ScreenInteractive {
  using namespace ftxui;
  if (config.width > 0 && config.height > 0)
    return ScreenInteractive::FixedSize(config.width, config.height);
  if (config.mode == "fullscreen")
    return ScreenInteractive::Fullscreen();
  if (config.mode == "fit")
    return ScreenInteractive::FitComponent();
  return ScreenInteractive::TerminalOutput();
}

/// The *terminal user interface (tui)* operator.
class tui_operator final : public crtp_operator<tui_operator> {
public:
  explicit tui_operator(configuration config) : config_{std::move(config)} {
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<std::monostate> {
    using namespace ftxui;
    auto screen = make_screen(config_);
    ui_state state;
    // Ban UI main loop into dedicated thread.
    auto thread = std::thread([&] {
      auto main = MainWindow(&screen, &state);
      screen.Loop(main);
    });
    for (auto&& slice : input) {
      // The task executes inside the UI thread. Therefore state access is
      // thread-safe.
      auto task = [&state, &screen, slice = std::move(slice)] {
        state.data.push_back(std::move(slice));
        screen.PostEvent(Event::Custom); // Redraw screen
      };
      screen.Post(task);
      co_yield {};
    }
    thread.join();
  }

  auto to_string() const -> std::string override {
    // TODO: print configuration as well.
    return fmt::format("tui");
  }

private:
  configuration config_;
};

class plugin final : public virtual operator_plugin {
public:
  // plugin API
  caf::error initialize([[maybe_unused]] const record& plugin_config,
                        [[maybe_unused]] const record& global_config) override {
    return {};
  }

  [[nodiscard]] std::string name() const override {
    return "tui";
  };

  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    using parsers::optional_ws_or_comment, parsers::required_ws_or_comment,
      parsers::end_of_pipeline_operator, parsers::str;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    // Parse options first.
    const auto options = option_set_parser{{{"width", 'w'}, {"height", 'h'}}};
    const auto option_parser = optional_ws_or_comment >> ~options;
    auto parsed_options = std::unordered_map<std::string, data>{};
    if (!option_parser(f, l, parsed_options)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error,
                        fmt::format("failed to parse {} operator options: '{}'",
                                    name(), pipeline)),
      };
    }
    // Assign parsed options to configuration.
    auto config = configuration();
    for (const auto& [key, value] : parsed_options) {
      auto integer = caf::get_if<uint64_t>(&value);
      if (!integer) {
        return {
          std::string_view{f, l},
          caf::make_error(ec::syntax_error,
                          fmt::format("invalid integer option for {} operator: "
                                      "'{}'",
                                      name(), value)),
        };
      }
      if (key == "w" || key == "width")
        config.width = detail::narrow_cast<int>(*integer);
      else if (key == "h" || key == "height")
        config.height = detail::narrow_cast<int>(*integer);
    }
    // Parse positional arguments.
    auto mode = str("fullscreen") | str("fit");
    const auto p = optional_ws_or_comment >> ~mode >> end_of_pipeline_operator;
    if (!p(f, l, config.mode)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error,
                        fmt::format("failed to parse {} operator: '{}'", name(),
                                    pipeline)),
      };
    }
    return {
      std::string_view{f, l},
      std::make_unique<tui_operator>(std::move(config)),
    };
  }
};

} // namespace

} // namespace vast::plugins::tui

VAST_REGISTER_PLUGIN(vast::plugins::tui::plugin)

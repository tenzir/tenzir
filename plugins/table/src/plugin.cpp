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

#include <optional>
#include <string>

namespace tenzir::plugins::table {

namespace {

struct printer_args {
  std::optional<located<int>> width;
  std::optional<located<int>> height;
  bool real_time;
  bool hide_types;

  friend auto inspect(auto& f, printer_args& x) -> bool {
    return f.object(x)
      .pretty_name("printer_args")
      .fields(f.field("width", x.width), f.field("height", x.height),
              f.field("real-time", x.real_time),
              f.field("hide_types", x.hide_types));
  }
};

class table_printer final : public plugin_printer {
public:
  table_printer() = default;

  explicit table_printer(printer_args args) : args_{std::move(args)} {
  }

  auto name() const -> std::string override {
    return "table";
  }

  auto instantiate(type, operator_control_plane&) const
    -> caf::expected<std::unique_ptr<printer_instance>> override {
    class screen_printer : public printer_instance {
    public:
      screen_printer(const printer_args args) : args_{std::move(args)} {
        if (args_.hide_types)
          state_.hide_types = true;
      }

      auto process(table_slice slice) -> generator<chunk_ptr> override {
        if (slice.rows() == 0) {
          co_yield {};
          co_return;
        }
        auto& table = state_.tables[slice.schema()];
        if (!table)
          table = std::make_shared<tui::table_state>();
        table->slices.push_back(slice);
        auto& component = components_[slice.schema()];
        if (!component)
          component = DataFrame(&state_, slice.schema());
        if (!args_.real_time)
          co_return;
        auto result = chunk::make(to_string(component));
        state_.tables.clear();
        components_.clear();
        co_yield result;
      }

      auto finish() -> generator<chunk_ptr> override {
        if (!args_.real_time)
          for (const auto& [schema, component] : components_)
            co_yield chunk::make(to_string(component));
      }

      auto to_string(const ftxui::Component& component) -> std::string {
        auto document = component->Render();
        auto width = args_.width ? ftxui::Dimension::Fixed(args_.width->inner)
                                 : ftxui::Dimension::Fit(document);
        auto height = args_.height
                        ? ftxui::Dimension::Fixed(args_.height->inner)
                        : ftxui::Dimension::Fit(document);
        auto screen = ftxui::Screen::Create(width, height);
        Render(screen, document);
        return screen.ToString() + '\n';
      }

    private:
      printer_args args_;
      tui::ui_state state_;
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

class plugin final : public virtual printer_plugin<table_printer> {
public:
  auto parse_printer(parser_interface& p) const
    -> std::unique_ptr<plugin_printer> override {
    auto parser
      = argument_parser{"table", fmt::format("https://docs.tenzir.com/docs/"
                                             "formats/table")};
    auto args = printer_args{};
    parser.add("-w,--width", args.width, "<int>");
    parser.add("-h,--height", args.height, "<int>");
    parser.add("-r,--real-time", args.real_time);
    parser.add("-T,--hide-types", args.hide_types);
    parser.parse(p);
    return std::make_unique<table_printer>(std::move(args));
  }
};

} // namespace

} // namespace tenzir::plugins::table

TENZIR_REGISTER_PLUGIN(tenzir::plugins::table::plugin)

//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/detail/assert.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice_builder.hpp>
#include <tenzir/to_lines.hpp>

#include <optional>

namespace tenzir::plugins::lines {

namespace {

struct parser_args {
  std::optional<location> skip_empty;

  template <class Inspector>
  friend auto inspect(Inspector& f, parser_args& x) -> bool {
    return f.object(x)
      .pretty_name("parser_args")
      .fields(f.field("skip_empty", x.skip_empty));
  }
};

auto line_type() -> type {
  return type{
    "tenzir.line",
    record_type{
      {"line", string_type{}},
    },
  };
}

class lines_parser final : public plugin_parser {
public:
  lines_parser() = default;

  explicit lines_parser(parser_args args) : args_{std::move(args)} {
  }

  auto name() const -> std::string override {
    return "lines";
  }

  auto
  instantiate(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> std::optional<generator<table_slice>> override {
    auto make = [](auto& ctrl, generator<chunk_ptr> input,
                   bool skip_empty) -> generator<table_slice> {
      auto num_non_empty_lines = size_t{0};
      auto num_empty_lines = size_t{0};
      auto builder = table_slice_builder{line_type()};
      auto last_finish = std::chrono::steady_clock::now();
      for (auto line : to_lines(std::move(input))) {
        if (not line) {
          co_yield {};
          continue;
        }
        if (line->empty()) {
          ++num_empty_lines;
          if (skip_empty) {
            co_yield {};
            continue;
          }
        } else {
          ++num_non_empty_lines;
        }
        auto num_lines = skip_empty ? num_non_empty_lines
                                    : num_non_empty_lines + num_empty_lines;
        if (not builder.add(*line)) {
          diagnostic::error("failed to add line")
            .hint("line number: ", num_lines + 1)
            .emit(ctrl.diagnostics());
          co_return;
        }
        const auto now = std::chrono::steady_clock::now();
        if (builder.rows() >= defaults::import::table_slice_size
            or last_finish + defaults::import::batch_timeout < now) {
          last_finish = now;
          co_yield builder.finish();
          builder = table_slice_builder{line_type()};
        }
      }
      if (builder.rows() > 0)
        co_yield builder.finish();
    };
    return make(ctrl, std::move(input), !!args_.skip_empty);
  }

  friend auto inspect(auto& f, lines_parser& x) -> bool {
    return f.object(x)
      .pretty_name("lines_parser")
      .fields(f.field("args", x.args_));
  }

private:
  parser_args args_;
};

class plugin final : public virtual parser_plugin<lines_parser> {
public:
  auto parse_parser(parser_interface& p) const
    -> std::unique_ptr<plugin_parser> override {
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/docs/formats/{}", name())};
    auto args = parser_args{};
    parser.add("-s,--skip-empty", args.skip_empty);
    parser.parse(p);
    return std::make_unique<lines_parser>(std::move(args));
  }
};

} // namespace

} // namespace tenzir::plugins::lines

TENZIR_REGISTER_PLUGIN(tenzir::plugins::lines::plugin)

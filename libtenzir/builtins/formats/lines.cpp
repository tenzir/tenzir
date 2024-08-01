//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/detail/assert.hpp>
#include <tenzir/table_slice_builder.hpp>
#include <tenzir/to_lines.hpp>
#include <tenzir/tql/parser.hpp>
#include <tenzir/tql2/plugin.hpp>

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
      if (builder.rows() > 0) {
        co_yield builder.finish();
      }
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

// To get better diagnostics in `write`, i.e.:
//    "'xsv' does not support heterogeneous outputs..."
// -> "'lines' does not..."
// Otherwise, we could just use the `ssv_plugin` directly
class lines_printer final : public plugin_printer {
public:
  lines_printer() = default;

  lines_printer(std::unique_ptr<plugin_printer> inner)
    : inner_(std::move(inner)) {
  }

  auto name() const -> std::string override {
    return "lines";
  }

  auto instantiate(type input_schema, operator_control_plane& ctrl) const
    -> caf::expected<std::unique_ptr<printer_instance>> override {
    TENZIR_ASSERT(inner_);
    return inner_->instantiate(std::move(input_schema), ctrl);
  }

  auto allows_joining() const -> bool override {
    TENZIR_ASSERT(inner_);
    return inner_->allows_joining();
  }

  auto prints_utf8() const -> bool override {
    return true;
  }

  friend auto inspect(auto& f, lines_printer& x) -> bool {
    return f.begin_object(caf::invalid_type_id, "lines_printer")
           && plugin_inspect(f, x.inner_) && f.end_object();
  }

private:
  std::unique_ptr<plugin_printer> inner_;
};

class plugin final : public virtual parser_plugin<lines_parser>,
                     public virtual printer_plugin<lines_printer> {
public:
  auto name() const -> std::string override {
    return "lines";
  }

  auto parse_parser(parser_interface& p) const
    -> std::unique_ptr<plugin_parser> override {
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/formats/{}", name())};
    auto args = parser_args{};
    parser.add("-s,--skip-empty", args.skip_empty);
    parser.parse(p);
    return std::make_unique<lines_parser>(std::move(args));
  }

  auto parse_printer(parser_interface& p) const
    -> std::unique_ptr<plugin_printer> override {
    if (not p.at_end()) {
      diagnostic::error("'lines' printer doesn't accept any arguments")
        .primary(p.current_span())
        .docs(fmt::format("https://docs.tenzir.com/formats/{}", name()))
        .throw_();
    }
    const auto* ssv_plugin = plugins::find<printer_parser_plugin>("ssv");
    TENZIR_DIAG_ASSERT(ssv_plugin);
    auto diag = null_diagnostic_handler{};
    auto parser = tql::make_parser_interface("--no-header", diag);
    TENZIR_DIAG_ASSERT(parser);
    auto result = ssv_plugin->parse_printer(*parser);
    TENZIR_DIAG_ASSERT(result);
    return std::make_unique<lines_printer>(std::move(result));
  }
};

} // namespace

class read_lines final
  : public virtual operator_plugin2<parser_adapter<lines_parser>> {
  auto name() const -> std::string override {
    return "read_lines";
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = parser_args{};
    argument_parser2::operator_(name())
      .add("skip_empty", args.skip_empty)
      .parse(inv, ctx)
      .ignore();
    return std::make_unique<parser_adapter<lines_parser>>(
      lines_parser{std::move(args)});
  }
};

} // namespace tenzir::plugins::lines

TENZIR_REGISTER_PLUGIN(tenzir::plugins::lines::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::lines::read_lines)

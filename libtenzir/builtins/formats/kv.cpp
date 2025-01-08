//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/collect.hpp>
#include <tenzir/concept/parseable/tenzir/data.hpp>
#include <tenzir/multi_series_builder.hpp>
#include <tenzir/multi_series_builder_argument_parser.hpp>
#include <tenzir/operator_control_plane.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/to_lines.hpp>

#include <arrow/api.h>
#include <re2/re2.h>

#include <string_view>

namespace tenzir::plugins::kv {

namespace {

constexpr auto docs = "https://docs.tenzir.com/formats/kv";

class splitter {
public:
  splitter() = default;

  splitter(const splitter& other)
    : regex_{std::make_unique<re2::RE2>(other.regex_->pattern())} {
  }
  splitter(splitter&&) = default;
  splitter& operator=(splitter&&) = default;

  explicit splitter(located<std::string_view> pattern) {
    auto regex = std::make_unique<re2::RE2>(pattern.inner,
                                            re2::RE2::CannedOptions::Quiet);
    if (not regex->ok()) {
      diagnostic::error("could not parse regex: {}", regex->error())
        .primary(pattern.source)
        .note("regex must be supported by RE2")
        .docs("https://github.com/google/re2/wiki/Syntax")
        .throw_();
    }
    auto groups = regex->NumberOfCapturingGroups();
    if (groups > 1) {
      diagnostic::error("regex must have at most one capturing group")
        .primary(pattern.source)
        .docs(docs)
        .throw_();
    }
    if (groups != 1) {
      regex = std::make_unique<re2::RE2>(fmt::format("({})", pattern.inner),
                                         re2::RE2::CannedOptions::Quiet);
      if (not regex->ok()) {
        diagnostic::error("internal error: regex could not be parsed "
                          "after adding a capture group")
          .primary(pattern.source)
          .note(regex->error())
          .throw_();
      }
    }
    regex_ = std::move(regex);
  }

  auto split(std::string_view input,
             const detail::quoting_escaping_policy& quoting) const
    -> std::pair<std::string_view, std::string_view> {
    TENZIR_ASSERT(regex_);
    TENZIR_ASSERT(regex_->NumberOfCapturingGroups() == 1);
    auto group = re2::StringPiece{};
    auto start_offset = 0;
    while (true) {
      if (not re2::RE2::PartialMatch(input.substr(start_offset), *regex_,
                                     &group)) {
        return {input, {}};
      }
      auto head = std::string_view{input.data(), group.data()};
      auto tail = std::string_view{group.data() + group.size(),
                                   input.data() + input.size()};
      auto is_valid = not quoting.is_inside_of_quotes(input, head.size());
      if (is_valid) {
        return {head, tail};
      } else {
        start_offset = head.size() + group.size();
      }
      if (head.size() + group.size() == 0) {
        return {input, {}};
      }
    }
    return {input, {}};
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, splitter& x) -> bool {
    if constexpr (Inspector::is_loading) {
      auto str = std::string{};
      if (not f.apply(str)) {
        return false;
      }
      x.regex_
        = std::make_unique<re2::RE2>(str, re2::RE2::CannedOptions::Quiet);
      if (not x.regex_->ok()) {
        f.set_error(caf::make_error(ec::serialization_error,
                                    fmt::format("could not parse regex: {}",
                                                x.regex_->error())));
        return false;
      }
      if (x.regex_->NumberOfCapturingGroups() != 1) {
        f.set_error(caf::make_error(
          ec::serialization_error,
          fmt::format("expected regex to have 1 capture group, but it has {}",
                      x.regex_->NumberOfCapturingGroups())));
        return false;
      }
      return true;
    } else {
      return f.apply(x.regex_->pattern());
    }
  }

private:
  std::unique_ptr<re2::RE2> regex_;
};

struct kv_args {
  multi_series_builder::options msb_opts_;
  detail::quoting_escaping_policy quoting_;
  splitter field_split_;
  splitter value_split_;

  friend auto inspect(auto& f, kv_args& x) -> bool {
    return f.object(x)
      .pretty_name("kv_parser")
      .fields(f.field("msb_options", x.msb_opts_),
              f.field("quoting", x.quoting_),
              f.field("field_split", x.field_split_),
              f.field("value_split", x.value_split_));
  }
};

class kv_parser;
auto parse_loop(generator<std::optional<std::string_view>> input,
                operator_control_plane& ctrl,
                kv_parser parser) -> generator<table_slice>;

class kv_parser final : public plugin_parser {
public:
  kv_parser() = default;

  explicit kv_parser(kv_args args) : args_{std::move(args)} {
  }

  auto name() const -> std::string override {
    return "kv";
  }

  auto
  instantiate(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> std::optional<generator<table_slice>> override {
    return parse_loop(to_lines(std::move(input)), ctrl, *this);
  }

  auto parse_line(multi_series_builder& builder, diagnostic_handler& dh,
                  std::string_view line) const -> void {
    auto event = builder.record();
    while (not line.empty()) {
      // TODO: We ignore split failures here. There might be better ways
      // to handle this.
      auto [head, tail] = args_.field_split_.split(line, args_.quoting_);
      auto [key_view, value_view]
        = args_.value_split_.split(head, args_.quoting_);
      auto key = args_.quoting_.unquote_unescape(key_view);
      auto value = args_.quoting_.unquote_unescape(value_view);
      event.unflattened_field(std::move(key)).data_unparsed(std::move(value));
      if (line == tail) {
        diagnostic::error("`kv` did not make progress")
          .note("check your field splitter")
          .emit(dh);
        // TODO: warn instead and just continue?
      }
      line = tail;
    }
  }

  auto
  parse_strings(const arrow::StringArray& input,
                diagnostic_handler& diagnostics) const -> std::vector<series> {
    auto dh = transforming_diagnostic_handler{diagnostics, [](auto diag) {
                                                diag.message = fmt::format(
                                                  "parse_kv: {}", diag.message);
                                                return diag;
                                              }};
    auto builder = multi_series_builder{args_.msb_opts_, dh};
    for (auto&& line : values(string_type{}, input)) {
      if (not line) {
        builder.null();
        continue;
      }
      parse_line(builder, diagnostics, *line);
    }
    return builder.finalize();
  }

  auto parse_strings(std::shared_ptr<arrow::StringArray> input,
                     operator_control_plane& ctrl) const
    -> std::vector<series> override {
    TENZIR_ASSERT(input);
    return parse_strings(*input, ctrl.diagnostics());
  }

  friend auto inspect(auto& f, kv_parser& x) -> bool {
    return f.apply(x.args_);
  }

  kv_args args_;
};

auto parse_loop(generator<std::optional<std::string_view>> input,
                operator_control_plane& ctrl,
                kv_parser parser) -> generator<table_slice> {
  auto dh = transforming_diagnostic_handler{
    ctrl.diagnostics(),
    [](auto diag) {
      diag.message = fmt::format("read_kv: {}", diag.message);
      return diag;
    },
  };
  auto builder = multi_series_builder(parser.args_.msb_opts_, dh);
  for (auto&& line : input) {
    if (not line) {
      co_yield {};
      continue;
    }
    for (auto&& slice : builder.yield_ready_as_table_slice()) {
      co_yield std::move(slice);
    }
    parser.parse_line(builder, ctrl.diagnostics(), *line);
  }
  for (auto&& slice : builder.finalize_as_table_slice()) {
    co_yield std::move(slice);
  }
}

class kv_plugin final : public virtual parser_plugin<kv_parser> {
public:
  auto parse_parser(parser_interface& p) const
    -> std::unique_ptr<plugin_parser> override {
    auto parser = argument_parser{"kv", docs};
    auto field_split = std::optional<located<std::string>>{
      std::in_place,
      "\\s",
      location::unknown,
    };
    auto value_split = std::optional<located<std::string>>{
      std::in_place,
      "=",
      location::unknown,
    };
    parser.add(field_split, "<field_split>");
    parser.add(value_split, "<value_split>");
    auto msb_parser = multi_series_builder_argument_parser{};
    msb_parser.add_all_to_parser(parser);
    parser.parse(p);
    auto dh = collecting_diagnostic_handler{};
    auto msb_opts = msb_parser.get_options(dh);
    for (auto&& diag : std::move(dh).collect()) {
      if (diag.severity == severity::error) {
        throw diag;
      }
    }
    msb_opts->settings.default_schema_name = "tenzir.kv";
    return std::make_unique<kv_parser>(kv_args{
      std::move(*msb_opts),
      detail::quoting_escaping_policy{},
      splitter{*field_split},
      splitter{*value_split},
    });
  }
};

class read_kv : public operator_plugin2<parser_adapter<kv_parser>> {
public:
  auto name() const -> std::string override {
    return "read_kv";
  }

  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
    auto parser = argument_parser2::operator_(name());
    auto field_split = std::optional<located<std::string>>{
      std::in_place,
      "\\s",
      location::unknown,
    };
    auto value_split = std::optional<located<std::string>>{
      std::in_place,
      "=",
      location::unknown,
    };
    parser.positional("field_split", field_split);
    parser.positional("value_split", value_split);
    auto msb_parser = multi_series_builder_argument_parser{};
    auto quoting = detail::quoting_escaping_policy{};
    msb_parser.add_all_to_parser(parser);
    parser.named_optional("quotes", quoting.quotes);
    TRY(parser.parse(inv, ctx));
    TRY(auto opts, msb_parser.get_options(ctx.dh()));
    opts.settings.default_schema_name = "tenzir.kv";
    return std::make_unique<parser_adapter<kv_parser>>(kv_parser{{
      std::move(opts),
      std::move(quoting),
      splitter{std::move(*field_split)},
      splitter{std::move(*value_split)},
    }});
  }
};

class parse_kv : public function_plugin {
public:
  auto name() const -> std::string override {
    return "parse_kv";
  }

  auto make_function(invocation inv,
                     session ctx) const -> failure_or<function_ptr> override {
    auto input = ast::expression{};
    auto parser = argument_parser2::function(name());
    auto field_split = std::optional<located<std::string>>{
      std::in_place,
      "\\s",
      location::unknown,
    };
    auto value_split = std::optional<located<std::string>>{
      std::in_place,
      "=",
      location::unknown,
    };
    auto quoting = detail::quoting_escaping_policy{};
    parser.positional("input", input, "string");
    parser.positional("field_split", field_split);
    parser.positional("value_split", value_split);
    parser.named_optional("quotes", quoting.quotes);
    TRY(parser.parse(inv, ctx));
    return function_use::make([input = std::move(input),
                               parser = kv_parser{{
                                 multi_series_builder::options{},
                                 std::move(quoting),
                                 splitter{std::move(*field_split)},
                                 splitter{std::move(*value_split)},
                               }}](evaluator eval, session ctx) {
      return map_series(eval(input), [&](series values) -> multi_series {
        if (values.type.kind().is<null_type>()) {
          return values;
        }
        auto strings = try_as<arrow::StringArray>(&*values.array);
        if (not strings) {
          diagnostic::warning("expected `string`, got `{}`", values.type.kind())
            .primary(input)
            .emit(ctx);
          return series::null(null_type{}, eval.length());
        }
        auto output = parser.parse_strings(*strings, ctx.dh());
        return multi_series{std::move(output)};
      });
    });
  }
};
} // namespace

} // namespace tenzir::plugins::kv

TENZIR_REGISTER_PLUGIN(tenzir::plugins::kv::kv_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::kv::read_kv)

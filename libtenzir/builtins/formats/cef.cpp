//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/concept/convertible/to.hpp>
#include <tenzir/concept/parseable/numeric.hpp>
#include <tenzir/concept/parseable/tenzir/data.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/data.hpp>
#include <tenzir/detail/assert.hpp>
#include <tenzir/detail/line_range.hpp>
#include <tenzir/detail/make_io_stream.hpp>
#include <tenzir/detail/string.hpp>
#include <tenzir/error.hpp>
#include <tenzir/module.hpp>
#include <tenzir/multi_series_builder.hpp>
#include <tenzir/multi_series_builder_argument_parser.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/to_lines.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/type.hpp>
#include <tenzir/view.hpp>

#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <fmt/format.h>

#include <istream>
#include <memory>

namespace tenzir::plugins::cef {

namespace {

/// Unescapes CEF string data containing \r, \n, \\, and \=.
std::string unescape(std::string_view value) {
  std::string result;
  result.reserve(value.size());
  for (auto i = 0u; i < value.size(); ++i) {
    if (value[i] != '\\') {
      result += value[i];
    } else if (i + 1 < value.size()) {
      auto next = value[i + 1];
      switch (next) {
        default:
          result += next;
          break;
        case 'r':
        case 'n':
          result += '\n';
          break;
      }
      ++i;
    }
  }
  return result;
}

/// Parses the CEF extension field as a sequence of key-value pairs.
/// This works by finding the kv separator ('=') and then using everything
/// between a (not quoted) whitespace and that separator as the key. The value
/// is simply the text after the separator before the start of the next key.
/// @param extension The string value of the extension field.
/// @returns A vector of key-value pairs with properly unescaped values.
auto parse_extension(std::string_view extension,
                     auto builder) -> std::optional<diagnostic> {
  if (extension.empty()) {
    return {};
  }
  // find the first not quoted, not escaped kv separator
  auto key_end = detail::find_first_not_in_quotes(extension, '=');
  while (detail::is_escaped(key_end, extension)) {
    key_end = detail::find_first_not_in_quotes(extension, '=', key_end + 1);
  }
  if (key_end == extension.npos) {
    return diagnostic::warning(
             "extension field did not contain a key-value separator")
      .done();
  }
  // extract the first key
  auto key = extension.substr(0, key_end);
  extension.remove_prefix(key_end + 1);
  if (extension.empty()) {
    return diagnostic::warning("extension field did not contain a value").done();
  }
  // find the next not quoted, not escaped kv separator
  while (not extension.empty()) {
    key_end = detail::find_first_not_in_quotes(extension, '=');
    while (detail::is_escaped(key_end, extension)) {
      key_end = detail::find_first_not_in_quotes(extension, '=', key_end + 1);
    }
    // find the last whitespace, determining the end of the value text
    auto value_end = key_end == extension.npos
                       ? extension.npos
                       : extension.find_last_of(" \t", key_end);
    auto value = unescape(detail::unquote(detail::trim(extension.substr(0, value_end))));
    key = tenzir::detail::trim(key);
    if constexpr (detail::multi_series_builder::has_unflattend_field<
                    decltype(builder)>) {
      auto field = builder.unflattend_field(key);
      field.data_unparsed(std::move(value));
    } else {
      auto field = builder.field(key);
      auto res = detail::data_builder::best_effort_parser(value);
      if (res) {
        field.data(*res);
      } else {
        field.data(std::move(value));
      }
    }
    if (value_end != extension.npos) {
      key = extension.substr(value_end + 1, key_end - value_end - 1);
    }
    if (key_end != extension.npos) {
      extension.remove_prefix(key_end + 1);
    } else {
      break;
    }
  }
  return {};
}

[[nodiscard]] auto
parse_line(std::string_view line, auto& msb) -> std::optional<diagnostic> {
  using namespace std::string_view_literals;
  auto fields = detail::split_escaped(line, "|", "\\", 8);
  if (fields.size() < 7 or fields.size() > 8) {
    return diagnostic::warning("incorrect field count in CEF event").done();
  }
  int64_t version;
  auto [ptr, ec] = std::from_chars(
    fields[0].c_str() + 4, fields[0].c_str() + fields[0].size(), version);
  if (not fields[0].starts_with("CEF:") or ec != std::errc{}) {
    return diagnostic::warning("invalid CEF header").done();
  }
  auto r = msb.record();
  r.field("cef_version").data(version);
  r.field("device_vendor").data(std::move(fields[1]));
  r.field("device_product").data(std::move(fields[2]));
  r.field("device_version").data(std::move(fields[3]));
  r.field("signature_id").data(std::move(fields[4]));
  r.field("name").data(std::move(fields[5]));
  r.field("severity").data(std::move(fields[6]));
  if (fields.size() == 8) {
    auto d = parse_extension(fields[7], r.field("extension").record());
    if (d) {
      msb.remove_last();
      return d;
    }
  }
  return {};
}

auto parse_loop(generator<std::optional<std::string_view>> lines,
                diagnostic_handler& diag, multi_series_builder::options options)
  -> generator<table_slice> {
  size_t line_counter = 0;
  auto dh = transforming_diagnostic_handler{
    diag,
    [&](diagnostic d) {
      d.message = fmt::format("cef parser: {}", d.message);
      d.notes.emplace(d.notes.begin(), diagnostic_note_kind::note,
                      fmt::format("line {}", line_counter));
      return d;
    },
  };
  auto msb = multi_series_builder{
    std::move(options),
    dh,
  };
  for (auto&& line : lines) {
    for (auto& v : msb.yield_ready_as_table_slice()) {
      co_yield std::move(v);
    }
    if (!line) {
      co_yield {};
      continue;
    }
    ++line_counter;
    if (line->empty()) {
      TENZIR_DEBUG("CEF parser ignored empty line");
      continue;
    }
    auto d = parse_line(*line, msb);
    if (d) {
      dh.emit(std::move(*d));
    }
  }
  for (auto& slice : msb.finalize_as_table_slice()) {
    co_yield std::move(slice);
  }
}

class cef_parser final : public plugin_parser {
public:
  auto name() const -> std::string override {
    return "cef";
  }

  cef_parser() = default;
  explicit cef_parser(multi_series_builder::options options)
    : options_{std::move(options)} {
    options_.settings.default_schema_name = "cef.event";
  }

  auto optimize(event_order order) -> std::unique_ptr<plugin_parser> override {
    auto opts = options_;
    opts.settings.ordered = order == event_order::ordered;
    return std::make_unique<cef_parser>(std::move(opts));
  }

  auto
  instantiate(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> std::optional<generator<table_slice>> override {
    return parse_loop(to_lines(std::move(input)), ctrl.diagnostics(), options_);
  }

  friend auto inspect(auto& f, cef_parser& x) -> bool {
    return f.apply(x.options_);
  }

private:
  multi_series_builder::options options_;
};

class cef_plugin final : public virtual parser_plugin<cef_parser> {
  auto parse_parser(parser_interface& p) const
    -> std::unique_ptr<plugin_parser> override {
    auto parser = argument_parser{"cef", "https://docs.tenzir.com/formats/cef"};
    auto msb_parser = multi_series_builder_argument_parser{};
    msb_parser.add_all_to_parser(parser);
    parser.parse(p);
    auto dh = collecting_diagnostic_handler{};
    auto opts = msb_parser.get_options(dh);
    for (auto& d : std::move(dh).collect()) {
      if (d.severity == severity::error) {
        throw std::move(d);
      }
    }
    TENZIR_ASSERT(opts);
    return std::make_unique<cef_parser>(std::move(*opts));
  }
};

class read_cef : public operator_plugin2<parser_adapter<cef_parser>> {
public:
  auto name() const -> std::string override {
    return "read_cef";
  }

  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
    auto parser = argument_parser2::operator_(name());
    auto msb_parser = multi_series_builder_argument_parser{};
    msb_parser.add_all_to_parser(parser);
    TRY(parser.parse(inv, ctx));
    TRY(auto opts, msb_parser.get_options(ctx.dh()));
    return std::make_unique<parser_adapter<cef_parser>>(
      cef_parser{std::move(opts)});
  }
};

class parse_cef final : public virtual method_plugin {
public:
  auto name() const -> std::string override {
    return "parse_cef";
  }

  auto make_function(invocation inv,
                     session ctx) const -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::method(name()).add(expr, "<string>").parse(inv, ctx));
    return function_use::make(
      [call = inv.call, expr = std::move(expr)](auto eval, session ctx) {
        auto arg = eval(expr);
        auto f = detail::overload{
          [&](const arrow::NullArray&) {
            return arg;
          },
          [&](const arrow::StringArray& arg) {
            auto b = series_builder{};
            for (auto string : arg) {
              if (not string) {
                b.null();
                continue;
              }
              auto diag = parse_line(*string, b);
              if (diag) {
                ctx.dh().emit(std::move(*diag));
                b.null();
              }
            }
            auto result = b.finish();
            // TODO: Consider whether we need heterogeneous for this. If so,
            // then we must extend the evaluator accordingly.
            if (result.size() != 1) {
              diagnostic::warning("got incompatible CEF messages")
                .primary(call)
                .emit(ctx);
              return series::null(null_type{}, arg.length());
            }
            return std::move(result[0]);
          },
          [&](const auto&) {
            diagnostic::warning("`parse_cef` expected `string`, got `{}`",
                                arg.type.kind())
              .primary(call)
              .emit(ctx);
            return series::null(null_type{}, arg.length());
          },
        };
        return caf::visit(f, *arg.array);
      });
  }
};

} // namespace
} // namespace tenzir::plugins::cef

TENZIR_REGISTER_PLUGIN(tenzir::plugins::cef::cef_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::cef::parse_cef)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::cef::read_cef)
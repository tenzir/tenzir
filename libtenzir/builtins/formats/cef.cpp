//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
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

/// Parses the CEF extension field as a sequence of key-value pairs for further
/// downstream processing.
/// @param extension The string value of the extension field.
/// @returns A vector of key-value pairs with properly unescaped values.
auto parse_extension(std::string_view extension, auto builder, bool raw,
                     std::string_view unflatten) -> std::optional<diagnostic> {
  if (extension.empty()) {
    return {};
  }
  auto key = extension.substr(0, extension.find('='));
  extension.remove_prefix(key.size() + 1);
  while (not extension.empty()) {
    auto next_kv_sep = extension.find('=');
    while (extension[next_kv_sep - 1] == '\\') {
      next_kv_sep = extension.find('=', next_kv_sep + 1);
    }
    auto value_end = extension.find_last_of(" \t", next_kv_sep);
    auto value = extension.substr(0, value_end);
    key = tenzir::detail::trim(key);
    auto field = builder.unflattend_field(key, unflatten);
    if (raw) {
      field.data(std::string{value});
    } else {
      field.data_unparsed(unescape(value));
    }
    if (value_end != extension.npos) {
      key = extension.substr(value_end + 1, next_kv_sep - value_end - 1);
    }
    if (next_kv_sep != extension.npos) {
      extension.remove_prefix(next_kv_sep + 1);
    } else {
      break;
    }
  }
  return {};
}

auto parse_line(std::string_view line, multi_series_builder& msb, bool raw,
                std::string_view unflatten) -> std::optional<diagnostic> {
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
  r.field("product_version").data(std::move(fields[3]));
  r.field("signature_id").data(std::move(fields[4]));
  r.field("name").data(std::move(fields[5]));
  r.field("severity").data(std::move(fields[6]));
  if (fields.size() == 8) {
    auto d = parse_extension(fields[7], r.field("extension").record(), raw,
                             unflatten);
    if (d) {
      msb.remove_last();
      return d;
    }
  }
  return {};
}

auto parse_loop(generator<std::optional<std::string_view>> lines,
                diagnostic_handler& diag,
                combined_parser_options options) -> generator<table_slice> {
  auto schemas = detail::multi_series_builder::get_schemas_unnested(
    true, not options.unflatten.empty());
  auto msb
    = multi_series_builder{options.builder_policy, options.builder_settings,
                           record_builder::basic_parser, std::move(schemas)};
  size_t line_counter = 0;
  for (auto&& line : lines) {
    for (auto& v : msb.yield_ready_as_table_slice()) {
      co_yield std::move(v);
    }
    for (auto e : msb.last_errors()) {
      diagnostic::error(e).emit(diag);
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
    auto d = parse_line(*line, msb, options.raw, options.unflatten);
    if (d) {
      diagnostic_builder{std::move(*d)}.hint("note {}", line_counter).emit(diag);
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
  cef_parser(combined_parser_options options) : options_{std::move(options)} {
    options_.builder_settings.default_name = "cef.event";
  }

  auto optimize(event_order order) -> std::unique_ptr<plugin_parser> override {
    auto opts = options_;
    opts.builder_settings.ordered = order == event_order::ordered;
    return std::make_unique<cef_parser>(std::move(opts));
  }

  auto
  instantiate(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> std::optional<generator<table_slice>> override {
    return parse_loop(to_lines(std::move(input)), ctrl.diagnostics(), options_);
  }

  friend auto inspect(auto& f, cef_parser& x) -> bool {
    return f.object(x).fields(f.field("options", x.options_));
  }

private:
  combined_parser_options options_;
};

class cef_plugin final : public virtual parser_plugin<cef_parser> {
  auto parse_parser(parser_interface& p) const
    -> std::unique_ptr<plugin_parser> override {
    auto parser = argument_parser{"cef", "https://docs.tenzir.com/formats/cef"};

    auto combined_parser = combined_parser_options_parser{};
    combined_parser.add_to_parser(parser);
    parser.parse(p);
    return std::make_unique<cef_parser>(combined_parser.get_options());
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
    auto opt_parser = combined_parser_options_parser{};
    opt_parser.add_to_parser(parser);
    auto result = parser.parse(inv, ctx);
    TRY(result);
    try {
      return std::make_unique<parser_adapter<cef_parser>>(
        cef_parser{opt_parser.get_options()});
    } catch (diagnostic& e) {
      ctx.dh().emit(e);
      return failure::promise();
    }
  }
};

} // namespace

} // namespace tenzir::plugins::cef

TENZIR_REGISTER_PLUGIN(tenzir::plugins::cef::cef_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::cef::read_cef)

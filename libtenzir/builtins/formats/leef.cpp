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
#include <tenzir/concept/parseable/tenzir/kvp.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/data.hpp>
#include <tenzir/detail/assert.hpp>
#include <tenzir/detail/coding.hpp>
#include <tenzir/detail/line_range.hpp>
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

#include <memory>
#include <string_view>

// The Log Event Extended Format (LEEF) is an event representation that has been
// popularized by IBM QRadar. The official documentation at
// https://www.ibm.com/docs/en/dsm?topic=overview-leef-event-components provides
// more details into the spec.

// TODO:
// - Parse the devTime attribute (and devTimeFormat) and assign it to the event
//   timestamp. An option for this behavior should exist.
// - Use the *Label field suffix as field name, e.g., Foo="42"\tFooLabel="xxx"
//   should be translated into xxx=42 by the parser. An option for this behavior
//   should exist.
// - Stretch: consider a timezone option in case devTimeFormat doesn't contain
//   one.
namespace tenzir::plugins::leef {

namespace {

// TODO: it's unlclear whether that's correct. There is not much info out there
// in the internet that tells us how to do this properly.
/// Unescapes LEEF string data containing \r, \n, \\, and \=.
auto unescape(std::string_view::iterator begin, std::string_view::iterator end,
              std::back_insert_iterator<std::string> out)
  -> std::string_view::iterator {
  TENZIR_ASSERT_EXPENSIVE(*std::prev(begin) == '\\');
  TENZIR_ASSERT_EXPENSIVE(begin < end);
  switch (*begin) {
    case 'n': {
      out = '\n';
      return ++begin;
    }
    case 'r': {
      out = '\n';
      return ++begin;
    }
    case 't': {
      out = '\t';
      return ++begin;
    }
    case '=': {
      out = '=';
      return ++begin;
    }
    case '\\': {
      out = '\\';
      return ++begin;
    }
    default: {
      return begin;
    }
  }
  TENZIR_UNREACHABLE();
  return begin;
}

/// Parses a LEEF delimiter.
auto parse_delimiter(std::string_view field) -> std::variant<char, diagnostic> {
  if (field.empty()) {
    return diagnostic::warning("got empty delimiter")
      .note("LEEF v2.0 requires a delimiter specification")
      .hint("delimiter must be a single character or start with 'x' or '0x'")
      .done();
  }
  if (field.starts_with("x") or field.starts_with("0x")) {
    // Spec: "The hex value can be represented by the prefix 0x or x, followed
    // by a series of 1-4 characters (0-9A-Fa-f)."
    // Me: WTH should 3 hex characters represent? I get 1. And 2. Also 4. But 3?
    auto i = field.find('x');
    TENZIR_ASSERT(i != std::string_view::npos);
    auto hex = field.substr(i + 1);
    for (auto h : hex) {
      if (std::isxdigit(h) == 0) {
        return diagnostic::warning("invalid hex delimiter: {}", field)
          .hint("hex delimiters with 'x' or '0x' require subsequent hex chars")
          .done();
      }
    }
    switch (hex.size()) {
      default:
        return diagnostic::warning("wrong hex delimiter size: {}", hex.size())
          .hint("need 1 or 2 hex chars")
          .done();
      case 1:
        return detail::hex_to_byte('0', hex[0]);
      case 2:
        return detail::hex_to_byte(hex[0], hex[1]);
      // TODO: address this only once a user ever gets such a weird log.
      case 3:
      case 4:
        return diagnostic::warning("wrong number of hex delimiters: {}",
                                   hex.size())
          .note("cannot interpret 3 or 4 characters in a meaningful way")
          .hint("need 1 or 2 hex chars")
          .done();
    }
  } else if (field.size() > 1) {
    return diagnostic::warning("invalid non-hex delimiter")
      .hint("expected a single character, but got {}", field.size())
      .done();
  }
  return field[0];
}

/// Parses the LEEF attributes field as a sequence of key-value pairs.
auto parse_attributes(char delimiter, std::string_view attributes, auto builder,
                      const detail::quoting_escaping_policy& quoting)
  -> std::optional<diagnostic> {
  while (not attributes.empty()) {
    auto attr_end = quoting.find_not_in_quotes(attributes, delimiter);
    /// We greedily accept more than one consecutive separator
    while (attr_end < attributes.size() - 1
           and attributes[attr_end + 1] == delimiter) {
      ++attr_end;
    }
    const auto attribute = attributes.substr(0, attr_end);
    auto sep_pos = quoting.find_not_in_quotes(attribute, '=', 0, true);
    if (sep_pos == 0) {
      return diagnostic::warning("missing key before separator in attributes")
        .note("attribute was `{}`", attribute)
        .done();
    }
    if (sep_pos == attribute.npos) {
      return diagnostic::warning("missing key-value separator in attribute")
        .note("attribute was `{}`", attribute)
        .done();
    }
    auto key = attribute.substr(0, sep_pos);
    auto value
      = quoting.unquote_unescape(detail::trim(attribute.substr(sep_pos + 1)));
    if constexpr (detail::multi_series_builder::has_unflattened_field<
                    decltype(builder)>) {
      auto field = builder.unflattened_field(key);
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
    if (attr_end != attributes.npos) {
      attributes.remove_prefix(attr_end + 1);
    } else {
      break;
    }
  }
  return {};
}

[[nodiscard]] auto parse_line(std::string_view line, auto& builder,
                              const detail::quoting_escaping_policy& quoting)
  -> std::optional<diagnostic> {
  using namespace std::string_view_literals;
  // We first need to find out whether we are LEEF 1.0 or 2.0. The latter has
  // one additional top-level component.
  auto num_fields = 0u;
  if (not line.starts_with("LEEF:")) {
    return diagnostic::warning("invalid LEEF event")
      .hint("LEEF events start with LEEF:$VERSION|...")
      .done();
  }
  auto pipe = line.find('|');
  if (pipe == std::string_view::npos) {
    return diagnostic::warning("invalid LEEF event")
      .note("could not find a pipe (|) that separates LEEF metadata")
      .done();
  }
  auto colon = line.find(':');
  TENZIR_ASSERT(colon != std::string_view::npos);

  const auto leef_version = line.substr(colon + 1, pipe - colon - 1);
  if (leef_version == "1.0") {
    num_fields = 5;
  } else if (leef_version == "2.0") {
    num_fields = 6;
  } else {
    return diagnostic::warning("unsupported LEEF version: {}", leef_version)
      .hint("only 1.0 and 2.0 are valid values")
      .done();
  }
  auto fields = detail::split_escaped(line, "|", "\\", num_fields);
  if (fields.size() != num_fields + 1) {
    return diagnostic::warning("LEEF {} requires at least {} fields",
                               leef_version, num_fields + 1)
      .note("got {} fields", fields.size())
      .done();
  }
  auto delimiter = '\t';
  if (leef_version == "2.0") {
    auto delim = parse_delimiter(fields[5]);
    if (const auto* c = std::get_if<char>(&delim)) {
      TENZIR_DEBUG("parsed LEEF delimiter: {:#04x}", *c);
      delimiter = *c;
    } else {
      return std::get<diagnostic>(delim);
    }
  }
  auto r = builder.record();
  r.field("leef_version").data(std::string{leef_version});
  r.field("vendor").data(std::move(fields[1]));
  r.field("product_name").data(std::move(fields[2]));
  r.field("product_version").data(std::move(fields[3]));
  r.field("event_class_id").data(std::move(fields[4]));

  auto d = parse_attributes(delimiter, fields[num_fields],
                            r.field("attributes").record(), quoting);
  if (d) {
    builder.remove_last();
    return d;
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
      d.message = fmt::format("leef parser: {}", d.message);
      d.notes.emplace(d.notes.begin(), diagnostic_note_kind::note,
                      fmt::format("line {}", line_counter));
      return d;
    },
  };
  auto quoting = detail::quoting_escaping_policy{
    .unescape_operation = unescape,
  };
  auto msb = multi_series_builder{
    std::move(options),
    dh,
  };
  for (auto&& line : lines) {
    for (auto& v : msb.yield_ready_as_table_slice()) {
      co_yield std::move(v);
    }
    if (! line) {
      co_yield {};
      continue;
    }
    ++line_counter;
    if (line->empty()) {
      TENZIR_DEBUG("LEEF parser ignored empty line");
      continue;
    }
    auto d = parse_line(*line, msb, quoting);
    if (d) {
      dh.emit(std::move(*d));
    }
  }
  for (auto& v : msb.finalize_as_table_slice()) {
    co_yield std::move(v);
  }
}

class leef_parser final : public plugin_parser {
public:
  auto name() const -> std::string override {
    return "leef";
  }

  leef_parser() = default;

  explicit leef_parser(multi_series_builder::options options)
    : options_{std::move(options)} {
    options_.settings.default_schema_name = "leef.event";
  }

  auto optimize(event_order order) -> std::unique_ptr<plugin_parser> override {
    auto opts = options_;
    opts.settings.ordered = order == event_order::ordered;
    return std::make_unique<leef_parser>(std::move(opts));
  }

  auto
  instantiate(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> std::optional<generator<table_slice>> override {
    return parse_loop(to_lines(std::move(input)), ctrl.diagnostics(), options_);
  }

  friend auto inspect(auto& f, leef_parser& x) -> bool {
    return f.apply(x.options_);
  }

private:
  multi_series_builder::options options_ = {};
};

class leef_plugin final : public virtual parser_plugin<leef_parser> {
  auto parse_parser(parser_interface& p) const
    -> std::unique_ptr<plugin_parser> override {
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/formats/{}", name())};
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
    return std::make_unique<leef_parser>(std::move(*opts));
  }
};

class read_leef : public operator_plugin2<parser_adapter<leef_parser>> {
public:
  auto name() const -> std::string override {
    return "read_leef";
  }
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto parser = argument_parser2::operator_(name());
    auto msb_parser = multi_series_builder_argument_parser{};
    msb_parser.add_all_to_parser(parser);
    TRY(parser.parse(inv, ctx));
    TRY(auto opts, msb_parser.get_options(ctx.dh()));
    return std::make_unique<parser_adapter<leef_parser>>(
      leef_parser{std::move(opts)});
  }
};

class parse_leef final : public virtual function_plugin {
public:
  auto name() const -> std::string override {
    return "parse_leef";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    auto parser = argument_parser2::function(name());
    parser.positional("x", expr, "string");
    auto msb_parser = multi_series_builder_argument_parser{};
    msb_parser.add_policy_to_parser(parser);
    msb_parser.add_settings_to_parser(parser, true, false);
    TRY(parser.parse(inv, ctx));
    TRY(auto msb_opts, msb_parser.get_options(ctx));
    return function_use::make([call = inv.call.get_location(),
                               msb_ops = std::move(msb_opts),
                               expr = std::move(expr)](auto eval, session ctx) {
      return map_series(eval(expr), [&](series arg) {
        auto f = detail::overload{
          [&](const arrow::NullArray&) -> multi_series {
            return arg;
          },
          [&](const arrow::StringArray& arg) -> multi_series {
            auto builder = multi_series_builder{
              msb_ops,
              ctx,
            };
            auto quoting = detail::quoting_escaping_policy{};
            for (auto string : arg) {
              if (not string) {
                builder.null();
                continue;
              }
              auto diag = parse_line(*string, builder, quoting);
              if (diag) {
                ctx.dh().emit(std::move(*diag));
                builder.null();
              }
            }
            return multi_series{builder.finalize()};
          },
          [&](const auto&) -> multi_series {
            diagnostic::warning("`parse_leef` expected `string`, got "
                                "`{}`",
                                arg.type.kind())
              .primary(call)
              .emit(ctx);
            return series::null(null_type{}, arg.length());
          },
        };
        return match(*arg.array, f);
      });
    });
  }
};

} // namespace
} // namespace tenzir::plugins::leef

TENZIR_REGISTER_PLUGIN(tenzir::plugins::leef::leef_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::leef::read_leef)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::leef::parse_leef)

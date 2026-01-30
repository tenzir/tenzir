//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_utils.hpp>
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
#include <tenzir/table_slice.hpp>
#include <tenzir/to_lines.hpp>
#include <tenzir/type.hpp>
#include <tenzir/view.hpp>
#include <tenzir/view3.hpp>

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
    msb_parser.add_settings_to_parser(
      parser, true, multi_series_builder_argument_parser::merge_option::hidden);
    TRY(parser.parse(inv, ctx));
    TRY(auto msb_opts, msb_parser.get_options(ctx));
    return function_use::make([call = inv.call.get_location(),
                               msb_ops = std::move(msb_opts),
                               expr = std::move(expr)](auto eval, session ctx) {
      return map_series(eval(expr), [&](series arg) {
        auto f = detail::overload{
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
            /// TODO: We actually know the type it would produce here, sans the
            /// attributes
            return series::null(null_type{}, arg.length());
          },
        };
        return match(*arg.array, f);
      });
    });
  }
};

struct printer_args final {
  ast::expression attributes;
  ast::expression vendor;
  ast::expression product_name;
  ast::expression product_version;
  ast::expression event_class_id;
  located<std::string> delimiter = located{"\t", location::unknown};
  located<std::string> null_value = located{std::string{}, location::unknown};
  located<std::string> flatten_separator
    = located{std::string{"."}, location::unknown};
  location op;

  auto add_to(argument_parser2& p) -> void {
    p.positional("attributes", attributes, "record");
    p.named("vendor", vendor, "string");
    p.named("product_name", product_name, "string");
    p.named("product_version", product_version, "string");
    p.named("event_class_id", event_class_id, "string");
    p.named_optional("delimiter", delimiter, "string");
    p.named_optional("null_value", null_value);
    p.named_optional("flatten_separator", flatten_separator);
  }

  auto loc(into_location loc) const -> location {
    return loc ? loc : op;
  }

  friend auto inspect(auto& f, printer_args& x) -> bool {
    return f.object(x).fields(
      f.field("attributes", x.attributes), f.field("vendor", x.vendor),
      f.field("product_name", x.product_name),
      f.field("product_version", x.product_version),
      f.field("event_class_id", x.event_class_id),
      f.field("delimiter", x.delimiter), f.field("null_value", x.null_value),
      f.field("flatten_separator", x.flatten_separator), f.field("op", x.op));
  }
};

void append_attributes(std::string& out, record_view3 attributes,
                       std::string_view delim, location loc,
                       diagnostic_handler& dh) {
  const auto f = detail::overload{
    [&](const caf::none_t&) {
      // noop
    },
    [&](auto v) {
      fmt::format_to(std::back_inserter(out), "{}", v);
    },

    [&](view3<list>) {
      diagnostic::warning("`list` is not supported in a LEEF attribute value")
        .primary(loc)
        .emit(dh);
    },
    [&](view3<tenzir::pattern>) {
      TENZIR_UNREACHABLE();
    },
    [&](view3<tenzir::record>) {
      TENZIR_UNREACHABLE();
    },
  };
  for (const auto& [k, v] : attributes) {
    out += k;
    out += '=';
    match(v, f);
    out.append(delim);
  }
  // Remove the final delimiter again
  out.erase(out.size() - 1);
}

class print_leef final : public virtual function_plugin {
public:
  auto name() const -> std::string override {
    return "print_leef";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    auto parser = argument_parser2::function(name());
    auto args = printer_args{};
    args.op = inv.call.get_location();
    args.add_to(parser);
    TRY(parser.parse(inv, ctx));
    if (args.delimiter.inner.size() != 1) {
      diagnostic::error("custom LEEF `delimiter` must be a single character")
        .primary(args.delimiter, "got `{}`", args.delimiter.inner)
        .emit(ctx);
      return failure::promise();
    }
    if (args.delimiter.inner == "|") {
      diagnostic::error("custom LEEF `delimiter` must not be `|`")
        .primary(args.delimiter)
        .emit(ctx);
      return failure::promise();
    }
    if (args.null_value.inner.contains("|")) {
      diagnostic::error("`null_value` must not contain `|`")
        .primary(args.null_value)
        .emit(ctx);
      return failure::promise();
    }
    if (args.flatten_separator.inner.contains("|")) {
      diagnostic::error("`flatten_separator` must not contain `|`")
        .primary(args.flatten_separator)
        .emit(ctx);
      return failure::promise();
    }
    return function_use::make([args = std::move(args)](
                                auto eval, session ctx) -> multi_series {
      const auto arr = std::array{
        eval(args.vendor),          eval(args.product_name),
        eval(args.product_version), eval(args.event_class_id),
        eval(args.attributes),
      };
      return map_series(
        arr, [&args, &ctx](const std::span<series> x) -> multi_series {
          TENZIR_ASSERT(x.size() == 5);
          const auto& vendor_series = x[0];
          const auto& product_name_series = x[1];
          const auto& product_version_series = x[2];
          const auto& event_class_id_series = x[3];
          const auto attributes_series_f
            = flatten(x[4], args.flatten_separator.inner);
          const auto& attributes_series = attributes_series_f.series;
          TENZIR_ASSERT(vendor_series.length() == product_name_series.length());
          TENZIR_ASSERT(vendor_series.length()
                        == product_version_series.length());
          TENZIR_ASSERT(vendor_series.length()
                        == event_class_id_series.length());
          TENZIR_ASSERT(vendor_series.length() == attributes_series.length());
          bool ok = true;
#define TYPE_CHECK_AND_MAKE_GEN(NAME, TYPE)                                    \
  if (not(NAME##_series.type.kind().template is<TYPE>())) {                    \
    ok = false;                                                                \
    diagnostic::warning("`" #NAME "` must be `{}`", type_kind{tag_v<TYPE>})    \
      .primary(args.loc(args.NAME), "got `{}`", NAME##_series.type.kind())     \
      .emit(ctx);                                                              \
  }                                                                            \
  auto NAME##_gen = values3(*NAME##_series.array)
          TYPE_CHECK_AND_MAKE_GEN(vendor, string_type);
          TYPE_CHECK_AND_MAKE_GEN(product_name, string_type);
          TYPE_CHECK_AND_MAKE_GEN(product_version, string_type);
          TYPE_CHECK_AND_MAKE_GEN(event_class_id, string_type);
          TYPE_CHECK_AND_MAKE_GEN(attributes, record_type);
#undef TYPE_CHECK_AND_MAKE_GEN
          if (not ok) {
            return series::null(string_type{}, vendor_series.length());
          }
          auto builder = type_to_arrow_builder_t<string_type>{};
          check(builder.Reserve(vendor_series.length()));
          auto str = std::string{};
          while (true) {
            const auto vendor = vendor_gen.next();
            const auto product_name = product_name_gen.next();
            const auto product_version = product_version_gen.next();
            const auto event_class_id = event_class_id_gen.next();
            const auto attributes = attributes_gen.next();
            if (not vendor) {
              TENZIR_ASSERT(not product_name);
              TENZIR_ASSERT(not product_version);
              TENZIR_ASSERT(not event_class_id);
              TENZIR_ASSERT(not attributes);
              break;
            }
            str = "LEEF:";
            if (args.delimiter.inner == "\t") {
              str.append("1.0");
            } else {
              str.append("2.0");
            }
            str += '|';
#define CHECK_APPEND_VALUE(field)                                              \
  if (auto* s = try_as<view3<std::string>>(*field)) {                          \
    if (s->contains('|')) {                                                    \
      diagnostic::warning("`" #field "` contains illegal character `|`")       \
        .primary(args.field)                                                   \
        .emit(ctx);                                                            \
      check(builder.AppendNull());                                             \
      continue;                                                                \
    } else {                                                                   \
      str.append(as<view3<std::string>>(*field));                              \
    }                                                                          \
  } else if (is<view3<caf::none_t>>(*field)) {                                 \
    diagnostic::warning("`" #field "` is `null`")                              \
      .primary(args.field)                                                     \
      .emit(ctx);                                                              \
    check(builder.AppendNull());                                               \
    continue;                                                                  \
  } else {                                                                     \
    TENZIR_UNREACHABLE();                                                      \
  }                                                                            \
  str += '|'
            CHECK_APPEND_VALUE(vendor);
            CHECK_APPEND_VALUE(product_name);
            CHECK_APPEND_VALUE(product_version);
            CHECK_APPEND_VALUE(event_class_id);
#undef CHECK_APPEND_VALUE
            if (args.delimiter.inner != "\t") {
              str.append(args.delimiter.inner);
              str += '|';
            }
            if (auto* r = try_as<view3<record>>(*attributes)) {
              append_attributes(str, *r, args.delimiter.inner,
                                args.attributes.get_location(), ctx);
            } else {
              TENZIR_ASSERT(is<view3<caf::none_t>>(*attributes));
              diagnostic::warning("`attributes` is `null`")
                .primary(args.attributes)
                .emit(ctx);
            }
            check(builder.Append(str));
          }
          return series{string_type{}, check(builder.Finish())};
        });
    });
  }
};

} // namespace
} // namespace tenzir::plugins::leef

TENZIR_REGISTER_PLUGIN(tenzir::plugins::leef::leef_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::leef::read_leef)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::leef::parse_leef)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::leef::print_leef)

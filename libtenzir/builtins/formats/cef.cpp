//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
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
#include <tenzir/view3.hpp>

#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <fmt/format.h>

#include <istream>
#include <memory>
#include <string_view>

namespace tenzir::plugins::cef {

namespace {

/// Unescapes CEF string data containing \r, \n, \\, and \=.
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
    case '=': {
      out = '=';
      return ++begin;
    }
    case '\\': {
      out = '\\';
      return ++begin;
    }
  }
  return begin;
}

auto unescape_string(std::string_view in) -> std::string {
  auto result = std::string{};
  result.reserve(in.size());
  for (auto it = in.begin(); it != in.end(); ++it) {
    if (*it == '\\' and it < in.end() - 1) {
      auto start = it + 1;
      auto end = unescape(start, in.end(), std::back_inserter(result));
      if (end != start) {
        continue;
      }
    }
    result += *it;
  }
  return result;
}

/// Checks whether a character is valid in a CEF extension key name.
/// The spec says alphanumeric, but real-world producers commonly use
/// underscores, dots, and hyphens.
auto is_cef_key_char(char c) -> bool {
  return std::isalnum(static_cast<unsigned char>(c)) or c == '_' or c == '.'
         or c == '-';
}

/// Unescapes a CEF extension value, removing surrounding double quotes if
/// present. Fast path avoids allocation when there are no special characters.
auto cef_unescape_value(std::string_view text) -> std::string {
  text = detail::trim(text);
  auto is_quoted
    = text.size() >= 2 and text.front() == '"' and text.back() == '"';
  if (is_quoted) {
    text.remove_prefix(1);
    text.remove_suffix(1);
  }
  // Fast path: no backslashes means no escaping needed.
  if (text.find('\\') == text.npos) {
    return std::string{text};
  }
  auto result = std::string{};
  result.reserve(text.size());
  for (size_t i = 0; i < text.size(); ++i) {
    if (text[i] == '\\' and i + 1 < text.size()) {
      switch (text[i + 1]) {
        case 'n':
          result += '\n';
          ++i;
          continue;
        case 'r':
          result += '\r';
          ++i;
          continue;
        case '=':
          result += '=';
          ++i;
          continue;
        case '\\':
          result += '\\';
          ++i;
          continue;
        case '"':
          result += '"';
          ++i;
          continue;
        default:
          break;
      }
    }
    result += text[i];
  }
  return result;
}

/// Parses the CEF extension field as a sequence of key-value pairs in a single
/// pass over the input. Tracks quoting (double quotes only) and backslash
/// escaping inline rather than delegating to the generic
/// quoting_escaping_policy machinery.
///
/// The algorithm finds '=' separators and uses the heuristic that a real
/// separator must be preceded by whitespace + a valid key name, to distinguish
/// from unescaped '=' embedded in values (e.g., DN strings "CN=foo,O=bar").
auto parse_extension(std::string_view ext, auto builder)
  -> std::optional<diagnostic> {
  if (ext.empty()) {
    return {};
  }
  auto emit_pair = [&](std::string key, std::string value) {
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
  };
  // Single pass: scan for unescaped, unquoted '=' characters.
  auto in_quotes = false;
  size_t i = 0;
  // Find the first '=' — this is always a real separator.
  for (; i < ext.size(); ++i) {
    auto c = ext[i];
    if (c == '\\') {
      ++i;
      continue;
    }
    if (c == '"') {
      in_quotes = not in_quotes;
      continue;
    }
    if (c == '=' and not in_quotes) {
      break;
    }
  }
  if (i >= ext.size()) {
    return diagnostic::warning(
             "extension field did not contain a key-value separator")
      .done();
  }
  auto current_key = unescape_string(detail::trim(ext.substr(0, i)));
  auto value_start = i + 1;
  ++i;
  // Scan the rest for subsequent separator '=' characters.
  for (; i < ext.size(); ++i) {
    auto c = ext[i];
    if (c == '\\') {
      ++i;
      continue;
    }
    if (c == '"') {
      in_quotes = not in_quotes;
      continue;
    }
    if (c != '=' or in_quotes) {
      continue;
    }
    // Found an unescaped, unquoted '=' at position i.
    // Check if this is a real key-value separator by looking backward for
    // whitespace followed by a valid key name.
    // Find the last whitespace before this '=' (searching within the value).
    for (auto j = i - 1; j >= value_start; --j) {
      if (ext[j] != ' ' and ext[j] != '\t') {
        continue;
      }
      // Validate key candidate: ext[j+1 .. i)
      auto key_candidate = ext.substr(j + 1, i - j - 1);
      auto valid_key = not key_candidate.empty();
      for (auto ch : key_candidate) {
        if (not is_cef_key_char(ch)) {
          valid_key = false;
          break;
        }
      }
      if (not valid_key) {
        continue;
      }
      // This is a real separator. Emit the previous key-value pair.
      auto raw_value = ext.substr(value_start, j - value_start);
      emit_pair(std::move(current_key), cef_unescape_value(raw_value));
      current_key = unescape_string(detail::trim(key_candidate));
      value_start = i + 1;
      break;
    }
    // If no whitespace was found before this '=', it's not a real separator
    // (e.g., "CN=foo" inside a DN value). Skip it.
  }
  // Emit the final key-value pair.
  auto raw_value = ext.substr(value_start);
  emit_pair(std::move(current_key), cef_unescape_value(raw_value));
  return {};
}

[[nodiscard]] auto parse_line(std::string_view line, location loc, auto& msb)
  -> std::optional<diagnostic> {
  using namespace std::string_view_literals;
  auto fields = detail::split_escaped(line, "|", "\\", 8);
  if (fields.size() < 7 or fields.size() > 8) {
    return diagnostic::warning("incorrect field count in CEF event")
      .primary(loc)
      .done();
  }
  if (not fields[0].starts_with("CEF:")) {
    return diagnostic::warning("invalid CEF header")
      .primary(loc)
      .note("header does not start with `CEF:`")
      .done();
  }
  int64_t version;
  auto [ptr, ec] = std::from_chars(
    fields[0].c_str() + 4, fields[0].c_str() + fields[0].size(), version);
  if (ec != std::errc{}) {
    return diagnostic::warning("invalid CEF header")
      .primary(loc)
      .note("failed to parse CEF version")
      .done();
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
                diagnostic_handler& diag, location loc,
                multi_series_builder::options options)
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
    if (! line) {
      co_yield {};
      continue;
    }
    ++line_counter;
    if (line->empty()) {
      TENZIR_DEBUG("CEF parser ignored empty line");
      continue;
    }
    auto d = parse_line(*line, loc, msb);
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
    return parse_loop(to_lines(std::move(input)), ctrl.diagnostics(), loc_,
                      options_);
  }

  friend auto inspect(auto& f, cef_parser& x) -> bool {
    return f.object(x).fields(f.field("loc", x.loc_),
                              f.field("options", x.options_));
  }

private:
  location loc_;
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

  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto parser = argument_parser2::operator_(name());
    auto msb_parser = multi_series_builder_argument_parser{};
    msb_parser.add_all_to_parser(parser);
    TRY(parser.parse(inv, ctx));
    TRY(auto opts, msb_parser.get_options(ctx.dh()));
    return std::make_unique<parser_adapter<cef_parser>>(
      cef_parser{std::move(opts)});
  }
};

class parse_cef final : public virtual function_plugin {
public:
  auto name() const -> std::string override {
    return "parse_cef";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
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
                               msb_opts = std::move(msb_opts),
                               expr = std::move(expr)](auto eval, session ctx) {
      return map_series(eval(expr), [&](series arg) {
        auto f = detail::overload{
          [&](const arrow::NullArray&) {
            return multi_series{arg};
          },
          [&](const arrow::StringArray& arg) {
            auto b = multi_series_builder{msb_opts, ctx};
            for (auto string : arg) {
              if (not string) {
                b.null();
                continue;
              }
              auto diag = parse_line(*string, call, b);
              if (diag) {
                ctx.dh().emit(std::move(*diag));
                b.null();
              }
            }
            return multi_series{b.finalize()};
          },
          [&](const auto&) -> multi_series {
            diagnostic::warning("`parse_cef` expected `string`, got `{}`",
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

struct printer_args final {
  ast::expression extension;
  ast::expression cef_version;
  ast::expression device_vendor;
  ast::expression device_product;
  ast::expression device_version;
  ast::expression signature_id;
  ast::expression name;
  ast::expression severity;
  located<std::string> null_value = located{std::string{}, location::unknown};
  located<std::string> flatten_separator
    = located{std::string{"."}, location::unknown};
  location op;

  auto add_to(argument_parser2& p) -> void {
    p.positional("extension", extension, "record");
    p.named("cef_version", cef_version, "string");
    p.named("device_vendor", device_vendor, "string");
    p.named("device_product", device_product, "string");
    p.named("device_version", device_version, "string");
    p.named("signature_id", signature_id, "string");
    p.named("name", name, "string");
    p.named("severity", severity, "string");
    p.named_optional("null_value", null_value);
    p.named_optional("flatten_separator", flatten_separator);
  }

  auto loc(into_location loc) const -> location {
    return loc ? loc : op;
  }

  friend auto inspect(auto& f, printer_args& x) -> bool {
    return f.object(x).fields(
      f.field("extension", x.extension), f.field("cef_version", x.cef_version),
      f.field("device_vendor", x.device_vendor),
      f.field("device_product", x.device_product),
      f.field("device_version", x.device_version),
      f.field("signature_id", x.signature_id), f.field("name", x.name),
      f.field("severity", x.severity), f.field("null_value", x.null_value),
      f.field("flatten_separator", x.flatten_separator), f.field("op", x.op));
  }
};

void append_extension(std::string& out, record_view3 attributes,
                      std::string_view null, location loc,
                      diagnostic_handler& dh) {
  const auto f = detail::overload{
    [&](const caf::none_t&) {
      out += null;
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
    out += ' ';
  }
  // Remove the final delimiter again
  out.erase(out.size() - 1);
}

class print_cef final : public virtual function_plugin {
public:
  auto name() const -> std::string override {
    return "print_cef";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    auto parser = argument_parser2::function(name());
    auto args = printer_args{};
    args.op = inv.call.get_location();
    args.add_to(parser);
    TRY(parser.parse(inv, ctx));
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
        eval(args.extension),      eval(args.cef_version),
        eval(args.device_vendor),  eval(args.device_product),
        eval(args.device_version), eval(args.signature_id),
        eval(args.name),           eval(args.severity),
      };
      return map_series(
        arr, [&args, &ctx](const std::span<series> x) -> multi_series {
          TENZIR_ASSERT(x.size() == 8);
          const auto extension_series_f
            = flatten(x[0], args.flatten_separator.inner);
          const auto& extension_series = extension_series_f.series;
          const auto& cef_version_series = x[1];
          const auto& device_vendor_series = x[2];
          const auto& device_product_series = x[3];
          const auto& device_version_series = x[4];
          const auto& signature_id_series = x[5];
          const auto& name_series = x[6];
          const auto& severity_series = x[7];
          TENZIR_ASSERT(extension_series.length()
                        == cef_version_series.length());
          TENZIR_ASSERT(extension_series.length()
                        == device_vendor_series.length());
          TENZIR_ASSERT(extension_series.length()
                        == device_product_series.length());
          TENZIR_ASSERT(extension_series.length()
                        == device_version_series.length());
          TENZIR_ASSERT(extension_series.length()
                        == signature_id_series.length());
          TENZIR_ASSERT(extension_series.length() == name_series.length());
          TENZIR_ASSERT(extension_series.length() == severity_series.length());
          bool ok = true;
#define TYPE_CHECK_AND_MAKE_GEN(NAME, TYPE)                                    \
  if (not(NAME##_series.type.kind().template is<TYPE>())) {                    \
    ok = false;                                                                \
    diagnostic::warning("`" #NAME "` must be `{}`", type_kind{tag_v<TYPE>})    \
      .primary(args.loc(args.NAME), "got `{}`", NAME##_series.type.kind())     \
      .emit(ctx);                                                              \
  }                                                                            \
  auto NAME##_gen = values3(*NAME##_series.array)
          TYPE_CHECK_AND_MAKE_GEN(extension, record_type);
          TYPE_CHECK_AND_MAKE_GEN(cef_version, string_type);
          TYPE_CHECK_AND_MAKE_GEN(device_vendor, string_type);
          TYPE_CHECK_AND_MAKE_GEN(device_product, string_type);
          TYPE_CHECK_AND_MAKE_GEN(device_version, string_type);
          TYPE_CHECK_AND_MAKE_GEN(signature_id, string_type);
          TYPE_CHECK_AND_MAKE_GEN(name, string_type);
          TYPE_CHECK_AND_MAKE_GEN(severity, string_type);
#undef TYPE_CHECK_AND_MAKE_GEN
          if (not ok) {
            return series::null(string_type{}, extension_series.length());
          }
          auto builder = type_to_arrow_builder_t<string_type>{};
          check(builder.Reserve(extension_series.length()));
          auto str = std::string{};
          while (true) {
            auto extension = extension_gen.next();
            auto cef_version = cef_version_gen.next();
            auto device_vendor = device_vendor_gen.next();
            auto device_product = device_product_gen.next();
            auto device_version = device_version_gen.next();
            auto signature_id = signature_id_gen.next();
            auto name = name_gen.next();
            auto severity = severity_gen.next();
            if (not extension) {
              TENZIR_ASSERT(not cef_version);
              TENZIR_ASSERT(not device_vendor);
              TENZIR_ASSERT(not device_product);
              TENZIR_ASSERT(not device_version);
              TENZIR_ASSERT(not signature_id);
              TENZIR_ASSERT(not name);
              TENZIR_ASSERT(not severity);
              break;
            }
            str = "CEF:";
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
            CHECK_APPEND_VALUE(cef_version);
            CHECK_APPEND_VALUE(device_vendor);
            CHECK_APPEND_VALUE(device_product);
            CHECK_APPEND_VALUE(device_version);
            CHECK_APPEND_VALUE(signature_id);
            CHECK_APPEND_VALUE(name);
            CHECK_APPEND_VALUE(severity);
#undef CHECK_APPEND_VALUE
            if (auto* r = try_as<view3<record>>(*extension)) {
              append_extension(str, *r, args.null_value.inner,
                               args.extension.get_location(), ctx);
            } else {
              TENZIR_ASSERT(is<view3<caf::none_t>>(*extension));
              diagnostic::warning("`extension` is `null`")
                .primary(args.extension)
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
} // namespace tenzir::plugins::cef

TENZIR_REGISTER_PLUGIN(tenzir::plugins::cef::cef_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::cef::parse_cef)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::cef::read_cef)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::cef::print_cef)

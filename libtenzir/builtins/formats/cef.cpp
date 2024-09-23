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

/// A shallow representation a of a CEF message.
struct message_view {
  uint16_t cef_version;
  std::string device_vendor;
  std::string device_product;
  std::string device_version;
  std::string signature_id;
  std::string name;
  std::string severity;
  record extension;
};

/// Parses the CEF extension field as a sequence of key-value pairs for further
/// downstream processing.
/// @param extension The string value of the extension field.
/// @returns A vector of key-value pairs with properly unescaped values.
caf::expected<record> parse_extension(std::string_view extension) {
  record result;
  auto splits = detail::split_escaped(extension, "=", "\\");
  if (splits.size() < 2)
    return caf::make_error(ec::parse_error, fmt::format("need at least one "
                                                        "key=value pair: {}",
                                                        extension));
  // Process intermediate 'k0=a b c k1=d e f' extensions. The algorithm splits
  // on '='. The first split is a key and the last split is a value. All
  // intermediate splits are "reversed" in that they have the pattern 'a b c k1'
  // where 'a b c' is the value from the previous key and 'k1`' is the key for
  // the next value.
  auto key = splits[0];
  // Strip leading whitespace on first key. The spec says that trailing
  // whitespace is considered part of the previous value, except for the last
  // space that is split on.
  for (size_t i = 0; i < key.size(); ++i)
    if (key[i] != ' ') {
      key = key.substr(i);
      break;
    }
  // Converts a raw, unescaped string to a data instance.
  auto to_data = [](std::string_view str) -> data {
    auto unescaped = unescape(str);
    auto parsed = data{};
    if (not(parsers::data - parsers::pattern)(unescaped, parsed)) {
      parsed = unescaped;
    }
    return parsed;
  };
  for (auto i = 1u; i < splits.size() - 1; ++i) {
    auto split = splits[i];
    auto j = split.rfind(' ');
    if (j == std::string_view::npos)
      return caf::make_error(
        ec::parse_error,
        fmt::format("invalid 'key=value=key' extension: {}", split));
    if (j == 0)
      return caf::make_error(
        ec::parse_error,
        fmt::format("empty value in 'key= value=key' extension: {}", split));
    auto value = split.substr(0, j);
    result.emplace(std::string{key}, to_data(value));
    key = split.substr(j + 1); // next key
  }
  auto value = splits[splits.size() - 1];
  result.emplace(std::string{key}, to_data(value));
  return result;
}

/// Converts a string view into a message.
caf::error convert(std::string_view line, message_view& msg) {
  using namespace std::string_view_literals;
  // Pipes in the extension field do not need escaping.
  auto fields = detail::split_escaped(line, "|", "\\", 8);
  if (fields.size() != 8)
    return caf::make_error(ec::parse_error, //
                           fmt::format("need exactly 8 fields, got '{}'",
                                       fields.size()));
  // Field 0: Version
  auto i = fields[0].find(':');
  if (i == std::string_view::npos)
    return caf::make_error(ec::parse_error, //
                           fmt::format("CEF version requires ':', got '{}'",
                                       fields[0]));
  auto cef_version_str
    = std::string_view{std::next(fields[0].begin(), i + 1), fields[0].end()};
  if (!parsers::u16(cef_version_str, msg.cef_version))
    return caf::make_error(ec::parse_error, //
                           fmt::format("failed to parse CEF version, got '{}'",
                                       cef_version_str));
  // Fields 1-6.
  msg.device_vendor = std::move(fields[1]);
  msg.device_product = std::move(fields[2]);
  msg.device_version = std::move(fields[3]);
  msg.signature_id = std::move(fields[4]);
  msg.name = std::move(fields[5]);
  msg.severity = std::move(fields[6]);
  // Field 7: Extension
  if (auto kvps = parse_extension(fields[7]))
    msg.extension = std::move(*kvps);
  else
    return kvps.error();
  return caf::none;
}

/// Parses a line of ASCII as CEF message.
/// @param msg The CEF message.
/// @param builder The table slice builder to add the message to.
void add(const message_view& msg, builder_ref builder) {
  auto event = builder.record();
  event.field("cef_version", uint64_t{msg.cef_version});
  event.field("device_vendor", msg.device_vendor);
  event.field("device_product", msg.device_product);
  event.field("device_version", msg.device_version);
  event.field("signature_id", msg.signature_id);
  event.field("name", msg.name);
  event.field("severity", msg.severity);
  event.field("extension", msg.extension);
}

auto impl(generator<std::optional<std::string_view>> lines, exec_ctx ctx)
  -> generator<table_slice> {
  auto builder = series_builder{};
  for (auto&& line : lines) {
    // TODO: Flush builder if maximum batch size or timeout is reached.
    if (!line) {
      co_yield {};
      continue;
    }
    if (line->empty()) {
      TENZIR_DEBUG("CEF parser ignored empty line");
      continue;
    }
    auto msg = to<message_view>(*line);
    if (!msg) {
      diagnostic::warning("failed to parse message: {}", msg.error())
        .note("line: `{}`", *line)
        .emit(ctrl.diagnostics());
      continue;
    }
    add(*msg, builder);
  }
  for (auto& slice : builder.finish_as_table_slice("cef.event")) {
    co_yield std::move(slice);
  }
}

class cef_parser final : public plugin_parser {
public:
  auto name() const -> std::string override {
    return "cef";
  }

  auto instantiate(generator<chunk_ptr> input, exec_ctx ctx) const
    -> std::optional<generator<table_slice>> override {
    return impl(to_lines(std::move(input)), ctrl);
  }

  friend auto inspect(auto& f, cef_parser& x) -> bool {
    return f.object(x).fields();
  }
};

class plugin final : public virtual parser_plugin<cef_parser> {
  auto parse_parser(parser_interface& p) const
    -> std::unique_ptr<plugin_parser> override {
    argument_parser{"cef", "https://docs.tenzir.com/formats/cef"}.parse(p);
    return std::make_unique<cef_parser>();
  }
};

class parse_cef final : public virtual method_plugin {
public:
  auto name() const -> std::string override {
    return "parse_cef";
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
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
            auto warn = false;
            auto b = series_builder{};
            for (auto string : arg) {
              if (not string) {
                b.null();
                continue;
              }
              auto msg = to<message_view>(*string);
              if (not msg) {
                warn = true;
                b.null();
                continue;
              }
              add(*msg, b);
            }
            if (warn) {
              diagnostic::warning("failed to parse CEF message")
                .primary(call)
                .emit(ctx);
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

TENZIR_REGISTER_PLUGIN(tenzir::plugins::cef::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::cef::parse_cef)

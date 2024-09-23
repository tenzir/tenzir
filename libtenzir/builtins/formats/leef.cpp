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
#include <tenzir/detail/make_io_stream.hpp>
#include <tenzir/detail/string.hpp>
#include <tenzir/error.hpp>
#include <tenzir/module.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/to_lines.hpp>
#include <tenzir/type.hpp>
#include <tenzir/view.hpp>

#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <fmt/format.h>

#include <memory>

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

/// A LEEF event.
struct event {
  std::string leef_version;
  std::string vendor;
  std::string product_name;
  std::string product_version;
  std::string event_id;
  char delim = '\t';
  record attributes;
};

// TODO: it's unlclear whether that's correct. There is not much info out there
// in the internet that tells us how to do this properly.
/// Unescapes LEEF string data containing \r, \n, \\, and \=.
auto unescape(std::string_view value) -> std::string {
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
        case 't':
          result += '\t';
          break;
      }
      ++i;
    }
  }
  return result;
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

// Converts a raw, unescaped string to a data instance.
auto to_data(std::string_view str) -> data {
  auto unescaped = unescape(str);
  auto result = data{};
  if (not(parsers::data - parsers::pattern)(unescaped, result)) {
    result = std::move(unescaped);
  }
  return result;
};

/// Parses the LEEF attributes field as a sequence of key-value pairs.
auto parse_attributes(char delimiter, std::string_view attributes)
  -> std::variant<record, diagnostic> {
  const auto key = +(parsers::printable - '=');
  const auto value = *(parsers::printable - delimiter);
  const auto kvp = key >> '=' >> value;
  const auto kvp_list = kvp % delimiter;
  auto kvps = std::vector<std::pair<std::string, std::string>>{};
  if (not kvp_list(attributes, kvps)) {
    return diagnostic::warning("failed to parse LEEF attributes")
      .note("attributes: {}", attributes)
      .done();
  }
  auto result = record{};
  result.reserve(kvps.size());
  for (auto& [key, value] : kvps) {
    result.emplace(std::move(key), to_data(value));
  }
  return result;
}

/// Converts a string view into a LEEF event.
auto to_event(std::string_view line) -> std::variant<event, diagnostic> {
  auto result = event{};
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
  result.leef_version = line.substr(colon + 1, pipe - colon - 1);
  if (result.leef_version == "1.0") {
    num_fields = 5;
  } else if (result.leef_version == "2.0") {
    num_fields = 6;
  } else {
    return diagnostic::warning("unsupported LEEF version: {}",
                               result.leef_version)
      .hint("only 1.0 and 2.0 are valid values")
      .done();
  }
  auto fields = detail::split_escaped(line, "|", "\\", num_fields);
  if (fields.size() != num_fields + 1) {
    return diagnostic::warning("LEEF {}.0 requires at least {} fields",
                               result.leef_version, num_fields + 1)
      .note("got {} fields", fields.size())
      .done();
  }
  auto delimiter = '\t';
  if (result.leef_version == "2.0") {
    auto delim = parse_delimiter(fields[5]);
    if (const auto* c = std::get_if<char>(&delim)) {
      TENZIR_DEBUG("parsed LEEF delimiter: {:#04x}", *c);
      delimiter = *c;
    } else {
      return std::get<diagnostic>(delim);
    }
  }
  result.vendor = std::move(fields[1]);
  result.product_name = std::move(fields[2]);
  result.product_version = std::move(fields[3]);
  result.event_id = std::move(fields[4]);
  auto kvps = parse_attributes(delimiter, fields[num_fields]);
  if (auto* xs = std::get_if<record>(&kvps)) {
    result.attributes = std::move(*xs);
  } else {
    return std::get<diagnostic>(kvps);
  }
  return result;
}

/// Adds a LEEF event to a builder.
void add(const event& e, builder_ref builder) {
  auto event = builder.record();
  event.field("leef_version", e.leef_version);
  event.field("vendor", e.vendor);
  event.field("product_name", e.product_name);
  event.field("product_version", e.product_version);
  event.field("attributes", e.attributes);
}

auto impl(generator<std::optional<std::string_view>> lines, exec_ctx ctx)
  -> generator<table_slice> {
  auto builder = series_builder{};
  for (auto&& line : lines) {
    if (!line) {
      co_yield {};
      continue;
    }
    if (line->empty()) {
      TENZIR_DEBUG("LEEF parser ignored empty line");
      continue;
    }
    auto e = to_event(*line);
    if (auto* diag = std::get_if<diagnostic>(&e)) {
      ctrl.diagnostics().emit(std::move(*diag));
      continue;
    }
    add(std::get<event>(e), builder);
  }
  for (auto& slice : builder.finish_as_table_slice("leef.event")) {
    co_yield std::move(slice);
  }
}

class leef_parser final : public plugin_parser {
public:
  auto name() const -> std::string override {
    return "leef";
  }

  auto instantiate(generator<chunk_ptr> input, exec_ctx ctx) const
    -> std::optional<generator<table_slice>> override {
    return impl(to_lines(std::move(input)), ctrl);
  }

  friend auto inspect(auto& f, leef_parser& x) -> bool {
    return f.object(x).fields();
  }
};

class plugin final : public virtual parser_plugin<leef_parser> {
  auto parse_parser(parser_interface& p) const
    -> std::unique_ptr<plugin_parser> override {
    argument_parser{name(),
                    fmt::format("https://docs.tenzir.com/formats/{}", name())}
      .parse(p);
    return std::make_unique<leef_parser>();
  }
};

} // namespace

} // namespace tenzir::plugins::leef

TENZIR_REGISTER_PLUGIN(tenzir::plugins::leef::plugin)

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
#include <tenzir/format/multi_schema_reader.hpp>
#include <tenzir/format/reader.hpp>
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

auto parse_delimiter(std::string_view field) -> caf::expected<char> {
  if (field.empty()) {
    return caf::make_error(ec::parse_error, "got empty delimiter");
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
        return caf::make_error(ec::parse_error,
                               fmt::format("invalid hex delimiter: {}", field));
      }
    }
    switch (hex.size()) {
      default:
        return caf::make_error(ec::parse_error,
                               fmt::format("wrong hex delimiter size: {}",
                                           hex.size()));
      case 1:
        return detail::hex_to_byte('0', hex[0]);
      case 2:
        return detail::hex_to_byte(hex[0], hex[1]);
      // TODO: address this only once a user ever gets such a weird log.
      case 3:
      case 4:
        return caf::make_error(ec::parse_error,
                               "cannot handle 3/4 byte delimiters");
    }
  } else if (field.size() > 1) {
    return caf::make_error(ec::parse_error,
                           fmt::format("expected single character, got '{}'",
                                       field));
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
  -> caf::expected<record> {
  const auto key = *(parsers::printable - '=');
  const auto value = *(parsers::printable - delimiter);
  const auto kvp = key >> '=' >> value;
  const auto kvp_list = kvp % delimiter;
  auto kvps = std::vector<std::pair<std::string, std::string>>{};
  if (not kvp_list(attributes, kvps)) {
    return caf::make_error(ec::parse_error, "failed to parse LEEF attributes "
                                            "as key-value pairs");
  }
  auto result = record{};
  result.reserve(kvps.size());
  for (auto& [key, value] : kvps) {
    result.emplace(std::move(key), to_data(value));
  }
  return result;
}

/// Converts a string view into a message.
auto convert(std::string_view line, event& e) -> caf::error {
  using namespace std::string_view_literals;
  // We first need to find out whether we are LEEF 1.0 or 2.0. The latter has
  // one additional top-level component.
  auto num_fields = 0u;
  if (line.starts_with("LEEF:1.0")) {
    num_fields = 5;
    e.leef_version = "1.0";
  } else if (line.starts_with("LEEF:2.0")) {
    num_fields = 6;
    e.leef_version = "2.0";
  } else {
    return caf::make_error(ec::parse_error, "unsupported LEEF version");
  }
  auto fields = detail::split_escaped(line, "|", "\\", num_fields);
  if (fields.size() != num_fields + 1) {
    return caf::make_error(ec::parse_error, //
                           fmt::format("LEEF {}.0 requires {}+1 fields"
                                       "for top-level fields, got {}",
                                       e.leef_version, num_fields,
                                       fields.size()));
  }
  auto delimiter = '\t';
  if (e.leef_version == "2.0") {
    if (auto delim = parse_delimiter(fields[5])) {
      delimiter = *delim;
    } else {
      return delim.error();
    }
  }
  e.vendor = std::move(fields[1]);
  e.product_name = std::move(fields[2]);
  e.product_version = std::move(fields[3]);
  e.event_id = std::move(fields[4]);
  if (auto kvps = parse_attributes(delimiter, fields[num_fields])) {
    e.attributes = std::move(*kvps);
  } else {
    return kvps.error();
  }
  return caf::none;
}

/// Parses a line of ASCII as LEEF message.
/// @param e The LEEF message.
/// @param builder The table slice builder to add the message to.
void add(const event& e, builder_ref builder) {
  auto event = builder.record();
  event.field("version", e.leef_version);
  event.field("vendor", e.vendor);
  event.field("product_name", e.product_name);
  event.field("product_version", e.product_version);
  event.field("attributes", e.attributes);
}

auto impl(generator<std::optional<std::string_view>> lines,
          operator_control_plane& ctrl) -> generator<table_slice> {
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
    auto e = to<event>(*line);
    if (!e) {
      diagnostic::warning("failed to parse message: {}", e.error())
        .note("line: `{}`", *line)
        .emit(ctrl.diagnostics());
      continue;
    }
    add(*e, builder);
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

  auto
  instantiate(generator<chunk_ptr> input, operator_control_plane& ctrl) const
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

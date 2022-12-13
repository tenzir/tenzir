//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "cef/parse.hpp"

#include <vast/concept/parseable/numeric.hpp>
#include <vast/concept/parseable/to.hpp>
#include <vast/concept/parseable/vast/data.hpp>
#include <vast/detail/string.hpp>
#include <vast/error.hpp>
#include <vast/table_slice_builder.hpp>
#include <vast/type.hpp>

#include <fmt/core.h>

namespace vast::plugins::cef {

namespace {

// Unescapes CEF string data containing \r, \n, \\, and \=.
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

} // namespace

caf::error convert(std::string_view line, message_view& msg) {
  using namespace std::string_view_literals;
  // Pipes in the extension field do not need escaping.
  auto fields = detail::split(line, "|", "\\", 8);
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
  auto cef_version_str = fields[0].substr(i + 1);
  if (!parsers::u16(cef_version_str, msg.cef_version))
    return caf::make_error(ec::parse_error, //
                           fmt::format("failed to parse CEF version, got '{}'",
                                       cef_version_str));
  // Fields 1-6.
  msg.device_vendor = fields[1];
  msg.device_product = fields[2];
  msg.device_version = fields[3];
  msg.signature_id = fields[4];
  msg.name = fields[5];
  msg.severity = fields[6];
  // Field 7: Extension
  if (auto kvps = parse_extension(fields[7]))
    msg.extension = std::move(*kvps);
  else
    return kvps.error();
  return caf::none;
}

caf::expected<record> parse_extension(std::string_view extension) {
  record result;
  auto splits = detail::split(extension, "=", "\\");
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
    if (auto x = to<data>(unescaped))
      return std::move(*x);
    return unescaped;
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

type infer(const message_view& msg) {
  static constexpr auto name = "cef.event";
  // These fields are always present.
  auto fields = std::vector<struct record_type::field>{
    {"cef_version", count_type{}},     {"device_vendor", string_type{}},
    {"device_product", string_type{}}, {"device_version", string_type{}},
    {"signature_id", string_type{}},   {"name", string_type{}},
    {"severity", string_type{}},
  };
  // Infer extension record, if present.
  auto deduce = [](const data& value) -> type {
    if (auto t = type::infer(value))
      return t;
    return type{string_type{}};
  };
  if (!msg.extension.empty()) {
    std::vector<struct record_type::field> ext_fields;
    ext_fields.reserve(msg.extension.size());
    for (const auto& [key, value] : msg.extension)
      ext_fields.emplace_back(std::string{key}, deduce(value));
    fields.emplace_back("extension", record_type{std::move(ext_fields)});
  }
  return {name, record_type{std::move(fields)}};
}

} // namespace vast::plugins::cef

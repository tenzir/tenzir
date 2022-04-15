//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "cef/parse.hpp"

#include <vast/concept/parseable/numeric.hpp>
#include <vast/detail/string.hpp>
#include <vast/error.hpp>
#include <vast/table_slice_builder.hpp>

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

caf::error convert(std::string_view line, message& msg) {
  using namespace std::string_view_literals;
  // Pipes in the extension field do not need escaping.
  auto fields = detail::split(line, "|", "\\", 8);
  if (fields.size() != 8)
    return caf::make_error(ec::parse_error, //
                           fmt::format("need exactly 8 fields, got {}",
                                       fields.size()));
  // Field 0: Version
  auto i = fields[0].find(':');
  if (i == std::string_view::npos)
    return caf::make_error(ec::parse_error, //
                           fmt::format("CEF version requires ':', got {}",
                                       fields[0]));
  auto cef_version_str = fields[0].substr(i + 1);
  if (!parsers::u16(cef_version_str, msg.cef_version))
    return caf::make_error(ec::parse_error, //
                           fmt::format("failed to parse CEF version, got {}",
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

caf::expected<std::vector<std::pair<std::string_view, std::string>>>
parse_extension(std::string_view extension) {
  std::vector<std::pair<std::string_view, std::string>> result;
  auto splits = detail::split(extension, "=", "\\");
  if (splits.size() < 2)
    return caf::make_error(ec::parse_error, fmt::format("need at least one "
                                                        "key=value pair: {}",
                                                        extension));
  // Process intermediate 'k0=a b c k1=d e f' extensions. The algorithm splits
  // on '='. The first split is the key and the last split is a value. All
  // intermediate splits are "reversed" in that they have the pattern 'a b c k1'
  // where 'a b c' is the value from the previous key and 'k1`' is the key for
  // the next value.
  auto key = splits[0];
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
    result.emplace_back(key, unescape(value));
    key = split.substr(j + 1); // next key
  }
  auto value = splits[splits.size() - 1];
  result.emplace_back(key, unescape(value));
  return result;
}

} // namespace vast::plugins::cef

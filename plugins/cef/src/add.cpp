//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "cef/add.hpp"

#include <vast/concept/parseable/numeric.hpp>
#include <vast/detail/string.hpp>
#include <vast/error.hpp>
#include <vast/table_slice_builder.hpp>

#include <fmt/core.h>

namespace vast::plugins::cef {

caf::error add(std::string_view line, table_slice_builder& builder) {
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
  auto cef_version = uint16_t{0};
  if (!parsers::u16(cef_version_str, cef_version))
    return caf::make_error(ec::parse_error, //
                           fmt::format("failed to parse CEF version, got {}",
                                       cef_version_str));
  if (!builder.add(make_view(count{cef_version})))
    return caf::make_error(ec::parse_error, //
                           fmt::format("failed to add CEF version: {}",
                                       cef_version));
  // Field 1: Device Vendor
  if (!builder.add(make_data_view(fields[1])))
    return caf::make_error(ec::parse_error, fmt::format("failed to add Device "
                                                        "Vendor: {}",
                                                        fields[1]));
  // Field 2: Device Product
  if (!builder.add(make_data_view(fields[2])))
    return caf::make_error(ec::parse_error, fmt::format("failed to add Device "
                                                        "Product: {}",
                                                        fields[2]));
  // Field 3: Device Version
  if (!builder.add(make_data_view(fields[3])))
    return caf::make_error(ec::parse_error, fmt::format("failed to add Device "
                                                        "Version: {}",
                                                        fields[3]));
  // Field 4: Signature ID
  if (!builder.add(make_data_view(fields[4])))
    return caf::make_error(ec::parse_error, fmt::format("failed to add "
                                                        "Signature ID: {}",
                                                        fields[4]));
  // Field 5: Name
  if (!builder.add(make_data_view(fields[5])))
    return caf::make_error(ec::parse_error,
                           fmt::format("failed to add Name: {}", fields[5]));
  // Field 6: Severity
  if (!builder.add(make_data_view(fields[6])))
    return caf::make_error(ec::parse_error,
                           fmt::format("failed to add Name: {}", fields[6]));
  // Field 7: Extension
  if (!builder.add(make_data_view(fields[7])))
    return caf::make_error(
      ec::parse_error, fmt::format("failed to add Extension: {}", fields[7]));
  return caf::none;
}

} // namespace vast::plugins::cef

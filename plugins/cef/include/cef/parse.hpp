//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <caf/expected.hpp>

#include <cstdint>
#include <string>
#include <tuple>
#include <vector>

namespace vast::plugins::cef {

/// A shallow representation a of a CEF message.
struct message {
  uint16_t cef_version;
  std::string_view device_vendor;
  std::string_view device_product;
  std::string_view device_version;
  std::string_view signature_id;
  std::string_view name;
  std::string_view severity;
  std::vector<std::pair<std::string_view, std::string>> extension;
};

/// Converts a string view into a message.
caf::error convert(std::string_view line, message& msg);

// Parses the CEF extension field as a sequence of key-value pairs for further
// downstream processing.
// @param extension The string value of the extension field.
// @returns A vector of key-value pairs with properly unescaped values.
caf::expected<std::vector<std::pair<std::string_view, std::string>>>
parse_extension(std::string_view extension);

} // namespace vast::plugins::cef

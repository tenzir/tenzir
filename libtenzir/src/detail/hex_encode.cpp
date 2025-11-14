//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/hex_encode.hpp"

#include <boost/algorithm/hex.hpp>

namespace tenzir::detail::hex {
auto encode(const std::string_view input) -> std::string {
  if (input.empty()) {
    return {};
  }
  auto encoded = std::string{};
  encoded.reserve(input.length() * 2);
  boost::algorithm::hex(input, std::back_inserter(encoded));
  return encoded;
}

auto decode(const std::string_view input) -> std::optional<std::string> {
  if (input.empty()) {
    return std::string{};
  }
  auto decoded = std::string{};
  decoded.reserve(input.length() / 2);
  try {
    boost::algorithm::unhex(input, std::back_inserter(decoded));
  } catch (boost::algorithm::hex_decode_error& ex) {
    return std::nullopt;
  }
  return decoded;
}

} // namespace tenzir::detail::hex

//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/strip_leading_indentation.hpp"

#include <tenzir/generator.hpp>

namespace tenzir::detail {

auto strip_leading_indentation(std::string&& code) -> std::string {
  /// Yields each line, including the trailing newline.
  auto each_line = [](std::string_view code) -> generator<std::string_view> {
    while (!code.empty()) {
      auto right = code.find('\n');
      if (right == std::string_view::npos) {
        // Handle the no trailing newline case.
        co_yield code.substr(0, right);
        break;
      }
      co_yield code.substr(0, right + 1);
      code.remove_prefix(right + 1);
    }
  };
  auto common_prefix = [](std::string_view lhs, std::string_view rhs) {
    return std::string_view{
      lhs.begin(),
      std::mismatch(lhs.begin(), lhs.end(), rhs.begin(), rhs.end()).first};
  };

  auto indentation = std::string_view{};
  auto start = true;
  for (auto line : each_line(code)) {
    if (auto x = line.find_first_not_of(" \t\n"); x != std::string::npos) {
      auto indent = line.substr(0, x);
      if (start) {
        indentation = indent;
        start = false;
      } else {
        indentation = common_prefix(indentation, indent);
      }
    }
  }
  if (indentation.empty())
    return code;
  auto stripped_code = std::string{};
  for (auto line : each_line(code)) {
    if (line.starts_with(indentation))
      line.remove_prefix(indentation.size());
    stripped_code += line;
  }
  return stripped_code;
}

} // namespace tenzir::detail

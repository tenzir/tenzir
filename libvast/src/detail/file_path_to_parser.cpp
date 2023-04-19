//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/detail/file_path_to_parser.hpp"

#include <filesystem>
#include <string>

namespace vast::detail {

constexpr inline auto fallback_parser = "json";

auto file_path_to_parser(std::string_view path) -> std::string {
  auto extension = std::filesystem::path(path).extension();
  if (extension.empty()) {
    return fallback_parser;
  }
  return extension.string().substr(1);
}

} // namespace vast::detail

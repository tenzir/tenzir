//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/detail/file_path_to_parser.hpp"

#include <array>

namespace vast::detail {

constexpr inline auto fallback_parser = "json";

const auto filename_to_parser_list
  = std::array<std::pair<std::filesystem::path, std::string>, 1>{
    {{"eve.json", "suricata"}}};

const auto extension_to_parser_list
  = std::array<std::pair<std::string, std::string>, 1>{{{".ndjson", "json"}}};

auto file_path_to_parser(const std::filesystem::path& path) -> std::string {
  for (const auto& [filename, parser] : filename_to_parser_list) {
    if (path == filename) {
      return parser;
    }
  }
  for (const auto& [special_extension, parser] : extension_to_parser_list) {
    if (path.extension() == special_extension) {
      return parser;
    }
  }
  auto fallback_ext = std::filesystem::path(path).extension();
  if (fallback_ext.empty()) {
    return fallback_parser;
  }
  return fallback_ext.string().substr(1);
}

} // namespace vast::detail

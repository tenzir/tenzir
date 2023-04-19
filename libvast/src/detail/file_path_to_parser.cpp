//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/detail/file_path_to_parser.hpp"

#include <array>
#include <filesystem>
#include <string>

namespace vast::detail {

constexpr inline auto fallback_parser = "json";

constexpr inline auto extension_to_parser_list
  = std::array<std::pair<std::string, std::string>, 2>{
    {{"eve.json", "suricata"}, {".ndjson", "json"}}};

auto file_path_to_parser(std::string_view path) -> std::string {
  for (const auto& [special_extension, parser] : extension_to_parser_list) {
    if (path.ends_with(special_extension)) {
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

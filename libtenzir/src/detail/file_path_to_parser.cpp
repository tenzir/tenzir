//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/file_path_to_parser.hpp"

#include "tenzir/plugin.hpp"

#include <array>

namespace tenzir::detail {

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
  auto fallback_ext = std::filesystem::path(path).extension().string();
  if (fallback_ext.empty()) {
    return fallback_parser;
  }
  fallback_ext = fallback_ext.substr(1);
  if (plugins::find<parser_parser_plugin>(fallback_ext)) {
    return fallback_ext;
  }
  TENZIR_VERBOSE("Could not find default parser for path {} - falling back "
                 "to "
                 "{}",
                 path, fallback_parser);
  return fallback_parser;
}

} // namespace tenzir::detail

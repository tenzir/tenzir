//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/file_path_to_plugin_name.hpp"

#include "tenzir/plugin.hpp"

#include <array>
#include <span>

namespace tenzir::detail {

namespace {
using map_entry = std::pair<std::string_view, std::string_view>;

constexpr auto filename_to_plugin_map = std::array<map_entry, 1>{{
  {"eve.json", "suricata"},
}};
constexpr auto extension_to_plugin_map = std::array<map_entry, 2>{{
  {".ndjson", "json"},
  {".yml", "yaml"},
}};

auto find_map_entry(std::span<const map_entry> map, std::string_view key)
  -> std::optional<std::string> {
  if (auto it = std::ranges::find(map, key,
                                  [](const auto& pair) {
                                    return pair.first;
                                  });
      it != map.end())
    return std::string{it->second};
  return std::nullopt;
}
} // namespace

auto file_path_to_plugin_name(const std::filesystem::path& path)
  -> std::optional<std::string> {
  if (auto name
      = find_map_entry(filename_to_plugin_map, path.filename().string()))
    return name;
  auto ext = path.extension().string();
  if (ext.size() <= 1)
    return std::nullopt;
  if (auto result = find_map_entry(extension_to_plugin_map, ext))
    return result;
  TENZIR_ASSERT(ext.size() > 1 && ext.front() == '.');
  return ext.substr(1);
}

} // namespace tenzir::detail

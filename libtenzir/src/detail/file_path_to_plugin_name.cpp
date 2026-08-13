//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/file_path_to_plugin_name.hpp"

#include "tenzir/detail/assert.hpp"

#include <algorithm>
#include <array>
#include <span>
#include <string_view>

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
  -> Option<std::string> {
  if (auto it = std::ranges::find(map, key,
                                  [](const auto& pair) {
                                    return pair.first;
                                  });
      it != map.end()) {
    return std::string{it->second};
  }
  return None{};
}
} // namespace

auto file_path_to_plugin_name(const std::filesystem::path& path)
  -> Option<std::string> {
  if (auto name
      = find_map_entry(filename_to_plugin_map, path.filename().string())) {
    return name;
  }
  auto ext = path.extension().string();
  if (ext.size() <= 1) {
    return None{};
  }
  if (auto result = find_map_entry(extension_to_plugin_map, ext)) {
    return result;
  }
  TENZIR_ASSERT(ext.size() > 1 and ext.front() == '.');
  return ext.substr(1);
}

} // namespace tenzir::detail

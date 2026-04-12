//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/defaults.hpp"
#include "tenzir/error.hpp"
#include "tenzir/logger.hpp"

#include <algorithm>
#include <filesystem>
#include <functional>
#include <system_error>
#include <vector>

namespace tenzir::detail {

/// Recursively traverses a directory and lists all file names that match a
/// given filter expresssion.
/// @param root_dir The directory to enumerate.
/// @param filter An optional filter function to apply on the filename of every
/// file in *dir*, which allows for filtering specific files.
/// @param max_recursion The maximum number of nested directories to traverse.
/// @returns A list of file that match *filter*.
inline caf::expected<std::vector<std::filesystem::path>>
filter_dir(const std::filesystem::path& root_dir,
           std::function<bool(const std::filesystem::path&)> filter = {},
           size_t max_recursion = defaults::max_recursion) {
  std::vector<std::filesystem::path> result;
  std::error_code err{};
  auto dir = std::filesystem::recursive_directory_iterator(root_dir, err);
  if (err)
    return caf::make_error(ec::filesystem_error, err.message());
  auto begin = std::filesystem::begin(dir);
  const auto end = std::filesystem::end(dir);
  while (begin != end) {
    const auto current_path = begin->path();
    const auto current_depth = static_cast<size_t>(begin.depth());
    if (current_depth >= max_recursion)
      return caf::make_error(ec::recursion_limit_reached,
                             fmt::format("reached recursion limit when "
                                         "filtering directory {}",
                                         root_dir));
    if (not filter or filter(current_path))
      result.push_back(current_path);
    ++begin;
  }
  std::sort(result.begin(), result.end());
  return result;
}

} // namespace tenzir::detail

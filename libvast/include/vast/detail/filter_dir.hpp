//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/defaults.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"

#include <algorithm>
#include <filesystem>
#include <functional>
#include <system_error>
#include <vector>

namespace vast::detail {

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
    if (!filter || filter(current_path))
      result.push_back(current_path);
    ++begin;
  }
  std::sort(result.begin(), result.end());
  return result;
}

} // namespace vast::detail

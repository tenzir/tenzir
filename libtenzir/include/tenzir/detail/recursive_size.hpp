//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/error.hpp"
#include "tenzir/logger.hpp"

#include <filesystem>
#include <system_error>

namespace tenzir::detail {

/// Calculates the sum of the sizes of all regular files in the directory.
/// @param root_dir The directory to traverse.
/// @returns The size of all regular files in *dir*.
inline caf::expected<size_t>
recursive_size(const std::filesystem::path& root_dir) {
  size_t total_size = 0;
  std::error_code err{};
  auto dir = std::filesystem::recursive_directory_iterator(root_dir, err);
  if (err)
    return caf::make_error(ec::filesystem_error, err.message());
  for (const auto& f : dir) {
    if (f.is_regular_file()) {
      const auto size = f.file_size(err);
      if (err) {
        if (err == std::errc::no_such_file_or_directory)
          continue;
        return caf::make_error(ec::filesystem_error, err.message());
      }
      TENZIR_TRACE("{} += {}", f.path().string(), size);
      total_size += size;
    }
  }
  return total_size;
}

} // namespace tenzir::detail

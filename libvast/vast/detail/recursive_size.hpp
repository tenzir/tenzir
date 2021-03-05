
/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include "vast/fwd.hpp"

#include "vast/error.hpp"
#include "vast/logger.hpp"

#include <filesystem>
#include <system_error>

namespace vast::detail {

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
      const auto size = f.file_size();
      VAST_TRACE("{} += {}", f.path().string(), size);
      total_size += size;
    }
  }
  return total_size;
}

} // namespace vast::detail

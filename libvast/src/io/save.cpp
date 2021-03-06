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

#include "vast/io/save.hpp"

#include "vast/error.hpp"
#include "vast/io/write.hpp"
#include "vast/logger.hpp"
#include "vast/path.hpp"

#include <cstddef>
#include <cstdio>
#include <filesystem>
#include <system_error>

namespace vast::io {

caf::error save(const path& filename, span<const std::byte> xs) {
  auto tmp = filename + ".tmp";
  if (auto err = write(tmp, xs)) {
    std::error_code ec{};
    if (const auto removed
        = std::filesystem::remove(std::filesystem::path{tmp.str()}, ec);
        !removed)
      VAST_WARN("failed to remove file {} : {}", tmp, ec.message());
    return err;
  }
  if (std::rename(tmp.str().c_str(), filename.str().c_str()) != 0) {
    std::error_code err{};
    if (const auto removed_files
        = std::filesystem::remove_all(std::filesystem::path{tmp.str()}, err);
        removed_files == 0)
      VAST_WARN("failed to remove any files from directory {} : {}", tmp,
                err.message());
    return caf::make_error(ec::filesystem_error,
                           "failed in rename(2):", std::strerror(errno));
  }
  return caf::none;
}

} // namespace vast::io

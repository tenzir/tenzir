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
#include <string>
#include <system_error>

namespace vast::io {

caf::error
save(const std::filesystem::path& filename, span<const std::byte> xs) {
  auto tmp = filename;
  tmp += ".tmp";
  if (auto err = write(tmp, xs)) {
    std::error_code ec{};
    if (const auto removed = std::filesystem::remove(tmp, ec); !removed || ec)
      VAST_WARN("failed to remove file {} : {}", tmp, ec.message());
    return err;
  }
  std::error_code err{};
  std::filesystem::rename(tmp, filename, err);
  if (err) {
    std::filesystem::remove(tmp, err);
    return caf::make_error(ec::filesystem_error,
                           fmt::format("failed to rename {} : {}", filename,
                                       err.message()));
  }
  return caf::none;
}

caf::error save(const path& filename, span<const std::byte> xs) {
  return save(std::filesystem::path{filename.str()}, xs);
}

} // namespace vast::io

//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/io/save.hpp"

#include "vast/error.hpp"
#include "vast/io/write.hpp"
#include "vast/logger.hpp"

#include <cstddef>
#include <cstdio>
#include <filesystem>
#include <span>
#include <string>
#include <system_error>

namespace vast::io {

caf::error
save(const std::filesystem::path& filename, std::span<const std::byte> xs) {
  auto tmp = filename;
  tmp += ".tmp";
  if (auto err = write(tmp, xs)) {
    std::error_code ec{};
    if (const auto removed = std::filesystem::remove(tmp, ec); !removed || ec)
      VAST_WARN("failed to remove file {} : {}", tmp, ec.message());
    return err;
  }
  std::error_code ec{};
  std::filesystem::rename(tmp, filename, ec);
  if (ec) {
    auto err = caf::make_error(ec::filesystem_error,
                               fmt::format("failed to rename {} : {}", filename,
                                           ec.message()));
    std::filesystem::remove(tmp, ec);
    return err;
  }
  return caf::none;
}

} // namespace vast::io

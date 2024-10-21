//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/io/save.hpp"

#include "tenzir/error.hpp"
#include "tenzir/io/write.hpp"
#include "tenzir/logger.hpp"

#include <cstddef>
#include <cstdio>
#include <filesystem>
#include <span>
#include <string>
#include <system_error>

namespace tenzir::io {

caf::error
save(const std::filesystem::path& filename, std::span<const std::byte> xs) {
  std::error_code ec{};
  if (filename.has_parent_path()) {
    std::filesystem::create_directories(filename.parent_path(), ec);
    if (ec) {
      return caf::make_error(ec::filesystem_error,
                             fmt::format("failed to create directory {}: {}",
                                         filename.parent_path(), ec));
    }
  }
  auto tmp = filename;
  tmp += ".tmp";
  if (auto err = write(tmp, xs)) {
    if (const auto removed = std::filesystem::remove(tmp, ec); !removed || ec)
      TENZIR_WARN("failed to remove file {} : {}", tmp, ec.message());
    return err;
  }
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

} // namespace tenzir::io

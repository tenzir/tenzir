//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/io/write.hpp"

#include "tenzir/error.hpp"
#include "tenzir/file.hpp"

#include <cstddef>
#include <filesystem>
#include <span>

namespace tenzir::io {

caf::error
write(const std::filesystem::path& filename, std::span<const std::byte> xs) {
  file f{filename};
  if (auto result = f.open(file::write_only); !result) {
    return result.error();
  }
  return f.write(xs.data(), xs.size());
}

} // namespace tenzir::io

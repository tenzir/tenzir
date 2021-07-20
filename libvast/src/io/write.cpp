//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/io/write.hpp"

#include "vast/error.hpp"
#include "vast/file.hpp"

#include <cstddef>
#include <filesystem>
#include <span>

namespace vast::io {

caf::error
write(const std::filesystem::path& filename, std::span<const std::byte> xs) {
  file f{filename};
  if (!f.open(file::write_only))
    return caf::make_error(ec::filesystem_error, "failed open file");
  return f.write(xs.data(), xs.size());
}

} // namespace vast::io

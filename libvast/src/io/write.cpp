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
#include "vast/path.hpp"

#include <cstddef>

namespace vast::io {

caf::error write(const path& filename, span<const std::byte> xs) {
  file f{filename};
  if (!f.open(file::write_only))
    return caf::make_error(ec::filesystem_error, "failed open file");
  return f.write(xs.data(), xs.size());
}

caf::error
write(const std::filesystem::path& filename, span<const std::byte> xs) {
  return write(vast::path{filename.string()}, xs);
}

} // namespace vast::io

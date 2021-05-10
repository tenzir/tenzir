//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "caf/fwd.hpp"
#include "vast/fwd.hpp"

#include "vast/span.hpp"

#include <cstddef>
#include <filesystem>
#include <vector>

namespace vast::io {

/// Reads bytes from a file into a buffer in one shot.
/// @param filename The file to read from.
/// @param xs The buffer to write into.
/// @returns An error if the operation failed.
caf::error read(const std::filesystem::path& filename, span<std::byte> xs);

/// Reads bytes from a file into a buffer in one shot.
/// @param filename The file to read from.
/// @returns The raw bytes of the buffer.
caf::expected<std::vector<std::byte>>
read(const std::filesystem::path& filename);

} // namespace vast::io

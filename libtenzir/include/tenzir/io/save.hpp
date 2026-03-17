//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include <cstddef>
#include <filesystem>
#include <span>

namespace tenzir::io {

/// Writes a buffer to a temporary file and atomically renames it to *filename*
/// afterwards.
/// @param filename The file to write to.
/// @param xs The buffer to read from.
/// @returns An error if the operation failed.
[[nodiscard]] caf::error
save(const std::filesystem::path& filename, std::span<const std::byte> xs);

} // namespace tenzir::io

//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <caf/error.hpp>

#include <filesystem>

namespace tenzir::detail {

/// Attempts to acquire a PID file at a given path.
/// @param filename The path to the PID file.
/// @returns `none` if acquiring succeeded and an error on failure.
/// @relates release_pid_file
auto acquire_pid_file(const std::filesystem::path& filename) -> caf::error;

} // namespace tenzir::detail

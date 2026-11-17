//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2019 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/aliases.hpp"

#include <caf/expected.hpp>

#include <filesystem>

namespace tenzir::detail {

/// Locates the path of a shared library or executable.
/// @param addr The address to use for the lookup, needs to be in an
///             mmaped region in order to succeed.
/// @returns the filesystem path to the library or executable mapped at address
///          addr, or error if the resolution fails.
caf::expected<std::filesystem::path> objectpath(const void* addr = nullptr);

/// Forks a process to execute the given comand and returns its output
/// as a string.
/// NOTE: This function assumes that `command` has been properly sanitized
/// and performs no further checks on its contents.
/// @returns the output of the command.
caf::expected<std::string> execute_blocking(const std::string& command);

} // namespace tenzir::detail

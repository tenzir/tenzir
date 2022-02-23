//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/aliases.hpp"

#include <caf/expected.hpp>

#include <filesystem>

namespace vast::detail {

/// Locates the path of a shared library or executable.
/// @param addr The address to use for the lookup, needs to be in an
///             mmaped region in order to succeed.
/// @returns the filesystem path to the library or executable mapped at address
///          addr, or error if the resolution fails.
caf::expected<std::filesystem::path> objectpath(const void* addr = nullptr);

/// Retrieves runtime information about the current process.
/// @returns a record containing information about system resource usage.
record get_status();

/// Forks a process to execute the given comand and returns its output
/// as a string.
/// NOTE: This function assumes that `command` has been properly sanitized
/// and performs no further checks on its contents.
/// @returns the output of the command.
caf::expected<std::string> execute_blocking(const std::string& command);

} // namespace vast::detail

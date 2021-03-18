// SPDX-FileCopyrightText: (c) 2019 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <caf/expected.hpp>
#include <caf/settings.hpp>

#include <filesystem>

namespace vast::detail {

/// Locates the path of a shared library or executable.
/// @param addr The address to use for the lookup, needs to be in an
///             mmaped region in order to succeed.
/// @returns the filesystem path to the library or executable mapped at address
///          addr, or error if the resolution fails.
caf::expected<std::filesystem::path> objectpath(const void* addr = nullptr);

/// Retrieves runtime information about the current process.
/// @returns a settings object containing information about system resource
///          usage.
caf::settings get_status();

} // namespace vast::detail

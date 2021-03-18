// SPDX-FileCopyrightText: (c) 2016 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <cstdint>
#include <string>

namespace vast::detail {

/// Retrieves the hostname of the system.
/// @returns The system hostname.
std::string hostname();

/// Retrieves the page size of the OS.
/// @returns The number of bytes of a page.
size_t page_size();

/// Retrieves the process ID.
/// @returns The ID of this process.
int32_t process_id();

} // namespace vast::detail


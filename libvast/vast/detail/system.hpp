/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#ifndef VAST_DETAIL_SYSTEM_HPP
#define VAST_DETAIL_SYSTEM_HPP

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

#endif

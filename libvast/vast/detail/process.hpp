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

#pragma once

#include "vast/path.hpp"

#include <caf/expected.hpp>
#include <caf/settings.hpp>

namespace vast::detail {

/// Locates the path of a shared library or executable.
/// @param addr The address to use for the lookup, needs to be in an
///             mmaped region in order to succeed.
/// @returns the filesystem path to the library or executable mapped at address
///          addr, or error if the resolution fails.
caf::expected<path> objectpath(const void* addr = nullptr);

/// Retrieves runtime information about the current process.
/// @returns a settings object containing information about system resource
///          usage.
caf::settings get_status();

} // namespace vast::detail

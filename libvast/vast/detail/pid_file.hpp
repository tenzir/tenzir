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

#include <caf/error.hpp>

#include <filesystem>

namespace vast::detail {

/// Attempts to acquire a PID file at a given path.
/// @param filename The path to the PID file.
/// @returns `none` if acquiring succeeded and an error on failure.
/// @relates release_pid_file
caf::error acquire_pid_file(const std::filesystem::path& filename);

} // namespace vast::detail

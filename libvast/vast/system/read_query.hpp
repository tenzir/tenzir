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

#include "vast/command.hpp"

#include <caf/fwd.hpp>

namespace vast::system {

/// Reads query from input file, STDIN or CLI arguments.
/// @param invocation The command line in parsed form.
/// @param file_option The option name that is used to pass the query by file
///                    instead of as command line argument(s).
/// @param argument_offset The number of argumetns to skip before the query.
/// @returns The query string or an error.
caf::expected<std::string>
read_query(const command::invocation& invocation, std::string_view file_option,
           size_t argument_offset = 0);

} // namespace vast::system

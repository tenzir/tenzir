//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

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
read_query(const invocation& inv, std::string_view file_option,
           size_t argument_offset = 0);

} // namespace vast::system

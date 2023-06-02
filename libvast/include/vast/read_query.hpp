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

namespace vast {

/// Whether a query must be provided.
enum class must_provide_query {
  yes, /// Not providing a query is an error.
  no,  /// Not providing a query parses everything.
};

/// Reads query from input file, STDIN or CLI arguments.
/// @param invocation The command line in parsed form.
/// @param file_option The option name that is used to pass the query by file
/// instead of as command line argument(s).
/// @param must_provide_query Whether the empty query should be transformed
/// into one that exports everything, or providing a query is required.
/// @param argument_offset The number of argumetns to skip before the query.
/// @returns The query string or an error.
caf::expected<std::string>
read_query(const invocation& inv, std::string_view file_option,
           enum must_provide_query must_provide_query,
           size_t argument_offset = 0);

} // namespace vast

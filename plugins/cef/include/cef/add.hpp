//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <vast/detail/generator.hpp>
#include <vast/fwd.hpp>
#include <vast/view.hpp>

#include <caf/error.hpp>

#include <string>
#include <tuple>

namespace vast::plugins::cef {

/// Parses a line of ASCII as CEF event.
/// @param line The CEF event.
/// @param builder The table slice builder.
caf::error add(std::string_view line, table_slice_builder& builder);

} // namespace vast::plugins::cef

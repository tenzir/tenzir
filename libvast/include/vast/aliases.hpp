//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/detail/stable_map.hpp"

#include <cstdint>
#include <limits>
#include <string>
#include <vector>

namespace vast {

/// A random-access sequence of data.
using list = std::vector<data>;

/// An associative array with ::data as both key and value.
using map = detail::stable_map<data, data>;

/// Maps field names to data elements.
using record = detail::stable_map<std::string, data>;

/// Uniquely identifies a VAST event.
using id = uint64_t;

/// The ID for invalid events
constexpr id invalid_id = std::numeric_limits<id>::max();

/// The largest possible event ID.
constexpr id max_id = invalid_id - 1;

/// The largest number of representable events.
constexpr id max_events = max_id + 1;

} // namespace vast

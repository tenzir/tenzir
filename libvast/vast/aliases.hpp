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

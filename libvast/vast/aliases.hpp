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

#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <set>
#include <vector>

namespace vast {

// -- data -------------------------------------------------------------------

class data;

using boolean = bool;
using integer = int64_t;
using count = uint64_t;
using real = double;

/// A random-access sequence of data.
using vector = std::vector<data>;

/// A mathematical set where each element is ::data.
using set = std::set<data>;

/// An associative array with ::data as both key and value.
using map = std::map<data, data>;

// ---------------------------------------------------------------------------

class ewah_bitstream;
using default_bitstream = ewah_bitstream;

/// Uniquely identifies a VAST event.
using id = uint64_t;

/// The ID for invalid events
constexpr id invalid_id = std::numeric_limits<id>::max();

/// The largest possible event ID.
constexpr id max_id = invalid_id - 1;

/// The largest number of representable events.
constexpr id max_events = max_id + 1;

/// Uniquely identifies a VAST type.
using type_id = uint64_t;

/// The data type for an enumeration.
using enumeration = uint32_t;

} // namespace vast


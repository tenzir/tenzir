#ifndef VAST_ALIASES_HPP
#define VAST_ALIASES_HPP

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
using table = std::map<data, data>;

// ---------------------------------------------------------------------------

class ewah_bitstream;
using default_bitstream = ewah_bitstream;

/// Uniquely identifies a VAST event.
using event_id = uint64_t;

/// The ID for invalid events
constexpr event_id invalid_event_id = std::numeric_limits<event_id>::max();

/// The largest possible event ID.
constexpr event_id max_event_id = invalid_event_id - 1;

/// The largest number of representable events.
constexpr event_id max_events = max_event_id + 1;

/// Uniquely identifies a VAST type.
using type_id = uint64_t;

/// The data type for an enumeration.
using enumeration = uint32_t;

} // namespace vast

#endif

#ifndef VAST_ALIASES_H
#define VAST_ALIASES_H

#include <cstdint>
#include <limits>
#include <memory>

namespace vast {

// Values
using boolean = bool;
using integer = int64_t;
using count = uint64_t;
using real = double;

class ewah_bitstream;
using default_bitstream = ewah_bitstream;

/// Uniquely identifies a VAST event.
using event_id = uint64_t;

/// The invalid event ID.
static constexpr event_id invalid_event_id = 0;

/// The smallest possible event ID.
static constexpr event_id min_event_id = 1;

/// The largest possible event ID.
static constexpr event_id max_event_id =
  std::numeric_limits<event_id>::max() - 1;

/// Uniquely identifies a VAST type.
using type_id = uint64_t;

} // namespace vast

#endif

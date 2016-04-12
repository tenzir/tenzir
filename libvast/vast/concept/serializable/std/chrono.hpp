#ifndef VAST_CONCEPT_SERIALIZABLE_STD_CHRONO_HPP
#define VAST_CONCEPT_SERIALIZABLE_STD_CHRONO_HPP

#include <chrono>

#include <caf/deserializer.hpp>
#include <caf/serializer.hpp>

// Put 'em in namespace caf so that ADL finds them.
namespace caf {

template <class Clock, class Duration>
void serialize(serializer& sink, std::chrono::time_point<Clock, Duration> t) {
  sink << t.time_since_epoch();
}

template <class Clock, class Duration>
void serialize(deserializer& source,
                 std::chrono::time_point<Clock, Duration>& t) {
  Duration since_epoch;
  source >> since_epoch;
  t = std::chrono::time_point<Clock, Duration>(since_epoch);
}

} // namespace caf

#endif
